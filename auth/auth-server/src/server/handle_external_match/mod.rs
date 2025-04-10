//! Handler code for proxied relayer requests
//!
//! At a high level the server must first authenticate the request, then forward
//! it to the relayer with admin authentication

use auth_server_api::{
    GasSponsorshipInfo, GasSponsorshipQueryParams, SponsoredMatchResponse, SponsoredQuoteResponse,
};
use bytes::Bytes;
use http::{HeaderMap, Method, Response, StatusCode};
use renegade_api::http::external_match::{
    AssembleExternalMatchRequest, ExternalMatchRequest, ExternalMatchResponse, ExternalOrder,
    ExternalQuoteRequest, ExternalQuoteResponse,
};
use renegade_circuit_types::fixed_point::FixedPoint;
use renegade_common::types::{token::Token, TimestampedPrice};
use renegade_constants::EXTERNAL_MATCH_RELAYER_FEE;
use tracing::{error, info, instrument, warn};
use warp::{reject::Rejection, reply::Reply};

use super::helpers::{generate_quote_uuid, overwrite_response_body};
use super::Server;
use crate::error::AuthServerError;
use crate::telemetry::helpers::{calculate_implied_price, record_relayer_request_500};
use crate::telemetry::labels::{GAS_SPONSORED_METRIC_TAG, SDK_VERSION_METRIC_TAG};
use crate::telemetry::{
    helpers::{
        await_settlement, record_endpoint_metrics, record_external_match_metrics, record_fill_ratio,
    },
    labels::{
        DECIMAL_CORRECTION_FIXED_METRIC_TAG, EXTERNAL_MATCH_QUOTE_REQUEST_COUNT,
        KEY_DESCRIPTION_METRIC_TAG, REQUEST_ID_METRIC_TAG,
    },
};

mod gas_sponsorship;
pub use gas_sponsorship::contract_interaction::sponsorAtomicMatchSettleWithRefundOptionsCall;

// -------------
// | Constants |
// -------------

/// The header name for the SDK version
const SDK_VERSION_HEADER: &str = "x-renegade-sdk-version";

/// The default SDK version to use if the header is not set
const SDK_VERSION_DEFAULT: &str = "pre-v0.1.0";

// ---------------
// | Server Impl |
// ---------------

/// Handle a proxied request
impl Server {
    /// Handle an external quote request
    #[instrument(skip(self, path, headers, body))]
    pub async fn handle_external_quote_request(
        &self,
        path: warp::path::FullPath,
        headers: warp::hyper::HeaderMap,
        body: Bytes,
        query_str: String,
    ) -> Result<impl Reply, Rejection> {
        // Authorize the request
        let path_str = path.as_str();
        let key_desc = self.authorize_request(path_str, &query_str, &headers, &body).await?;
        self.check_quote_rate_limit(key_desc.clone()).await?;

        // Send the request to the relayer
        let mut resp =
            self.send_admin_request(Method::POST, path_str, headers.clone(), body.clone()).await?;

        let status = resp.status();
        if status == StatusCode::INTERNAL_SERVER_ERROR {
            record_relayer_request_500(key_desc.clone(), path_str.to_string());
        }
        if status != StatusCode::OK {
            log_unsuccessful_relayer_request(&resp, &key_desc, path_str, &body, &headers);
            return Ok(resp);
        }

        // Parse query params
        let query_params = serde_urlencoded::from_str::<GasSponsorshipQueryParams>(&query_str)
            .map_err(AuthServerError::serde)?;

        let sponsored_quote_response = self
            .maybe_apply_gas_sponsorship_to_quote(key_desc.clone(), resp.body(), &query_params)
            .await?;

        overwrite_response_body(&mut resp, sponsored_quote_response.clone())?;

        let server_clone = self.clone();
        tokio::spawn(async move {
            // Cache the gas sponsorship info for the quote in Redis if it exists
            if let Err(e) =
                server_clone.cache_quote_gas_sponsorship_info(&sponsored_quote_response).await
            {
                error!("Error caching quote gas sponsorship info: {e}");
            }

            // Log the quote response & emit metrics
            if let Err(e) = server_clone.handle_quote_response(
                key_desc,
                &body,
                &headers,
                &sponsored_quote_response,
            ) {
                warn!("Error handling quote: {e}");
            }
        });

        Ok(resp)
    }

    /// Handle an external quote-assembly request
    #[instrument(skip(self, path, headers, body))]
    pub async fn handle_external_quote_assembly_request(
        &self,
        path: warp::path::FullPath,
        headers: warp::hyper::HeaderMap,
        body: Bytes,
        query_str: String,
    ) -> Result<impl Reply, Rejection> {
        // Authorize the request
        let path_str = path.as_str();
        let key_desc = self.authorize_request(path_str, &query_str, &headers, &body).await?;
        self.check_bundle_rate_limit(key_desc.clone()).await?;

        // Update the request to remove the effects of gas sponsorship, if
        // necessary
        let (assemble_match_req, gas_sponsorship_info) =
            self.maybe_update_assembly_request(&body).await?;

        let req_body = serde_json::to_vec(&assemble_match_req).map_err(AuthServerError::serde)?;

        // Send the request to the relayer
        let mut resp = self
            .send_admin_request(Method::POST, path_str, headers.clone(), req_body.clone().into())
            .await?;

        let status = resp.status();
        if status == StatusCode::INTERNAL_SERVER_ERROR {
            record_relayer_request_500(key_desc.clone(), path_str.to_string());
        }
        if status != StatusCode::OK {
            log_unsuccessful_relayer_request(&resp, &key_desc, path_str, &req_body, &headers);
            return Ok(resp);
        }

        // Parse query params
        let query_params = serde_urlencoded::from_str::<GasSponsorshipQueryParams>(&query_str)
            .map_err(AuthServerError::serde)?;

        // Apply gas sponsorship to the resulting bundle, if necessary
        let sponsored_match_resp = self
            .maybe_apply_gas_sponsorship_to_assembled_quote(
                key_desc.clone(),
                resp.body(),
                query_params,
                gas_sponsorship_info,
            )
            .await?;

        overwrite_response_body(&mut resp, sponsored_match_resp.clone())?;

        let server_clone = self.clone();
        tokio::spawn(async move {
            if let Err(e) = server_clone
                .handle_quote_assembly_bundle_response(
                    key_desc,
                    assemble_match_req,
                    &headers,
                    sponsored_match_resp,
                )
                .await
            {
                warn!("Error handling bundle: {e}");
            };
        });

        Ok(resp)
    }

    /// Handle an external match request
    #[instrument(skip(self, path, headers, body))]
    pub async fn handle_external_match_request(
        &self,
        path: warp::path::FullPath,
        headers: warp::hyper::HeaderMap,
        body: Bytes,
        query_str: String,
    ) -> Result<impl Reply, Rejection> {
        // Authorize the request
        let path_str = path.as_str();
        let key_description = self.authorize_request(path_str, &query_str, &headers, &body).await?;

        self.check_bundle_rate_limit(key_description.clone()).await?;

        // Send the request to the relayer, potentially sponsoring the gas costs

        let mut resp =
            self.send_admin_request(Method::POST, path_str, headers.clone(), body.clone()).await?;

        let status = resp.status();
        if status == StatusCode::INTERNAL_SERVER_ERROR {
            record_relayer_request_500(key_description.clone(), path_str.to_string());
        }
        if status != StatusCode::OK {
            log_unsuccessful_relayer_request(&resp, &key_description, path_str, &body, &headers);
            return Ok(resp);
        }

        let query_params = serde_urlencoded::from_str::<GasSponsorshipQueryParams>(&query_str)
            .map_err(AuthServerError::serde)?;

        let sponsored_match_resp = self
            .maybe_apply_gas_sponsorship_to_match(
                key_description.clone(),
                resp.body(),
                &query_params,
            )
            .await?;

        overwrite_response_body(&mut resp, sponsored_match_resp.clone())?;

        // Watch the bundle for settlement
        let server_clone = self.clone();
        tokio::spawn(async move {
            if let Err(e) = server_clone
                .handle_direct_match_bundle_response(
                    key_description,
                    &body,
                    &headers,
                    sponsored_match_resp,
                )
                .await
            {
                warn!("Error handling bundle: {e}");
            };
        });

        Ok(resp)
    }

    // --- Sponsorship --- //

    /// Potentially apply gas sponsorship to the given
    /// external quote, returning the resulting `SponsoredQuoteResponse`
    async fn maybe_apply_gas_sponsorship_to_quote(
        &self,
        key_description: String,
        resp_body: &[u8],
        query_params: &GasSponsorshipQueryParams,
    ) -> Result<SponsoredQuoteResponse, AuthServerError> {
        // Parse query params.
        let (sponsorship_disabled, refund_address, refund_native_eth) =
            query_params.get_or_default();

        // Check gas sponsorship rate limit
        let gas_sponsorship_rate_limited =
            !self.check_gas_sponsorship_rate_limit(key_description).await;

        let sponsor_match = !(gas_sponsorship_rate_limited || sponsorship_disabled);

        // Parse response body
        let external_quote_response: ExternalQuoteResponse =
            serde_json::from_slice(resp_body).map_err(AuthServerError::serde)?;

        // Whether or not sponsorship was requested, we return a
        // `SponsoredQuoteResponse`. This simply has one extra field,
        // `signed_gas_sponsorship_info`, which we expect clients to ignore if they did
        // not request sponsorship.

        if !sponsor_match {
            return Ok(SponsoredQuoteResponse {
                signed_quote: external_quote_response.signed_quote,
                gas_sponsorship_info: None,
            });
        }

        let sponsored_quote_response = self
            .construct_sponsored_quote_response(
                external_quote_response,
                refund_native_eth,
                refund_address,
            )
            .await?;

        Ok(sponsored_quote_response)
    }

    /// Check if the given assembly request pertains to a sponsored quote,
    /// and if so, remove the effects of gas sponsorship from the quote.
    ///
    /// Returns the assembly request, and the gas sponsorship info, if any.
    async fn maybe_update_assembly_request(
        &self,
        req_body: &[u8],
    ) -> Result<(AssembleExternalMatchRequest, Option<GasSponsorshipInfo>), AuthServerError> {
        let mut assemble_match_req: AssembleExternalMatchRequest =
            serde_json::from_slice(req_body).map_err(AuthServerError::serde)?;

        let redis_key = generate_quote_uuid(&assemble_match_req.signed_quote);
        let gas_sponsorship_info = match self.read_gas_sponsorship_info_from_redis(redis_key).await
        {
            Err(e) => {
                error!("Error reading gas sponsorship info from Redis: {e}");
                None
            },
            Ok(gas_sponsorship_info) => gas_sponsorship_info,
        };

        if let Some(ref gas_sponsorship_info) = gas_sponsorship_info
            && gas_sponsorship_info.requires_match_result_update()
        {
            // Reconstruct original signed quote
            let quote = &mut assemble_match_req.signed_quote.quote;
            self.remove_gas_sponsorship_from_quote(quote, gas_sponsorship_info.refund_amount);
        }

        Ok((assemble_match_req, gas_sponsorship_info))
    }

    /// Apply gas sponsorship to the given assembled quote, returning the
    /// resulting `SponsoredMatchResponse`. We don't
    /// check the gas sponsorship rate limit here, since this is checked during
    /// the quote request stage.
    #[allow(deprecated)]
    async fn maybe_apply_gas_sponsorship_to_assembled_quote(
        &self,
        key_description: String,
        resp_body: &[u8],
        mut query_params: GasSponsorshipQueryParams,
        gas_sponsorship_info: Option<GasSponsorshipInfo>,
    ) -> Result<SponsoredMatchResponse, AuthServerError> {
        // Parse response body
        let external_match_resp: ExternalMatchResponse =
            serde_json::from_slice(resp_body).map_err(AuthServerError::serde)?;

        // Whether or not the quote was sponsored, we return a
        // `SponsoredMatchResponse`. This simply has one extra field,
        // `is_sponsored`, which we expect clients to ignore if they did not
        // request sponsorship.
        let sponsored_match_resp = if let Some(gas_sponsorship_info) = gas_sponsorship_info {
            info!("Sponsoring assembled quote bundle via gas sponsor");

            let refund_native_eth = gas_sponsorship_info.refund_native_eth;
            let refund_address = gas_sponsorship_info.get_refund_address();
            let refund_amount = gas_sponsorship_info.get_refund_amount();

            self.construct_sponsored_match_response(
                external_match_resp,
                refund_native_eth,
                refund_address,
                refund_amount,
            )?
        } else if query_params.use_gas_sponsorship.unwrap_or(false) {
            // If the deprecated `use_gas_sponsorship` query parameter is set, it means an
            // outdated client must have been used to invoke the assembly endpoint.
            // In this case, for backwards compatibility, we use the deprecated default of
            // native ETH refunds.
            query_params.refund_native_eth = Some(true);

            return self
                .maybe_apply_gas_sponsorship_to_match(key_description, resp_body, &query_params)
                .await;
        } else {
            SponsoredMatchResponse {
                match_bundle: external_match_resp.match_bundle,
                is_sponsored: false,
                gas_sponsorship_info: None,
            }
        };

        Ok(sponsored_match_resp)
    }

    /// Potentially apply gas sponsorship to the given
    /// external match, returning the resulting `SponsoredMatchResponse`
    async fn maybe_apply_gas_sponsorship_to_match(
        &self,
        key_description: String,
        resp_body: &[u8],
        query_params: &GasSponsorshipQueryParams,
    ) -> Result<SponsoredMatchResponse, AuthServerError> {
        // Parse query params
        let (sponsorship_omitted, refund_address, refund_native_eth) =
            query_params.get_or_default();

        // Check gas sponsorship rate limit
        let gas_sponsorship_rate_limited =
            !self.check_gas_sponsorship_rate_limit(key_description).await;

        let sponsor_match = !(gas_sponsorship_rate_limited || sponsorship_omitted);

        // Parse response body
        let external_match_resp: ExternalMatchResponse =
            serde_json::from_slice(resp_body).map_err(AuthServerError::serde)?;

        // Whether or not sponsorship was requested, we return a
        // `SponsoredMatchResponse`. This simply has one extra field,
        // `is_sponsored`, which we expect clients to ignore if they did not
        // request sponsorship.

        if !sponsor_match {
            return Ok(SponsoredMatchResponse {
                match_bundle: external_match_resp.match_bundle,
                is_sponsored: false,
                gas_sponsorship_info: None,
            });
        }

        info!("Sponsoring match bundle via gas sponsor");

        // Compute refund amount
        let refund_amount = self
            .get_refund_amount(&external_match_resp.match_bundle.match_result, refund_native_eth)
            .await?;

        let sponsored_match_resp = self.construct_sponsored_match_response(
            external_match_resp,
            refund_native_eth,
            refund_address,
            refund_amount,
        )?;

        Ok(sponsored_match_resp)
    }

    // --- Bundle Tracking --- //

    /// Handle a bundle response from a quote assembly request
    async fn handle_quote_assembly_bundle_response(
        &self,
        key: String,
        req: AssembleExternalMatchRequest,
        headers: &HeaderMap,
        resp: SponsoredMatchResponse,
    ) -> Result<(), AuthServerError> {
        let original_order = &req.signed_quote.quote.order;
        let updated_order = req.updated_order.as_ref().unwrap_or(original_order);

        let request_id = uuid::Uuid::new_v4().to_string();
        let sdk_version = get_sdk_version(headers);
        if req.updated_order.is_some() {
            log_updated_order(&key, original_order, updated_order, &request_id, &sdk_version);
        }

        self.handle_bundle_response(
            key,
            updated_order.clone(),
            resp,
            Some(request_id),
            "assemble-external-match",
            sdk_version,
        )
        .await
    }

    /// Handle a bundle response from a direct match request
    async fn handle_direct_match_bundle_response(
        &self,
        key: String,
        req: &[u8],
        headers: &HeaderMap,
        resp: SponsoredMatchResponse,
    ) -> Result<(), AuthServerError> {
        let req: ExternalMatchRequest =
            serde_json::from_slice(req).map_err(AuthServerError::serde)?;
        let order = req.external_order;
        let sdk_version = get_sdk_version(headers);
        self.handle_bundle_response(key, order, resp, None, "request-external-match", sdk_version)
            .await
    }

    /// Record and watch a bundle that was forwarded to the client
    ///
    /// This method will await settlement and update metrics, rate limits, etc
    async fn handle_bundle_response(
        &self,
        key: String,
        order: ExternalOrder,
        resp: SponsoredMatchResponse,
        request_id: Option<String>,
        endpoint: &str,
        sdk_version: String,
    ) -> Result<(), AuthServerError> {
        // Log the bundle
        let request_id = request_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        log_bundle(&order, &resp, &key, &request_id, endpoint, &sdk_version)?;

        // Note: if sponsored in-kind w/ refund going to the receiver,
        // the amounts in the match bundle will have been updated
        let SponsoredMatchResponse { match_bundle, is_sponsored, gas_sponsorship_info } = resp;

        let labels = vec![
            (KEY_DESCRIPTION_METRIC_TAG.to_string(), key.clone()),
            (REQUEST_ID_METRIC_TAG.to_string(), request_id.clone()),
            (DECIMAL_CORRECTION_FIXED_METRIC_TAG.to_string(), "true".to_string()),
            (GAS_SPONSORED_METRIC_TAG.to_string(), is_sponsored.to_string()),
            (SDK_VERSION_METRIC_TAG.to_string(), sdk_version.clone()),
        ];

        // Record quote comparisons before settlement, if enabled
        if let Some(quote_metrics) = self.quote_metrics.clone() {
            let bundle_clone = match_bundle.clone();
            let labels_clone = labels.clone();

            // Record quote comparisons concurrently, so as to not interfere with awaiting
            // settlement
            tokio::spawn(async move {
                quote_metrics.record_quote_comparison(&bundle_clone, &labels_clone).await;
            });
        }

        // If the bundle settles, increase the API user's a rate limit token balance
        let did_settle = await_settlement(&match_bundle, &self.arbitrum_client).await?;
        if did_settle {
            self.add_bundle_rate_limit_token(key.clone()).await;
            if let Some(gas_sponsorship_info) = gas_sponsorship_info {
                self.record_settled_match_sponsorship(
                    &match_bundle,
                    gas_sponsorship_info,
                    key,
                    request_id,
                    sdk_version,
                )
                .await?;
            }
        }

        // Record metrics
        record_external_match_metrics(&order, &match_bundle, &labels, did_settle)?;

        Ok(())
    }

    /// Handle a quote response
    fn handle_quote_response(
        &self,
        key: String,
        req: &[u8],
        headers: &HeaderMap,
        resp: &SponsoredQuoteResponse,
    ) -> Result<(), AuthServerError> {
        let sdk_version = get_sdk_version(headers);

        // Log the quote parameters
        log_quote(resp, &key, &sdk_version)?;

        // Only proceed with metrics recording if sampled
        if !self.should_sample_metrics() {
            return Ok(());
        }

        // Parse request and response for metrics
        let req: ExternalQuoteRequest =
            serde_json::from_slice(req).map_err(AuthServerError::serde)?;

        // Get the decimal-corrected price
        let price: TimestampedPrice = resp.signed_quote.quote.price.clone().into();

        let relayer_fee = FixedPoint::from_f64_round_down(EXTERNAL_MATCH_RELAYER_FEE);

        // Calculate requested and matched quote amounts
        let requested_quote_amount = req
            .external_order
            .get_quote_amount(FixedPoint::from_f64_round_down(price.price), relayer_fee);

        let matched_quote_amount = resp.signed_quote.quote.match_result.quote_amount;

        // Record fill ratio metric
        let request_id = uuid::Uuid::new_v4();
        let labels = vec![
            (KEY_DESCRIPTION_METRIC_TAG.to_string(), key),
            (REQUEST_ID_METRIC_TAG.to_string(), request_id.to_string()),
            (DECIMAL_CORRECTION_FIXED_METRIC_TAG.to_string(), "true".to_string()),
            (SDK_VERSION_METRIC_TAG.to_string(), sdk_version),
        ];
        record_fill_ratio(requested_quote_amount, matched_quote_amount, &labels)?;

        // Record endpoint metrics
        let base_token = Token::from_addr_biguint(&req.external_order.base_mint);
        record_endpoint_metrics(&base_token.addr, EXTERNAL_MATCH_QUOTE_REQUEST_COUNT, &labels);

        Ok(())
    }
}

// -------------------
// | Logging helpers |
// -------------------

/// Parse the SDK version from the given headers.
/// If unset or malformed, returns an empty string.
fn get_sdk_version(headers: &HeaderMap) -> String {
    headers
        .get(SDK_VERSION_HEADER)
        .map(|v| v.to_str().unwrap_or_default())
        .unwrap_or(SDK_VERSION_DEFAULT)
        .to_string()
}

/// Log a non-200 response from the relayer for the given request
fn log_unsuccessful_relayer_request(
    resp: &Response<Bytes>,
    key_description: &str,
    path: &str,
    req_body: &[u8],
    headers: &HeaderMap,
) {
    let status = resp.status();
    let text = String::from_utf8_lossy(resp.body()).to_string();
    let req_body = String::from_utf8_lossy(req_body).to_string();
    let sdk_version = get_sdk_version(headers);
    warn!(
        key_description = key_description,
        path = path,
        request_body = req_body,
        sdk_version = sdk_version,
        "Non-200 response from relayer: {status}: {text}",
    );
}

/// Log a quote
fn log_quote(
    resp: &SponsoredQuoteResponse,
    key_description: &str,
    sdk_version: &str,
) -> Result<(), AuthServerError> {
    let SponsoredQuoteResponse { signed_quote, gas_sponsorship_info } = resp;
    let match_result = signed_quote.match_result();
    let is_buy = match_result.direction;
    let recv = signed_quote.receive_amount();
    let send = signed_quote.send_amount();
    let is_sponsored = gas_sponsorship_info.is_some();
    let (refund_amount, refund_native_eth) = gas_sponsorship_info
        .as_ref()
        .map(|s| (s.gas_sponsorship_info.refund_amount, s.gas_sponsorship_info.refund_native_eth))
        .unwrap_or((0, false));

    info!(
            is_sponsored = is_sponsored,
            key_description = key_description,
            sdk_version = sdk_version,
            "Sending quote(is_buy: {is_buy}, receive: {} ({}), send: {} ({}), refund_amount: {} (refund_native_eth: {})) to client",
            recv.amount,
            recv.mint,
            send.amount,
            send.mint,
            refund_amount,
            refund_native_eth
        );

    Ok(())
}

/// Log an updated order
fn log_updated_order(
    key: &str,
    original_order: &ExternalOrder,
    updated_order: &ExternalOrder,
    request_id: &str,
    sdk_version: &str,
) {
    let original_base_amount = original_order.base_amount;
    let updated_base_amount = updated_order.base_amount;
    let original_quote_amount = original_order.quote_amount;
    let updated_quote_amount = updated_order.quote_amount;
    info!(
            key_description = key,
            request_id = request_id,
            sdk_version = sdk_version,
            "Quote updated(original_base_amount: {}, updated_base_amount: {}, original_quote_amount: {}, updated_quote_amount: {})",
            original_base_amount, updated_base_amount, original_quote_amount, updated_quote_amount
        );
}

/// Log the bundle parameters
fn log_bundle(
    order: &ExternalOrder,
    resp: &SponsoredMatchResponse,
    key_description: &str,
    request_id: &str,
    endpoint: &str,
    sdk_version: &str,
) -> Result<(), AuthServerError> {
    let SponsoredMatchResponse { match_bundle, is_sponsored, gas_sponsorship_info } = resp;

    // Get the decimal-corrected price
    let price = calculate_implied_price(match_bundle, true /* decimal_correct */)?;
    let price_fixed = FixedPoint::from_f64_round_down(price);

    let match_result = &match_bundle.match_result;
    let is_buy = match_result.direction;
    let recv = &match_bundle.receive;
    let send = &match_bundle.send;

    let relayer_fee = FixedPoint::from_f64_round_down(EXTERNAL_MATCH_RELAYER_FEE);

    // Get the base fill ratio
    let requested_base_amount = order.get_base_amount(price_fixed, relayer_fee);
    let response_base_amount = match_result.base_amount;
    let base_fill_ratio = response_base_amount as f64 / requested_base_amount as f64;

    // Get the quote fill ratio
    let requested_quote_amount = order.get_quote_amount(price_fixed, relayer_fee);
    let response_quote_amount = match_result.quote_amount;
    let quote_fill_ratio = response_quote_amount as f64 / requested_quote_amount as f64;

    // Get the gas sponsorship info
    let (refund_amount, refund_native_eth) = gas_sponsorship_info
        .as_ref()
        .map(|info| (info.refund_amount, info.refund_native_eth))
        .unwrap_or((0, false));

    info!(
            requested_base_amount = requested_base_amount,
            response_base_amount = response_base_amount,
            requested_quote_amount = requested_quote_amount,
            response_quote_amount = response_quote_amount,
            base_fill_ratio = base_fill_ratio,
            quote_fill_ratio = quote_fill_ratio,
            key_description = key_description,
            request_id = request_id,
            is_sponsored = is_sponsored,
            endpoint = endpoint,
            sdk_version = sdk_version,
            "Sending bundle(is_buy: {}, recv: {} ({}), send: {} ({}), refund_amount: {} (refund_native_eth: {})) to client",
            is_buy,
            recv.amount,
            recv.mint,
            send.amount,
            send.mint,
            refund_amount,
            refund_native_eth
        );

    Ok(())
}
