//! Handlers for swap endpoints

use std::sync::Arc;
use std::time::Duration;

use funds_manager_api::quoters::{QuoteParams, SwapImmediateResponse, SwapIntoTargetTokenRequest};
use renegade_types_core::Chain;
use tracing::instrument;
use warp::reply::Json;

use crate::log_task;
use crate::logger::{Outcome, Task};
use crate::{error::ApiError, execution_client::error::ExecutionClientError, server::Server};

/// The default source tag for untagged swap requests
const DEFAULT_SOURCE: &str = "unknown";

/// Server-side wall-clock budget for a swap-into-target-token request. Set
/// below the funds-manager ALB idle timeout so a slow swap returns a structured
/// 500 the caller can classify, instead of the ALB emitting an opaque HTML 504.
/// Keep this strictly less than the ALB `idle_timeout` (renegade-infra
/// modules/funds-manager).
const SWAP_DEADLINE: Duration = Duration::from_secs(280);

/// Wrap an error as an `ApiError::InternalError` warp rejection so
/// `handle_rejection` renders a 500 carrying the error message, instead of
/// warp's opaque default "Unhandled rejection" body (which is unclassifiable
/// downstream — e.g. by the synthetic tester). Mirrors the pattern already used
/// in `handlers/quoters.rs`.
fn internal_rejection<E: std::fmt::Display>(e: E) -> warp::Rejection {
    warp::reject::custom(ApiError::InternalError(e.to_string()))
}

/// Handler for executing an immediate swap
#[instrument(skip_all)]
pub(crate) async fn swap_immediate_handler(
    chain: Chain,
    params: QuoteParams,
    server: Arc<Server>,
) -> Result<Json, warp::Rejection> {
    let execution_client = server.get_execution_client(&chain).map_err(internal_rejection)?;
    let custody_client = server.get_custody_client(&chain).map_err(internal_rejection)?;
    let metrics_recorder = server.get_metrics_recorder(&chain).map_err(internal_rejection)?;

    let source = params.source.clone().unwrap_or_else(|| DEFAULT_SOURCE.to_string());

    // Top up the quoter hot wallet gas before swapping
    custody_client.top_up_quoter_hot_wallet_gas().await.map_err(internal_rejection)?;

    // Execute the swap, decaying the size of the swap each time it fails to execute
    let outcome = execution_client
        .swap_immediate_decaying(params)
        .await
        .map_err(internal_rejection)?
        .ok_or_else(|| {
            internal_rejection(ExecutionClientError::custom("No swap executed".to_string()))
        })?;

    // Compute swap costs and respond
    let execution_cost = match metrics_recorder.record_swap_cost(&outcome, &source).await {
        Ok(data) => data.execution_cost_usdc,
        Err(e) => {
            log_task!(
                Task::RecordMetric,
                Outcome::Failed,
                metric = "swap-cost",
                error = %e,
                "failed to record swap cost metrics: {e}"
            );
            0.0 // Default to 0 USD
        },
    };

    Ok(warp::reply::json(&SwapImmediateResponse {
        quote: outcome.quote.into(),
        tx_hash: format!("{:#x}", outcome.tx_hash),
        execution_cost,
    }))
}

/// Handler for executing a swap to cover a target amount of a given token
#[instrument(skip_all)]
pub(crate) async fn swap_into_target_token_handler(
    chain: Chain,
    req: SwapIntoTargetTokenRequest,
    server: Arc<Server>,
) -> Result<Json, warp::Rejection> {
    let execution_client = server.get_execution_client(&chain).map_err(internal_rejection)?;
    let custody_client = server.get_custody_client(&chain).map_err(internal_rejection)?;
    let metrics_recorder = server.get_metrics_recorder(&chain).map_err(internal_rejection)?;

    let source = req.quote_params.source.clone().unwrap_or_else(|| DEFAULT_SOURCE.to_string());

    // Top up the quoter hot wallet gas before swapping
    custody_client.top_up_quoter_hot_wallet_gas().await.map_err(internal_rejection)?;

    // Execute the swap, decaying the size of the swap each time it fails to execute.
    // Bounded by a server-side deadline below the ALB idle timeout so a slow swap
    // returns a classifiable 500 rather than an opaque ALB 504.
    let outcomes = tokio::time::timeout(SWAP_DEADLINE, execution_client.try_swap_into_target_token(req))
        .await
        .map_err(|_| {
            internal_rejection(format!(
                "swap into target token exceeded {}s deadline",
                SWAP_DEADLINE.as_secs()
            ))
        })?
        .map_err(internal_rejection)?;

    // Compute swap costs and respond
    let mut responses = vec![];
    for outcome in outcomes {
        let execution_cost = match metrics_recorder.record_swap_cost(&outcome, &source).await {
            Ok(data) => data.execution_cost_usdc,
            Err(e) => {
                log_task!(
                    Task::RecordMetric,
                    Outcome::Failed,
                    metric = "swap-cost",
                    error = %e,
                    "failed to record swap cost metrics: {e}"
                );
                0.0 // Default to 0 USD
            },
        };

        responses.push(SwapImmediateResponse {
            quote: outcome.quote.into(),
            tx_hash: format!("{:#x}", outcome.tx_hash),
            execution_cost,
        });
    }

    Ok(warp::reply::json(&responses))
}
