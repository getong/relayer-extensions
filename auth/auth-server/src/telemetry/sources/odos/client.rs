use super::types::{OdosQuoteRequest, OdosQuoteResponse};
use crate::telemetry::sources::http_utils::{send_request, HttpError};

// -------------
// | Constants |
// -------------

/// Base URL for the Odos API
const BASE_URL: &str = "https://api.odos.xyz";
/// API endpoint for fetching quotes
const QUOTE_ROUTE: &str = "/sor/quote/v2";

// Default configuration values
const DEFAULT_CHAIN_ID: u64 = 42161; // Arbitrum
const DEFAULT_DISABLE_RFQS: bool = false;
const DEFAULT_SLIPPAGE_LIMIT_PERCENT: f64 = 0.3;
const DEFAULT_TIMEOUT_SECS: u64 = 5;

/// Configuration options for the Odos client
#[derive(Debug, Clone)]
pub struct OdosConfig {
    /// Chain ID for the target blockchain (e.g., 42161 for Arbitrum)
    pub chain_id: u64,
    /// Whether to disable RFQs (Request for Quotes)
    pub disable_rfqs: bool,
    /// Maximum allowed slippage as a percentage
    pub slippage_limit_percent: f64,
    /// Request timeout in seconds
    pub timeout_secs: u64,
}

impl Default for OdosConfig {
    fn default() -> Self {
        Self {
            chain_id: DEFAULT_CHAIN_ID,
            disable_rfqs: DEFAULT_DISABLE_RFQS,
            slippage_limit_percent: DEFAULT_SLIPPAGE_LIMIT_PERCENT,
            timeout_secs: DEFAULT_TIMEOUT_SECS,
        }
    }
}

// ----------
// | Client |
// ----------

/// HTTP client for interacting with the Odos API
#[derive(Debug, Clone)]
pub(super) struct OdosClient {
    /// Configuration settings
    config: OdosConfig,
}

impl OdosClient {
    /// Creates a new OdosClient instance with the given configuration
    pub fn new(config: OdosConfig) -> Self {
        Self { config }
    }

    /// Makes an HTTP request to the Odos API to fetch a quote
    pub async fn get_quote(
        &self,
        in_token: &str,
        in_amount: u128,
        out_token: &str,
    ) -> Result<OdosQuoteResponse, HttpError> {
        let request = OdosQuoteRequest::new(
            &self.config,
            in_token.to_string(),
            in_amount,
            out_token.to_string(),
        );

        let url = format!("{}{}", BASE_URL, QUOTE_ROUTE);
        let response = send_request(&url, request, self.config.timeout_secs).await?;

        response
            .json::<OdosQuoteResponse>()
            .await
            .map_err(|e| HttpError::Network("Failed to parse response".to_string(), e))
    }
}
