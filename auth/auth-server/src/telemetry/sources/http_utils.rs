//! Utilities for HTTP requests

use reqwest::{Client, Response};
use serde::Serialize;
use std::time::Duration;
use thiserror::Error;

/// An error with the HTTP client
#[derive(Debug, Error)]
pub enum HttpError {
    /// A network error
    #[error("Network error: {0}")]
    Network(String, #[source] reqwest::Error),

    /// An API error
    #[error("API error: {0}")]
    Api(String),

    /// A response parsing error
    #[error("Response parsing error: {0}")]
    Parsing(String),
}

impl HttpError {
    /// Create a new parsing error
    #[allow(clippy::needless_pass_by_value)]
    pub fn parsing<T: ToString>(msg: T) -> Self {
        Self::Parsing(msg.to_string())
    }
}

/// Sends a basic GET request
pub async fn send_get_request(url: &str, timeout_secs: u64) -> Result<Response, HttpError> {
    let client = Client::builder()
        .timeout(Duration::from_secs(timeout_secs))
        .build()
        .map_err(|e| HttpError::Network("Failed to create HTTP client".to_string(), e))?;

    let response = client.get(url).send().await.map_err(|e| {
        if e.is_timeout() {
            HttpError::Network(format!("Request timed out after {} seconds", timeout_secs), e)
        } else {
            HttpError::Network("Failed to send request".to_string(), e)
        }
    })?;

    if !response.status().is_success() {
        let status = response.status();
        let message = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
        return Err(HttpError::Api(format!("Status {}: {}", status, message)));
    }

    Ok(response)
}

/// Sends a basic POST request
pub async fn send_post_request<T: Serialize>(
    url: &str,
    body: Option<T>,
    timeout_secs: u64,
) -> Result<Response, HttpError> {
    let client = Client::builder()
        .timeout(Duration::from_secs(timeout_secs))
        .build()
        .map_err(|e| HttpError::Network("Failed to create HTTP client".to_string(), e))?;

    let response = client
        .post(url)
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await
        .map_err(|e| {
            if e.is_timeout() {
                HttpError::Network(format!("Request timed out after {} seconds", timeout_secs), e)
            } else {
                HttpError::Network("Failed to send request".to_string(), e)
            }
        })?;

    if !response.status().is_success() {
        let status = response.status();
        let message = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
        return Err(HttpError::Api(format!("Status {}: {}", status, message)));
    }

    Ok(response)
}
