//! Handlers for executing swaps

use ethers::{
    providers::Middleware,
    signers::LocalWallet,
    types::{Eip1559TransactionRequest, TransactionReceipt},
};
use funds_manager_api::quoters::ExecutionQuote;
use tracing::info;

use crate::helpers::TransactionHash;

use super::{error::ExecutionClientError, ExecutionClient};

impl ExecutionClient {
    /// Execute a quoted swap
    pub async fn execute_swap(
        &self,
        quote: ExecutionQuote,
        wallet: &LocalWallet,
    ) -> Result<TransactionHash, ExecutionClientError> {
        // Execute the swap
        let receipt = self.execute_swap_tx(quote, wallet).await?;
        let tx_hash = receipt.transaction_hash;
        info!("Swap executed at {tx_hash:#x}");
        Ok(tx_hash)
    }

    /// Execute a swap
    async fn execute_swap_tx(
        &self,
        quote: ExecutionQuote,
        wallet: &LocalWallet,
    ) -> Result<TransactionReceipt, ExecutionClientError> {
        let client = self.get_signer(wallet.clone());
        let tx = Eip1559TransactionRequest::new()
            .to(quote.to)
            .from(quote.from)
            .value(quote.value)
            .data(quote.data);

        // Send the transaction
        let pending_tx = client
            .send_transaction(tx, None /* block */)
            .await
            .map_err(ExecutionClientError::arbitrum)?;
        pending_tx
            .await
            .map_err(ExecutionClientError::arbitrum)?
            .ok_or_else(|| ExecutionClientError::arbitrum("Transaction failed"))
    }
}
