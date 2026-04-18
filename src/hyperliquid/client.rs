use anyhow::{Context, Result, bail};
use reqwest::Client;

use super::types::*;

// Re-export SDK types used by the OMS
pub use hyperliquid_rust_sdk::{
    BaseUrl, ClientCancelRequest, ClientCancelRequestCloid, ClientLimit, ClientModifyRequest,
    ClientOrder, ClientOrderRequest, ClientTrigger, ExchangeClient, ExchangeResponseStatus,
};

// ---------------------------------------------------------------------------
// Client — info API (unsigned) + SDK ExchangeClient (signed)
// ---------------------------------------------------------------------------

pub struct HyperliquidClient {
    http: Client,
    base_url: String,
    /// The parent account (EOA) address — used for info queries and WS subscriptions.
    pub account_address: String,
    /// The API wallet address derived from the signing key.
    pub agent_address: String,
    /// The SDK exchange client for signed operations (order, cancel, modify).
    /// Initialized once during OMS init via init_exchange().
    exchange: tokio::sync::OnceCell<ExchangeClient>,
    /// Raw private key string, kept to initialize the SDK exchange client.
    private_key: String,
}

impl HyperliquidClient {
    pub fn new(
        private_key: &str,
        account_address: String,
        base_url: Option<String>,
    ) -> Result<Self> {
        // Parse the key just to extract the agent address
        let signer: alloy::signers::local::PrivateKeySigner = private_key
            .parse()
            .context("failed to parse private key")?;
        let agent_address = format!("{:#x}", signer.address());
        let base_url = base_url.unwrap_or_else(|| "https://api.hyperliquid.xyz".to_string());

        Ok(Self {
            http: Client::new(),
            base_url,
            account_address,
            agent_address,
            exchange: tokio::sync::OnceCell::new(),
            private_key: private_key.to_string(),
        })
    }

    /// Initialize the SDK exchange client. Call once during OMS init.
    /// Safe to call from &self (uses OnceCell).
    pub async fn init_exchange(&self) -> Result<()> {
        let pk = self.private_key.clone();
        let base_url = self.base_url.clone();

        self.exchange.get_or_try_init(|| async {
            let wallet: alloy::signers::local::PrivateKeySigner = pk
                .parse()
                .context("failed to parse private key for SDK wallet")?;

            let sdk_base = if base_url.contains("testnet") {
                BaseUrl::Testnet
            } else {
                BaseUrl::Mainnet
            };

            ExchangeClient::new(None, wallet, Some(sdk_base), None, None)
                .await
                .map_err(|e| anyhow::anyhow!("failed to init SDK exchange client: {e}"))
        }).await?;
        Ok(())
    }

    /// Get a reference to the SDK exchange client. Panics if not initialized.
    pub fn exchange(&self) -> &ExchangeClient {
        self.exchange.get().expect("SDK exchange client not initialized")
    }

    pub fn ws_url(&self) -> String {
        self.base_url
            .replace("https://", "wss://")
            .replace("http://", "ws://")
            + "/ws"
    }

    // -----------------------------------------------------------------------
    // Info API (unsigned) — kept as-is, uses our own reqwest client
    // -----------------------------------------------------------------------

    pub async fn info<T: serde::de::DeserializeOwned>(&self, req: &InfoRequest<'_>) -> Result<T> {
        let body = self.info_raw(req).await?;
        serde_json::from_str(&body)
            .with_context(|| format!("failed to parse info response: {body}"))
    }

    pub async fn info_raw(&self, req: &InfoRequest<'_>) -> Result<String> {
        let resp = self
            .http
            .post(format!("{}/info", self.base_url))
            .json(req)
            .send()
            .await
            .context("info request failed")?;

        let status = resp.status();
        let body = resp.text().await?;
        if !status.is_success() {
            bail!("info API error {status}: {body}");
        }
        Ok(body)
    }

    pub async fn fetch_meta(&self) -> Result<MetaResponse> {
        self.info(&InfoRequest::Meta).await
    }

    pub async fn fetch_open_orders(&self) -> Result<Vec<OpenOrderWire>> {
        self.info(&InfoRequest::OpenOrders { user: &self.account_address })
            .await
    }

    pub async fn fetch_clearinghouse_state(&self) -> Result<ClearinghouseResponse> {
        self.info(&InfoRequest::ClearinghouseState { user: &self.account_address })
            .await
    }

    pub async fn fetch_spot_state(&self) -> Result<SpotClearinghouseResponse> {
        self.info(&InfoRequest::SpotClearinghouseState { user: &self.account_address })
            .await
    }

    pub async fn fetch_user_fills(&self) -> Result<Vec<UserFillWire>> {
        self.info(&InfoRequest::UserFills { user: &self.account_address })
            .await
    }
}
