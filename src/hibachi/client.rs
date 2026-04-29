use anyhow::{Context, Result, bail};
use reqwest::Client;
use sha2::{Digest, Sha256};

use super::types::*;

// ---------------------------------------------------------------------------
// Contract metadata (resolved at init from exchange-info)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ContractInfo {
    pub id: u32,
    pub symbol: String,
    pub underlying_decimals: u32,
    pub settlement_decimals: u32,
    pub tick_size: f64,
    pub lot_size: f64,
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

pub struct HibachiClient {
    http: Client,
    api_url: String,
    data_api_url: String,
    pub api_key: String,
    pub account_id: u64,
    private_key: Vec<u8>,
    is_ecdsa: bool,
}

impl HibachiClient {
    pub fn new(
        api_key: String,
        account_id: u64,
        private_key: String,
        api_url: Option<String>,
        data_api_url: Option<String>,
    ) -> Result<Self> {
        let api_url = api_url.unwrap_or_else(|| "https://api.hibachi.xyz".to_string());
        let data_api_url = data_api_url.unwrap_or_else(|| "https://data-api.hibachi.xyz".to_string());

        let is_ecdsa = private_key.starts_with("0x");
        let key_bytes = if is_ecdsa {
            hex::decode(private_key.strip_prefix("0x").unwrap_or(&private_key))
                .context("failed to decode ECDSA private key")?
        } else {
            private_key.into_bytes()
        };

        Ok(Self {
            http: Client::new(),
            api_url,
            data_api_url,
            api_key,
            account_id,
            private_key: key_bytes,
            is_ecdsa,
        })
    }

    pub fn api_url(&self) -> &str {
        &self.api_url
    }

    pub fn data_api_url(&self) -> &str {
        &self.data_api_url
    }

    pub fn trade_ws_url(&self) -> String {
        let base = self.api_url.replace("https://", "wss://").replace("http://", "ws://");
        format!("{base}/ws/trade?accountId={}", self.account_id)
    }

    pub fn account_ws_url(&self) -> String {
        let base = self.api_url.replace("https://", "wss://").replace("http://", "ws://");
        format!("{base}/ws/account?accountId={}", self.account_id)
    }

    // -----------------------------------------------------------------------
    // Signing
    // -----------------------------------------------------------------------

    /// Sign a binary payload using ECDSA (SHA256 + secp256k1) or HMAC-SHA256.
    pub fn sign(&self, payload: &[u8]) -> Result<String> {
        if self.is_ecdsa {
            self.sign_ecdsa(payload)
        } else {
            self.sign_hmac(payload)
        }
    }

    fn sign_ecdsa(&self, payload: &[u8]) -> Result<String> {
        use alloy::signers::SignerSync;
        use alloy::primitives::B256;

        let signer = alloy::signers::local::PrivateKeySigner::from_bytes(
            &B256::from_slice(&self.private_key),
        )?;

        // SHA256 hash the payload
        let hash = Sha256::digest(payload);
        let hash_b256 = B256::from_slice(&hash);

        // Sign the hash directly (sign_hash, not sign_message which would keccak-prefix)
        let sig = signer.sign_hash_sync(&hash_b256)?;

        // r (32) || s (32) || v (1) = 65 bytes → 130 hex chars
        let r = sig.r();
        let s = sig.s();
        let v = sig.v();
        let v_byte: u8 = if v { 1 } else { 0 };
        Ok(format!("{:064x}{:064x}{:02x}", r, s, v_byte))
    }

    fn sign_hmac(&self, payload: &[u8]) -> Result<String> {
        use hmac::{Hmac, Mac};
        type HmacSha256 = Hmac<Sha256>;

        let mut mac = HmacSha256::new_from_slice(&self.private_key)
            .map_err(|e| anyhow::anyhow!("HMAC init failed: {e}"))?;
        mac.update(payload);
        let result = mac.finalize();
        Ok(hex::encode(result.into_bytes()))
    }

    /// Build the binary payload for order placement/modification signing.
    pub fn build_order_payload(
        &self,
        nonce: u64,
        contract_id: u32,
        quantity_raw: u64,
        side_code: u32, // 0=ASK, 1=BID
        price_raw: Option<u64>, // None for market orders
        max_fees_raw: u64,
    ) -> Vec<u8> {
        let mut buf = Vec::with_capacity(40);
        buf.extend_from_slice(&nonce.to_be_bytes());
        buf.extend_from_slice(&contract_id.to_be_bytes());
        buf.extend_from_slice(&quantity_raw.to_be_bytes());
        buf.extend_from_slice(&side_code.to_be_bytes());
        if let Some(p) = price_raw {
            buf.extend_from_slice(&p.to_be_bytes());
        }
        buf.extend_from_slice(&max_fees_raw.to_be_bytes());
        buf
    }

    /// Build the binary payload for cancel signing.
    pub fn build_cancel_payload(&self, order_id: u64) -> Vec<u8> {
        order_id.to_be_bytes().to_vec()
    }

    /// Generate a nonce (microsecond timestamp).
    pub fn gen_nonce() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        now.as_micros() as u64
    }

    // -----------------------------------------------------------------------
    // REST API
    // -----------------------------------------------------------------------

    fn auth_headers(&self) -> reqwest::header::HeaderMap {
        let mut h = reqwest::header::HeaderMap::new();
        h.insert("Authorization", self.api_key.parse().unwrap());
        h.insert("Content-Type", "application/json".parse().unwrap());
        h.insert("Accept", "application/json".parse().unwrap());
        h
    }

    pub async fn get_exchange_info(&self) -> Result<ExchangeInfoResponse> {
        let url = format!("{}/market/exchange-info", self.data_api_url);
        let resp = self.http.get(&url).send().await?;
        if resp.status() == 429 {
            let retry_after = resp.headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(60);
            anyhow::bail!("429 Too Many Requests (retry-after: {retry_after}s)");
        }
        let resp = resp.error_for_status()?;
        Ok(resp.json().await?)
    }

    pub async fn get_account_info(&self) -> Result<AccountInfoResponse> {
        let url = format!("{}/trade/account/info", self.api_url);
        let resp = self.http
            .get(&url)
            .headers(self.auth_headers())
            .query(&[("accountId", self.account_id.to_string())])
            .send()
            .await?
            .error_for_status()?;
        Ok(resp.json().await?)
    }

    pub async fn get_pending_orders(&self) -> Result<Vec<HibachiOrder>> {
        let url = format!("{}/trade/orders", self.api_url);
        let resp = self.http
            .get(&url)
            .headers(self.auth_headers())
            .query(&[("accountId", self.account_id.to_string())])
            .send()
            .await?
            .error_for_status()?;
        Ok(resp.json().await?)
    }

    pub async fn place_order_rest(
        &self,
        body: serde_json::Value,
    ) -> Result<PlaceOrderResponse> {
        let url = format!("{}/trade/order", self.api_url);
        let resp = self.http
            .post(&url)
            .headers(self.auth_headers())
            .json(&body)
            .send()
            .await?;
        let status = resp.status();
        let text = resp.text().await?;
        if !status.is_success() {
            bail!("place_order failed ({}): {}", status, text);
        }
        serde_json::from_str(&text).context("failed to parse place_order response")
    }

    pub async fn cancel_order_rest(
        &self,
        body: serde_json::Value,
    ) -> Result<()> {
        let url = format!("{}/trade/order", self.api_url);
        let resp = self.http
            .delete(&url)
            .headers(self.auth_headers())
            .json(&body)
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await?;
            bail!("cancel_order failed ({}): {}", status, text);
        }
        Ok(())
    }

    pub async fn cancel_all_rest(&self, contract_id: Option<u32>) -> Result<()> {
        let url = format!("{}/trade/orders", self.api_url);
        let mut body = serde_json::json!({
            "accountId": self.account_id,
        });
        if let Some(cid) = contract_id {
            body["contractId"] = serde_json::json!(cid);
        }
        let nonce = Self::gen_nonce();
        let payload = nonce.to_be_bytes().to_vec();
        let signature = self.sign(&payload)?;
        body["nonce"] = serde_json::json!(nonce);
        body["signature"] = serde_json::json!(signature);

        let resp = self.http
            .delete(&url)
            .headers(self.auth_headers())
            .json(&body)
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await?;
            bail!("cancel_all failed ({}): {}", status, text);
        }
        Ok(())
    }

    pub async fn modify_order_rest(
        &self,
        body: serde_json::Value,
    ) -> Result<PlaceOrderResponse> {
        let url = format!("{}/trade/order", self.api_url);
        let resp = self.http
            .put(&url)
            .headers(self.auth_headers())
            .json(&body)
            .send()
            .await?;
        let status = resp.status();
        let text = resp.text().await?;
        if !status.is_success() {
            bail!("modify_order failed ({}): {}", status, text);
        }
        serde_json::from_str(&text).context("failed to parse modify_order response")
    }
}
