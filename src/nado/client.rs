use alloy::primitives::{Address, B256, U256};
use alloy::signers::local::PrivateKeySigner;
use alloy::signers::Signer;
use alloy::sol;
use alloy::sol_types::SolStruct;
use anyhow::{Context, Result, bail};
use reqwest::Client;

use super::types::*;

// ---------------------------------------------------------------------------
// EIP-712 typed structs
// ---------------------------------------------------------------------------

sol! {
    #[derive(Debug)]
    struct Order {
        bytes32 sender;
        int128 priceX18;
        int128 amount;
        uint64 expiration;
        uint64 nonce;
        uint128 appendix;
    }

    #[derive(Debug)]
    struct Cancellation {
        bytes32 sender;
        uint32[] productIds;
        bytes32[] digests;
        uint64 nonce;
    }

    #[derive(Debug)]
    struct StreamAuthentication {
        bytes32 sender;
        uint64 expiration;
    }
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

pub struct NadoClient {
    http: Client,
    signer: PrivateKeySigner,
    pub account_address: String,
    /// Address derived from the signing private key
    pub signer_address: String,
    pub sender: String,      // 32-byte hex (address + subaccount)
    gateway_url: String,
    subscriptions_url: String,
    chain_id: Option<u64>,
    endpoint_addr: Option<String>,
}

impl NadoClient {
    pub fn new(
        private_key: &str,
        account_address: String,
        subaccount_name: &str,
        gateway_url: Option<String>,
        subscriptions_url: Option<String>,
    ) -> Result<Self> {
        let signer: PrivateKeySigner = private_key
            .parse()
            .context("failed to parse private key")?;
        let signer_address = format!("{}", signer.address());

        let sender = build_sender(&account_address, subaccount_name);
        let gateway_url = gateway_url
            .unwrap_or_else(|| "https://gateway.prod.nado.xyz/v1".to_string());
        let subscriptions_url = subscriptions_url
            .unwrap_or_else(|| "wss://gateway.prod.nado.xyz/v1/subscribe".to_string());

        Ok(Self {
            http: Client::new(),
            signer,
            account_address,
            signer_address,
            sender,
            gateway_url,
            subscriptions_url,
            chain_id: None,
            endpoint_addr: None,
        })
    }

    pub fn gateway_ws_url(&self) -> String {
        self.gateway_url
            .replace("https://", "wss://")
            .replace("http://", "ws://")
            + "/ws"
    }

    pub fn subscriptions_ws_url(&self) -> &str {
        &self.subscriptions_url
    }

    // -----------------------------------------------------------------------
    // Bootstrap: fetch chain_id and endpoint_addr
    // -----------------------------------------------------------------------

    pub async fn fetch_contracts(&mut self) -> Result<ContractsData> {
        let resp: GatewayResponse = self
            .http
            .get(format!("{}/query?type=contracts", self.gateway_url))
            .send()
            .await?
            .json()
            .await?;

        if resp.status != "success" {
            bail!("contracts query failed: {:?}", resp.error);
        }
        let data: ContractsData = serde_json::from_value(
            resp.data.context("no data in contracts response")?,
        )?;
        self.chain_id = Some(data.chain_id.parse().context("bad chain_id")?);
        self.endpoint_addr = Some(data.endpoint_addr.clone());
        Ok(data)
    }

    pub async fn fetch_pairs(&self) -> Result<Vec<PairRow>> {
        let pairs: Vec<PairRow> = self
            .http
            .get("https://gateway.prod.nado.xyz/v2/pairs")
            .header("Accept-Encoding", "gzip")
            .send()
            .await?
            .json()
            .await?;
        Ok(pairs)
    }

    // -----------------------------------------------------------------------
    // Queries
    // -----------------------------------------------------------------------

    pub async fn query_subaccount_info(&self) -> Result<SubaccountInfo> {
        let resp: GatewayResponse = self
            .http
            .post(format!("{}/query", self.gateway_url))
            .json(&serde_json::json!({
                "type": "subaccount_info",
                "subaccount": self.sender,
            }))
            .send()
            .await?
            .json()
            .await?;

        if resp.status != "success" {
            bail!("subaccount_info query failed: {:?}", resp.error);
        }
        let info: SubaccountInfo = serde_json::from_value(
            resp.data.context("no data in subaccount_info response")?,
        )?;
        Ok(info)
    }

    pub async fn query_linked_signer(&self) -> Result<String> {
        let resp: GatewayResponse = self
            .http
            .post(format!("{}/query", self.gateway_url))
            .json(&serde_json::json!({
                "type": "linked_signer",
                "subaccount": self.sender,
            }))
            .send()
            .await?
            .json()
            .await?;

        if resp.status != "success" {
            bail!("linked_signer query failed: {:?}", resp.error);
        }
        let data: LinkedSignerInfo = serde_json::from_value(
            resp.data.context("no data in linked_signer response")?,
        )?;
        Ok(data.linked_signer)
    }

    /// Returns true if the signer key matches the account address (i.e. main wallet).
    pub fn is_main_wallet(&self) -> bool {
        self.signer_address.eq_ignore_ascii_case(&self.account_address)
    }

    // -----------------------------------------------------------------------
    // Signing helpers
    // -----------------------------------------------------------------------

    fn eip712_domain(&self, verifying_contract: &str) -> alloy::sol_types::Eip712Domain {
        let chain_id = self.chain_id.unwrap_or(0);
        let contract: Address = verifying_contract.parse().unwrap_or(Address::ZERO);
        alloy::sol_types::Eip712Domain {
            name: Some(std::borrow::Cow::Borrowed("Nado")),
            version: Some(std::borrow::Cow::Borrowed("0.0.1")),
            chain_id: Some(U256::from(chain_id)),
            verifying_contract: Some(contract),
            salt: None,
        }
    }

    fn parse_sender_bytes32(&self) -> B256 {
        let hex = self.sender.strip_prefix("0x").unwrap_or(&self.sender);
        let bytes: [u8; 32] = hex::decode(hex)
            .unwrap_or_else(|_| vec![0u8; 32])
            .try_into()
            .unwrap_or([0u8; 32]);
        B256::from(bytes)
    }

    pub async fn sign_order(
        &self,
        product_id: u32,
        price_x18: i128,
        amount: i128,
        expiration: u64,
        nonce: u64,
        appendix: u128,
    ) -> Result<String> {
        let verifying_contract = order_verifying_contract(product_id);
        let domain = self.eip712_domain(&verifying_contract);

        let order = Order {
            sender: self.parse_sender_bytes32(),
            priceX18: price_x18,
            amount,
            expiration,
            nonce,
            appendix,
        };

        let signing_hash = order.eip712_signing_hash(&domain);
        let sig = self.signer.sign_hash(&signing_hash).await?;
        Ok(format!("0x{}", hex::encode(sig.as_bytes())))
    }

    pub async fn sign_cancellation(
        &self,
        product_ids: &[u32],
        digests: &[B256],
        nonce: u64,
    ) -> Result<String> {
        let endpoint = self.endpoint_addr.as_deref().unwrap_or("0x0000000000000000000000000000000000000000");
        let domain = self.eip712_domain(endpoint);

        let cancel = Cancellation {
            sender: self.parse_sender_bytes32(),
            productIds: product_ids.to_vec(),
            digests: digests.to_vec(),
            nonce,
        };

        let signing_hash = cancel.eip712_signing_hash(&domain);
        let sig = self.signer.sign_hash(&signing_hash).await?;
        Ok(format!("0x{}", hex::encode(sig.as_bytes())))
    }

    pub async fn sign_stream_auth(&self, expiration: u64) -> Result<String> {
        let endpoint = self.endpoint_addr.as_deref().unwrap_or("0x0000000000000000000000000000000000000000");
        let domain = self.eip712_domain(endpoint);

        let auth = StreamAuthentication {
            sender: self.parse_sender_bytes32(),
            expiration,
        };

        let signing_hash = auth.eip712_signing_hash(&domain);
        let sig = self.signer.sign_hash(&signing_hash).await?;
        Ok(format!("0x{}", hex::encode(sig.as_bytes())))
    }

    // -----------------------------------------------------------------------
    // Execute (REST)
    // -----------------------------------------------------------------------

    pub async fn place_order_execute(
        &self,
        product_id: u32,
        order: OrderFields,
        signature: String,
        id: u64,
    ) -> Result<GatewayResponse> {
        let msg = PlaceOrderMsg {
            place_order: PlaceOrderPayload {
                product_id,
                order,
                signature,
                id,
                spot_leverage: None,
            },
        };
        let resp: GatewayResponse = self
            .http
            .post(format!("{}/execute", self.gateway_url))
            .json(&msg)
            .send()
            .await?
            .json()
            .await?;
        Ok(resp)
    }

    pub async fn cancel_orders_execute(
        &self,
        tx: CancelTx,
        signature: String,
        id: Option<u64>,
    ) -> Result<GatewayResponse> {
        let msg = CancelOrdersMsg {
            cancel_orders: CancelOrdersPayload {
                tx,
                signature,
                id,
            },
        };
        let resp: GatewayResponse = self
            .http
            .post(format!("{}/execute", self.gateway_url))
            .json(&msg)
            .send()
            .await?
            .json()
            .await?;
        Ok(resp)
    }
}
