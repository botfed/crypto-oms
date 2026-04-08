use std::fmt;

#[derive(Debug)]
pub enum OmsError {
    /// OMS not ready yet — still loading initial state
    NotReady,
    /// Exchange rejected the request
    ExchangeError(String),
    /// Order type or TIF not supported on this exchange
    UnsupportedOrderType(String),
    /// Order not found
    OrderNotFound(u64),
    /// Network / connectivity issue
    ConnectionError(String),
    /// Request timed out waiting for exchange ack
    Timeout(u64),
    /// Generic
    Internal(String),
}

impl fmt::Display for OmsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OmsError::NotReady => write!(f, "OMS not ready"),
            OmsError::ExchangeError(e) => write!(f, "exchange error: {e}"),
            OmsError::UnsupportedOrderType(e) => write!(f, "unsupported order type: {e}"),
            OmsError::OrderNotFound(id) => write!(f, "order not found: {id}"),
            OmsError::ConnectionError(e) => write!(f, "connection error: {e}"),
            OmsError::Timeout(id) => write!(f, "timeout waiting for ack on order {id}"),
            OmsError::Internal(e) => write!(f, "internal error: {e}"),
        }
    }
}

impl std::error::Error for OmsError {}
