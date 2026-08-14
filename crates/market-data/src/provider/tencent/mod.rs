//! Eastmoney market data provider for A-shares and Hong Kong stocks.

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::errors::MarketDataError;
use crate::models::{Coverage, InstrumentKind, ProviderInstrument, Quote, QuoteContext, SearchResult};
use crate::provider::{MarketDataProvider, ProviderCapabilities, RateLimit};

// Keep the persisted provider ID stable. Existing installations and assets store TENCENT.
const PROVIDER_ID: &str = "TENCENT";
const QUOTE_URL: &str = "https://push2.eastmoney.com/api/qt/stock/get";
const KLINE_URL: &str = "https://push2his.eastmoney.com/api/qt/stock/kline/get";
const SEARCH_URL: &str = "https://searchapi.eastmoney.com/api/suggest/get";
const SEARCH_TOKEN: &str = "D43BF722C8E33BDC906FB84D85E326E8";

/// Kept under the old Rust type name so existing callers remain source-compatible.
pub struct TencentProvider { client: reqwest::Client }

impl TencentProvider {
    pub fn new() -> Self {
        let client = reqwest::Client::builder().timeout(Duration::from_secs(30)).build().unwrap_or_else(|_| reqwest::Client::new());
        Self { client }
    }

    fn extract_symbol(instrument: &ProviderInstrument) -> Result<String, MarketDataError> {
        match instrument {
            ProviderInstrument::EquitySymbol { symbol } => Self::normalize_symbol(symbol),
            _ => Err(MarketDataError::UnsupportedAssetType(format!("TENCENT only supports equities, got: {:?}", instrument))),
        }
    }

    /// Convert persisted Tencent/Yahoo/plain symbols to Eastmoney's `market.code` secid.
    /// This keeps assets created before the provider switch working without migration.
    fn normalize_symbol(symbol: &str) -> Result<String, MarketDataError> {
        let value = symbol.trim();
        let lower = value.to_ascii_lowercase();

        let normalized = if matches!(value.split_once('.'), Some(("0" | "1" | "116", _))) {
            value.to_string()
        } else if let Some(code) = lower.strip_prefix("sh") {
            format!("1.{}", code)
        } else if let Some(code) = lower.strip_prefix("sz") {
            format!("0.{}", code)
        } else if let Some(code) = lower.strip_prefix("hk") {
            format!("116.{:0>5}", code)
        } else if let Some(code) = lower.strip_suffix(".ss") {
            format!("1.{}", code)
        } else if let Some(code) = lower.strip_suffix(".sz") {
            format!("0.{}", code)
        } else if let Some(code) = lower.strip_suffix(".hk") {
            format!("116.{:0>5}", code)
        } else if value.len() == 6 && value.bytes().all(|byte| byte.is_ascii_digit()) {
            match value.as_bytes()[0] {
                b'5' | b'6' | b'9' => format!("1.{}", value),
                b'0' | b'1' | b'2' | b'3' => format!("0.{}", value),
                _ => return Err(MarketDataError::SymbolNotFound(value.to_string())),
            }
        } else if (1..=5).contains(&value.len()) && value.bytes().all(|byte| byte.is_ascii_digit()) {
            format!("116.{:0>5}", value)
        } else {
            return Err(MarketDataError::SymbolNotFound(value.to_string()));
        };

        Ok(normalized)
    }

    fn currency_for_symbol(symbol: &str) -> &'static str {
        if symbol.starts_with("116.") { "HKD" } else { "CNY" }
    }

    fn provider_error(message: impl Into<String>) -> MarketDataError {
        MarketDataError::ProviderError { provider: PROVIDER_ID.to_string(), message: message.into() }
    }

    async fn checked_response(&self, request: reqwest::RequestBuilder) -> Result<reqwest::Response, MarketDataError> {
        let response = request
            .header(reqwest::header::USER_AGENT, "Mozilla/5.0")
            .header(reqwest::header::ACCEPT, "application/json")
            .send().await?;
        if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(MarketDataError::RateLimited { provider: PROVIDER_ID.to_string() });
        }
        if !response.status().is_success() {
            return Err(Self::provider_error(format!("HTTP {} from Eastmoney", response.status())));
        }
        Ok(response)
    }

    fn parse_quote(symbol: &str, body: QuoteResponse) -> Result<Quote, MarketDataError> {
        if body.rc != 0 { return Err(Self::provider_error(format!("Quote API error {}", body.rc))); }
        let data = body.data.ok_or_else(|| MarketDataError::SymbolNotFound(symbol.to_string()))?;
        let scaled = |value: Option<i64>| value.map(|v| Decimal::from(v) / Decimal::from(100));
        let close = scaled(data.close).ok_or_else(|| MarketDataError::SymbolNotFound(symbol.to_string()))?;
        let timestamp = data.timestamp.and_then(|v| Utc.timestamp_opt(v, 0).single()).unwrap_or_else(Utc::now);
        Ok(Quote {
            timestamp, open: scaled(data.open), high: scaled(data.high), low: scaled(data.low), close,
            volume: data.volume.map(Decimal::from), currency: Self::currency_for_symbol(symbol).to_string(),
            source: PROVIDER_ID.to_string(),
        })
    }

    async fn fetch_kline(&self, symbol: &str, start: NaiveDate, end: NaiveDate) -> Result<Vec<Quote>, MarketDataError> {
        let days = (end - start).num_days().max(1);
        let count = (days + 10).clamp(20, 100_000);
        let begin = start.format("%Y%m%d").to_string();
        let finish = end.format("%Y%m%d").to_string();
        let limit = count.to_string();
        let response = self.checked_response(self.client.get(KLINE_URL).query(&[
            ("secid", symbol), ("klt", "101"), ("fqt", "1"),
            ("beg", begin.as_str()), ("end", finish.as_str()),
            ("lmt", limit.as_str()), ("fields1", "f1,f2,f3,f4,f5,f6"),
            ("fields2", "f51,f52,f53,f54,f55,f56"),
        ])).await?;
        let body: KlineResponse = response.json().await.map_err(|e| Self::provider_error(format!("Failed to parse kline response: {}", e)))?;
        if body.rc != 0 { return Err(Self::provider_error(format!("Kline API error {}: {}", body.rc, body.message))); }
        let rows = body.data.ok_or_else(|| MarketDataError::SymbolNotFound(symbol.to_string()))?.klines;
        let currency = Self::currency_for_symbol(symbol).to_string();
        let mut quotes = Vec::with_capacity(rows.len());
        for raw in rows {
            let row: Vec<&str> = raw.split(',').collect();
            if row.len() < 6 { continue; }
            let date = NaiveDate::parse_from_str(row[0], "%Y-%m-%d").map_err(|_| MarketDataError::ValidationFailed { message: format!("Invalid kline date '{}'", row[0]) })?;
            if date < start || date > end { continue; }
            let parse = |value: &str| -> Option<Decimal> { value.parse().ok() };
            let close = parse(row[2]).ok_or_else(|| MarketDataError::ValidationFailed { message: format!("Invalid kline close '{}'", row[2]) })?;
            let timestamp = date.and_hms_opt(12, 0, 0).map(|dt| Utc.from_utc_datetime(&dt)).unwrap_or_else(Utc::now);
            quotes.push(Quote { timestamp, open: parse(row[1]), high: parse(row[3]), low: parse(row[4]), close, volume: parse(row[5]), currency: currency.clone(), source: PROVIDER_ID.to_string() });
        }
        if quotes.is_empty() { return Err(MarketDataError::NoDataForRange); }
        quotes.sort_by_key(|quote| quote.timestamp);
        Ok(quotes)
    }
}

impl Default for TencentProvider { fn default() -> Self { Self::new() } }

#[async_trait]
impl MarketDataProvider for TencentProvider {
    fn id(&self) -> &'static str { PROVIDER_ID }
    fn priority(&self) -> u8 { 2 }
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities { instrument_kinds: &[InstrumentKind::Equity], coverage: Coverage { equity_mic_allow: Some(&["XSHG", "XSHE", "XHKG"]), equity_mic_deny: None, allow_unknown_mic: true, metal_quote_ccy_allow: None }, supports_latest: true, supports_historical: true, supports_search: true, supports_profile: false, supports_dividends: false }
    }
    fn rate_limit(&self) -> RateLimit { RateLimit { requests_per_minute: 120, max_concurrency: 3, min_delay: Duration::from_millis(100) } }

    async fn get_latest_quote(&self, _context: &QuoteContext, instrument: ProviderInstrument) -> Result<Quote, MarketDataError> {
        let symbol = Self::extract_symbol(&instrument)?;
        let response = self.checked_response(self.client.get(QUOTE_URL).query(&[("secid", symbol.as_str()), ("fields", "f43,f44,f45,f46,f47,f57,f58,f60,f86")])).await?;
        let body = response.json().await.map_err(|e| Self::provider_error(format!("Failed to parse quote response: {}", e)))?;
        Self::parse_quote(&symbol, body)
    }

    async fn get_historical_quotes(&self, _context: &QuoteContext, instrument: ProviderInstrument, start: DateTime<Utc>, end: DateTime<Utc>) -> Result<Vec<Quote>, MarketDataError> {
        let symbol = Self::extract_symbol(&instrument)?;
        self.fetch_kline(&symbol, start.date_naive(), end.date_naive()).await
    }

    async fn search(&self, query: &str) -> Result<Vec<SearchResult>, MarketDataError> {
        let response = self.checked_response(self.client.get(SEARCH_URL).query(&[("input", query), ("type", "14"), ("token", SEARCH_TOKEN), ("count", "20")])).await?;
        let body: SearchResponse = response.json().await.map_err(|e| Self::provider_error(format!("Failed to parse search response: {}", e)))?;
        let mut results = Vec::new();
        for item in body.table.data {
            let (mic, currency, exchange) = match item.market.as_str() { "1" => ("XSHG", "CNY", "SH"), "0" => ("XSHE", "CNY", "SZ"), "116" => ("XHKG", "HKD", "HK"), _ => continue };
            results.push(SearchResult::new(&item.code, &item.name, exchange, "EQUITY").with_exchange_mic(mic).with_currency(currency).with_data_source(PROVIDER_ID));
        }
        Ok(results)
    }
}

#[derive(Debug, Deserialize)] struct QuoteResponse { rc: i32, data: Option<QuoteData> }
#[derive(Debug, Deserialize)] struct QuoteData { #[serde(rename="f43")] close: Option<i64>, #[serde(rename="f44")] high: Option<i64>, #[serde(rename="f45")] low: Option<i64>, #[serde(rename="f46")] open: Option<i64>, #[serde(rename="f47")] volume: Option<i64>, #[serde(rename="f86")] timestamp: Option<i64> }
#[derive(Debug, Deserialize)] struct KlineResponse { rc: i32, #[serde(default, rename="message")] message: String, data: Option<KlineData> }
#[derive(Debug, Deserialize)] struct KlineData { #[serde(default)] klines: Vec<String> }
#[derive(Debug, Default, Deserialize)] struct SearchResponse { #[serde(default, rename="QuotationCodeTable")] table: SearchTable }
#[derive(Debug, Default, Deserialize)] struct SearchTable { #[serde(default, rename="Data")] data: Vec<SearchItem> }
#[derive(Debug, Deserialize)] struct SearchItem { #[serde(rename="Code")] code: String, #[serde(rename="Name")] name: String, #[serde(rename="MktNum")] market: String }

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use std::sync::Arc;

    #[test] fn provider_id_remains_compatible() { assert_eq!(TencentProvider::new().id(), "TENCENT"); }
    #[test] fn normalizes_legacy_and_current_symbols() {
        for (input, expected) in [
            ("sh601288", "1.601288"),
            ("sz000001", "0.000001"),
            ("hk00700", "116.00700"),
            ("601288.SS", "1.601288"),
            ("000001.SZ", "0.000001"),
            ("0700.HK", "116.00700"),
            ("601288", "1.601288"),
            ("700", "116.00700"),
            ("116.00700", "116.00700"),
        ] {
            assert_eq!(TencentProvider::normalize_symbol(input).unwrap(), expected);
        }
    }
    #[test] fn parses_a_share_quote() {
        let body = QuoteResponse { rc: 0, data: Some(QuoteData { close: Some(1050), high: Some(1055), low: Some(1040), open: Some(1045), volume: Some(123456), timestamp: Some(1_700_000_000) }) };
        let quote = TencentProvider::parse_quote("1.600000", body).unwrap();
        assert_eq!(quote.close, dec!(10.5)); assert_eq!(quote.volume, Some(dec!(123456))); assert_eq!(quote.currency, "CNY");
    }
    #[test] fn supports_cn_and_hk() {
        let provider = TencentProvider::new();
        for (ticker, mic) in [("600000", "XSHG"), ("000001", "XSHE"), ("00700", "XHKG")] {
            let instrument = crate::models::InstrumentId::Equity { ticker: Arc::from(ticker), mic: Some(mic.into()) };
            assert!(provider.capabilities().supports_instrument(&instrument));
        }
    }
}
