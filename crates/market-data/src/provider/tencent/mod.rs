//! Tencent (QQ) market data provider for A-shares and Hong Kong stocks.
//!
//! Uses Tencent's public quote endpoints:
//! - Real-time quotes: `https://qt.gtimg.cn/q=sh600000,sz000001,hk00700`
//! - Historical klines: `https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?...`
//!
//! Symbol format (produced by the resolver):
//! - Shanghai A-share: `sh600000`
//! - Shenzhen A-share: `sz000001`
//! - Hong Kong stock: `hk00700` (5-digit, zero-padded)

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::errors::MarketDataError;
use crate::models::{
    Coverage, InstrumentKind, ProviderInstrument, Quote, QuoteContext, SearchResult,
};
use crate::provider::{MarketDataProvider, ProviderCapabilities, RateLimit};

const PROVIDER_ID: &str = "TENCENT";
const QUOTE_URL: &str = "https://qt.gtimg.cn/q=";
const KLINE_URL: &str = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get";
const SEARCH_URL: &str = "https://smartbox.gtimg.cn/s3/?v=2&q=";

/// Tencent market data provider for Chinese A-shares and Hong Kong stocks.
pub struct TencentProvider {
    client: reqwest::Client,
}

impl TencentProvider {
    pub fn new() -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self { client }
    }

    /// Extract the Tencent symbol (e.g. "sh600000") from the instrument.
    fn extract_symbol(instrument: &ProviderInstrument) -> Result<String, MarketDataError> {
        match instrument {
            ProviderInstrument::EquitySymbol { symbol } => Ok(symbol.to_string()),
            _ => Err(MarketDataError::UnsupportedAssetType(format!(
                "TENCENT only supports equities, got: {:?}",
                instrument
            ))),
        }
    }

    /// Currency for a Tencent symbol based on its market prefix.
    fn currency_for_symbol(symbol: &str) -> &'static str {
        if symbol.starts_with("hk") {
            "HKD"
        } else {
            "CNY"
        }
    }

    /// Parse the real-time quote response line.
    ///
    /// Format: `v_sh600000="1~浦发银行~600000~10.50~10.48~10.45~...~20260812153000~..."`
    /// Key fields (tilde-separated, after the quote):
    ///   [0] market flag, [1] name, [2] code, [3] price, [4] prev close,
    ///   [5] open, [6] volume (lots), [30] timestamp, [33] high, [34] low
    fn parse_quote_line(symbol: &str, line: &str) -> Result<Quote, MarketDataError> {
        let data = line
            .split_once("=\"")
            .map(|(_, rest)| rest.trim_end_matches('"').trim_end_matches(';'))
            .ok_or_else(|| MarketDataError::ProviderError {
                provider: PROVIDER_ID.to_string(),
                message: format!("Malformed quote line for {}", symbol),
            })?;

        let fields: Vec<&str> = data.split('~').collect();
        if fields.len() < 6 {
            return Err(MarketDataError::SymbolNotFound(symbol.to_string()));
        }

        // Empty price means the symbol does not exist or is suspended.
        let price_str = fields.get(3).copied().unwrap_or_default();
        if price_str.is_empty() {
            return Err(MarketDataError::SymbolNotFound(symbol.to_string()));
        }

        let close: Decimal = price_str
            .parse()
            .map_err(|_| MarketDataError::ValidationFailed {
                message: format!("Invalid price '{}' for {}", price_str, symbol),
            })?;

        let parse_opt = |idx: usize| -> Option<Decimal> {
            fields
                .get(idx)
                .filter(|s| !s.is_empty())
                .and_then(|s| s.parse().ok())
        };

        // Timestamp field (index 30): "20260812153000" in China Standard Time (UTC+8).
        let timestamp = fields
            .get(30)
            .filter(|s| s.len() >= 14)
            .and_then(|s| {
                chrono::NaiveDateTime::parse_from_str(&s[..14], "%Y%m%d%H%M%S")
                    .ok()
                    .map(|dt| {
                        let offset = chrono::FixedOffset::east_opt(8 * 3600).unwrap();
                        offset.from_local_datetime(&dt).unwrap().with_timezone(&Utc)
                    })
            })
            .unwrap_or_else(Utc::now);

        // Volume is reported in lots (手); convert to shares for A/HK (1 lot = 100 shares).
        let volume = parse_opt(6).map(|v| v * Decimal::from(100));

        Ok(Quote {
            timestamp,
            open: parse_opt(5),
            high: parse_opt(33),
            low: parse_opt(34),
            close,
            volume,
            currency: Self::currency_for_symbol(symbol).to_string(),
            source: PROVIDER_ID.to_string(),
        })
    }

    /// Fetch historical quotes via the kline endpoint.
    async fn fetch_kline(
        &self,
        symbol: &str,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<Vec<Quote>, MarketDataError> {
        let days = (end - start).num_days().max(1);
        // Request enough rows; Tencent caps at 640 per request.
        let count = (days + 1).clamp(1, 640);

        let url = format!(
            "{}?param={},day,{},{},{},qfq",
            KLINE_URL,
            symbol,
            start.format("%Y-%m-%d"),
            end.format("%Y-%m-%d"),
            count
        );

        // Tencent returns gzip-encoded bodies when the client advertises it;
        // reqwest is built without the gzip feature, so request plain identity.
        let response = self
            .client
            .get(&url)
            .header(reqwest::header::ACCEPT_ENCODING, "identity")
            .send()
            .await?;
        if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(MarketDataError::RateLimited {
                provider: PROVIDER_ID.to_string(),
            });
        }
        if !response.status().is_success() {
            return Err(MarketDataError::ProviderError {
                provider: PROVIDER_ID.to_string(),
                message: format!("HTTP {} from kline endpoint", response.status()),
            });
        }

        let body: KlineResponse = response.json().await.map_err(|e| {
            MarketDataError::ProviderError {
                provider: PROVIDER_ID.to_string(),
                message: format!("Failed to parse kline response: {}", e),
            }
        })?;

        if body.code != 0 {
            return Err(MarketDataError::ProviderError {
                provider: PROVIDER_ID.to_string(),
                message: format!("Kline API error code {}: {}", body.code, body.msg),
            });
        }

        let symbol_data = body
            .data
            .get(symbol)
            .ok_or_else(|| MarketDataError::SymbolNotFound(symbol.to_string()))?;

        let rows: &Vec<KlineRow> = symbol_data
            .qfqday
            .as_ref()
            .or(symbol_data.day.as_ref())
            .ok_or(MarketDataError::NoDataForRange)?;

        if rows.is_empty() {
            return Err(MarketDataError::NoDataForRange);
        }

        let currency = Self::currency_for_symbol(symbol).to_string();
        let mut quotes = Vec::with_capacity(rows.len());
        for row in rows {
            let date = NaiveDate::parse_from_str(row.date(), "%Y-%m-%d").map_err(|_| {
                MarketDataError::ValidationFailed {
                    message: format!("Invalid kline date '{}'", row.date()),
                }
            })?;
            if date < start || date > end {
                continue;
            }
            // Use midday UTC to avoid timezone boundary issues.
            let timestamp = date
                .and_hms_opt(12, 0, 0)
                .map(|dt| Utc.from_utc_datetime(&dt))
                .unwrap_or_else(Utc::now);

            let parse = |s: &str| -> Option<Decimal> { s.parse().ok() };
            let close = parse(row.close()).ok_or_else(|| MarketDataError::ValidationFailed {
                message: format!("Invalid kline close '{}'", row.close()),
            })?;

            quotes.push(Quote {
                timestamp,
                open: parse(row.open()),
                high: parse(row.high()),
                low: parse(row.low()),
                close,
                volume: parse(row.volume()),
                currency: currency.clone(),
                source: PROVIDER_ID.to_string(),
            });
        }

        if quotes.is_empty() {
            return Err(MarketDataError::NoDataForRange);
        }

        quotes.sort_by_key(|q| q.timestamp);
        Ok(quotes)
    }
}

impl Default for TencentProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MarketDataProvider for TencentProvider {
    fn id(&self) -> &'static str {
        PROVIDER_ID
    }

    fn priority(&self) -> u8 {
        // Higher priority than Yahoo for CN/HK markets.
        2
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            instrument_kinds: &[InstrumentKind::Equity],
            coverage: Coverage {
                equity_mic_allow: Some(&["XSHG", "XSHE", "XHKG"]),
                equity_mic_deny: None,
                allow_unknown_mic: false,
                metal_quote_ccy_allow: None,
            },
            supports_latest: true,
            supports_historical: true,
            supports_search: true,
            supports_profile: false,
            supports_dividends: false,
        }
    }

    fn rate_limit(&self) -> RateLimit {
        RateLimit {
            requests_per_minute: 300,
            max_concurrency: 5,
            min_delay: Duration::from_millis(50),
        }
    }

    async fn get_latest_quote(
        &self,
        _context: &QuoteContext,
        instrument: ProviderInstrument,
    ) -> Result<Quote, MarketDataError> {
        let symbol = Self::extract_symbol(&instrument)?;
        let url = format!("{}{}", QUOTE_URL, symbol);

        let response = self
            .client
            .get(&url)
            .header(reqwest::header::ACCEPT_ENCODING, "identity")
            .send()
            .await?;
        if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(MarketDataError::RateLimited {
                provider: PROVIDER_ID.to_string(),
            });
        }
        if !response.status().is_success() {
            return Err(MarketDataError::ProviderError {
                provider: PROVIDER_ID.to_string(),
                message: format!("HTTP {} from quote endpoint", response.status()),
            });
        }

        // Response is GBK-encoded; decode lossily since we only need ASCII digits.
        let bytes = response.bytes().await?;
        let text = String::from_utf8_lossy(&bytes);

        let line = text.lines().next().ok_or_else(|| MarketDataError::ProviderError {
            provider: PROVIDER_ID.to_string(),
            message: "Empty quote response".to_string(),
        })?;

        // Suspended/unknown symbols return `v_pv_none_match="1"`.
        if line.contains("pv_none_match") {
            return Err(MarketDataError::SymbolNotFound(symbol));
        }

        Self::parse_quote_line(&symbol, line)
    }

    async fn get_historical_quotes(
        &self,
        _context: &QuoteContext,
        instrument: ProviderInstrument,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<Quote>, MarketDataError> {
        let symbol = Self::extract_symbol(&instrument)?;
        self.fetch_kline(&symbol, start.date_naive(), end.date_naive())
            .await
    }

    async fn search(&self, query: &str) -> Result<Vec<SearchResult>, MarketDataError> {
        let url = format!("{}{}&t=all", SEARCH_URL, urlencoding::encode(query));
        let response = self
            .client
            .get(&url)
            .header(reqwest::header::ACCEPT_ENCODING, "identity")
            .send()
            .await?;
        if !response.status().is_success() {
            return Err(MarketDataError::ProviderError {
                provider: PROVIDER_ID.to_string(),
                message: format!("HTTP {} from search endpoint", response.status()),
            });
        }

        let bytes = response.bytes().await?;
        let text = String::from_utf8_lossy(&bytes);
        // Format: `v_hint="sh~600000~浦发银行~..."` or multiple results separated by `^`.
        let data = match text.split_once("=\"") {
            Some((_, rest)) => rest.trim_end_matches('"').trim_end_matches(';'),
            None => return Ok(vec![]),
        };

        let mut results = Vec::new();
        for entry in data.split('^') {
            let fields: Vec<&str> = entry.split('~').collect();
            if fields.len() < 3 {
                continue;
            }
            let market = fields[0];
            let code = fields[1];
            let name = fields[2];
            let (mic, currency) = match market {
                "sh" => ("XSHG", "CNY"),
                "sz" => ("XSHE", "CNY"),
                "hk" => ("XHKG", "HKD"),
                _ => continue,
            };
            results.push(
                SearchResult::new(code, name, market.to_uppercase(), "EQUITY")
                    .with_exchange_mic(mic)
                    .with_currency(currency)
                    .with_data_source(PROVIDER_ID),
            );
        }

        Ok(results)
    }
}

// ---------------------------------------------------------------------------
// Kline response models
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct KlineResponse {
    code: i32,
    #[serde(default)]
    msg: String,
    #[serde(default)]
    data: std::collections::HashMap<String, KlineSymbolData>,
}

#[derive(Debug, Deserialize)]
struct KlineSymbolData {
    /// 前复权日线
    qfqday: Option<Vec<KlineRow>>,
    /// 不复权日线
    day: Option<Vec<KlineRow>>,
}

/// Kline row: [date, open, close, high, low, volume].
/// Tencent returns rows as JSON arrays, so a tuple struct is used for deserialization.
#[derive(Debug, Deserialize)]
struct KlineRow(String, String, String, String, String, String);

impl KlineRow {
    fn date(&self) -> &str {
        &self.0
    }
    fn open(&self) -> &str {
        &self.1
    }
    fn close(&self) -> &str {
        &self.2
    }
    fn high(&self) -> &str {
        &self.3
    }
    fn low(&self) -> &str {
        &self.4
    }
    fn volume(&self) -> &str {
        &self.5
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use std::borrow::Cow;
    use std::sync::Arc;

    fn equity_instrument(symbol: &str) -> ProviderInstrument {
        ProviderInstrument::EquitySymbol {
            symbol: Arc::from(symbol),
        }
    }

    #[test]
    fn test_provider_id() {
        let provider = TencentProvider::new();
        assert_eq!(provider.id(), "TENCENT");
    }

    #[test]
    fn test_capabilities_coverage() {
        let provider = TencentProvider::new();
        let caps = provider.capabilities();

        let sh = crate::models::InstrumentId::Equity {
            ticker: Arc::from("600000"),
            mic: Some(Cow::Borrowed("XSHG")),
        };
        assert!(caps.supports_instrument(&sh));

        let hk = crate::models::InstrumentId::Equity {
            ticker: Arc::from("00700"),
            mic: Some(Cow::Borrowed("XHKG")),
        };
        assert!(caps.supports_instrument(&hk));

        let us = crate::models::InstrumentId::Equity {
            ticker: Arc::from("AAPL"),
            mic: Some(Cow::Borrowed("XNAS")),
        };
        assert!(!caps.supports_instrument(&us));
    }

    #[test]
    fn test_extract_symbol() {
        let inst = equity_instrument("sh600000");
        assert_eq!(TencentProvider::extract_symbol(&inst).unwrap(), "sh600000");
    }

    #[test]
    fn test_currency_for_symbol() {
        assert_eq!(TencentProvider::currency_for_symbol("sh600000"), "CNY");
        assert_eq!(TencentProvider::currency_for_symbol("sz000001"), "CNY");
        assert_eq!(TencentProvider::currency_for_symbol("hk00700"), "HKD");
    }

    #[test]
    fn test_parse_quote_line() {
        let line = "v_sh600000=\"1~浦发银行~600000~10.50~10.48~10.45~123456~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~20260812153000~0.02~0.19~10.55~10.40~\";";
        let quote = TencentProvider::parse_quote_line("sh600000", line).unwrap();
        assert_eq!(quote.close, dec!(10.50));
        assert_eq!(quote.open, Some(dec!(10.45)));
        assert_eq!(quote.high, Some(dec!(10.55)));
        assert_eq!(quote.low, Some(dec!(10.40)));
        assert_eq!(quote.currency, "CNY");
        assert_eq!(quote.source, "TENCENT");
    }

    #[test]
    fn test_parse_quote_line_empty_price() {
        let line = "v_sh999999=\"1~~~~~~\";";
        let result = TencentProvider::parse_quote_line("sh999999", line);
        assert!(matches!(result, Err(MarketDataError::SymbolNotFound(_))));
    }

    #[test]
    fn test_parse_quote_line_malformed() {
        let result = TencentProvider::parse_quote_line("sh600000", "garbage");
        assert!(matches!(result, Err(MarketDataError::ProviderError { .. })));
    }

    #[test]
    fn test_hk_quote_currency() {
        let line = "v_hk00700=\"1~腾讯控股~00700~380.00~375.00~376.00~100000~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~20260812160000~5.00~1.33~382.00~374.00~\";";
        let quote = TencentProvider::parse_quote_line("hk00700", line).unwrap();
        assert_eq!(quote.currency, "HKD");
        assert_eq!(quote.close, dec!(380.00));
    }

    #[test]
    fn test_extract_symbol_rejects_non_equity() {
        let inst = ProviderInstrument::FxPair {
            from: Cow::Borrowed("EUR"),
            to: Cow::Borrowed("USD"),
        };
        let result = TencentProvider::extract_symbol(&inst);
        assert!(matches!(
            result,
            Err(MarketDataError::UnsupportedAssetType(_))
        ));
    }
}
