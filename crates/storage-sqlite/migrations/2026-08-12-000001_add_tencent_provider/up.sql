-- Add Tencent market data provider for A-shares and Hong Kong stocks
INSERT OR IGNORE INTO market_data_providers (id, name, description, url, priority, enabled, logo_filename, last_synced_at, last_sync_status, last_sync_error)
VALUES
    ('TENCENT', 'Tencent (QQ) Finance', 'Tencent provides real-time and historical quotes for China A-shares (Shanghai/Shenzhen) and Hong Kong stocks. No API key required.', 'https://gu.qq.com/', 14, TRUE, 'tencent.png', NULL, NULL, NULL);
