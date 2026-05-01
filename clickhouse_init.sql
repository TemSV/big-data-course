CREATE TABLE IF NOT EXISTS default.user_aggregated_features (
    user_id Int32,
    total_session_duration Int64, 
    avg_clicks Float64,
    total_sessions Int64
) ENGINE = MergeTree()
ORDER BY user_id;