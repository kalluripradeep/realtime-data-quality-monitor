-- Create database schema for data quality metrics

-- Table to store quality metrics over time
CREATE TABLE IF NOT EXISTS quality_metrics (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
    metric_name VARCHAR(50) NOT NULL,
    metric_value DECIMAL(10, 2) NOT NULL,
    dimension VARCHAR(50) NOT NULL,  -- completeness, timeliness, accuracy, etc.
    details JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Table to store individual quality issues
CREATE TABLE IF NOT EXISTS quality_issues (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(100),
    issue_type VARCHAR(100) NOT NULL,
    issue_description TEXT,
    severity VARCHAR(20) NOT NULL,  -- low, medium, high, critical
    detected_at TIMESTAMP NOT NULL DEFAULT NOW(),
    resolved BOOLEAN DEFAULT FALSE,
    order_data JSONB
);

-- Table to store aggregated statistics
CREATE TABLE IF NOT EXISTS quality_stats (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    total_records INTEGER NOT NULL,
    clean_records INTEGER NOT NULL,
    issues_found INTEGER NOT NULL,
    completeness_score DECIMAL(5, 2),
    timeliness_score DECIMAL(5, 2),
    accuracy_score DECIMAL(5, 2),
    overall_score DECIMAL(5, 2),
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Indexes for better query performance
CREATE INDEX idx_metrics_timestamp ON quality_metrics(timestamp DESC);
CREATE INDEX idx_metrics_dimension ON quality_metrics(dimension);
CREATE INDEX idx_issues_detected_at ON quality_issues(detected_at DESC);
CREATE INDEX idx_issues_severity ON quality_issues(severity);
CREATE INDEX idx_stats_window ON quality_stats(window_start DESC);

-- Insert initial test data
INSERT INTO quality_stats (window_start, window_end, total_records, clean_records, issues_found, 
                          completeness_score, timeliness_score, accuracy_score, overall_score)
VALUES 
    (NOW() - INTERVAL '1 hour', NOW(), 100, 70, 30, 85.5, 92.3, 78.9, 85.6);

COMMENT ON TABLE quality_metrics IS 'Real-time quality metrics from Flink processing';
COMMENT ON TABLE quality_issues IS 'Individual data quality issues detected';
COMMENT ON TABLE quality_stats IS 'Aggregated quality statistics per time window';