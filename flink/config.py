import os

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = 'orders'
KAFKA_GROUP_ID = 'flink-quality-checker'

# PostgreSQL Configuration
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'data_quality')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'admin')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'admin123')

# Quality Check Configuration
WINDOW_SIZE_SECONDS = 60  # Calculate metrics every 60 seconds
MAX_LATENCY_SECONDS = 300  # 5 minutes max acceptable latency

# Alert Configuration
ALERT_QUALITY_THRESHOLD = 90.0  # Alert if quality drops below this %
ALERT_ISSUE_RATE_THRESHOLD = 40.0  # Alert if issue rate exceeds this %
ALERT_CRITICAL_THRESHOLD = 100  # Alert if critical issues exceed this count

# Email Alert Configuration (disabled)
EMAIL_ALERTS_ENABLED = False  # Set to True to enable email alerts
EMAIL_TO = None
EMAIL_FROM = None
EMAIL_PASSWORD = None
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587