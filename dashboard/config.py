import os

# PostgreSQL Configuration
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'data_quality')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'admin')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'admin123')

# Dashboard Configuration
REFRESH_INTERVAL = 5  # Seconds between auto-refresh
CHART_HISTORY_MINUTES = 60  # Show last 60 minutes of data
MAX_ISSUES_DISPLAY = 20  # Show top 20 recent issues
