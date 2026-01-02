# 🎯 Real-Time Data Quality Monitor

A production-ready real-time data quality monitoring system built with Apache Kafka, Python, PostgreSQL, and Streamlit.

## 🏗️ Architecture
```
┌─────────────────┐
│  Data Producer  │ ── Generates orders with quality issues (10/sec)
└────────┬────────┘
         │ Kafka Topic: orders
         ▼
┌─────────────────┐
│      Kafka      │ ── Message streaming platform
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Quality Monitor │ ── Real-time quality checks
│                 │    • Completeness (99%)
│                 │    • Timeliness (94%)  
│                 │    • Accuracy (95%)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   PostgreSQL    │ ── Metrics storage
│                 │    • 10,000+ metrics
│                 │    • 1,000+ issues
│                 │    • 60s windowing
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    Dashboard    │ ── Real-time visualization
└─────────────────┘
```

## ✨ Features

### Data Quality Dimensions
- **Completeness** - Detects missing or null values in required fields
- **Timeliness** - Monitors data latency and delayed arrivals
- **Accuracy** - Validates data types, ranges, and formats
- **Real-time Processing** - Quality checks run on streaming data
- **Windowed Aggregation** - Statistics calculated every 60 seconds

### Quality Issues Detected
- ❌ Missing customer IDs
- ❌ Invalid quantities (negative, zero, out of range)
- ❌ Invalid prices (negative, zero)
- ❌ Delayed timestamps (> 5 minutes latency)
- ❌ Wrong data types
- ❌ Negative total amounts

### Dashboard Features
- 📊 Real-time quality score (overall: 96.35%)
- 📈 Historical trend charts (last hour)
- 🎯 Quality dimension gauges
- 🚨 Recent issues with severity levels
- 🔄 Auto-refresh every 5 seconds
- 📋 Issue breakdown by severity

## 🚀 Quick Start

### Prerequisites
- Docker Desktop installed
- 8GB RAM recommended
- Ports available: 8502, 5432, 9092, 2181

### Installation
```bash
# Clone the repository
git clone https://github.com/kalluripradeep/realtime-data-quality-monitor.git
cd realtime-data-quality-monitor

# Start all services
docker compose up -d

# Wait 30 seconds for services to initialize
# Access dashboard at http://localhost:8502
```

### Verify Services
```bash
# Check all services are running
docker compose ps

# View quality monitor logs
docker compose logs quality-monitor --tail 50

# Check database metrics
docker compose exec postgres psql -U admin -d data_quality -c "SELECT COUNT(*) FROM quality_metrics;"
```

## 📊 System Performance

### Real-Time Metrics (After 1 Hour)
- **Total Orders Processed:** 36,000+
- **Quality Metrics Collected:** 10,000+
- **Issues Detected:** 1,000+ (30% of orders)
- **Overall Quality Score:** 96.35%
- **Processing Latency:** < 100ms per order

### Quality Scores
- **Completeness:** 99.02%
- **Timeliness:** 94.29%
- **Accuracy:** 94.87%
- **Overall:** 96.35%

## 🛠️ Tech Stack

- **Kafka** - Apache Kafka 7.5.0 for message streaming
- **Python 3.11** - Core processing language
- **PostgreSQL 15** - Metrics storage
- **Streamlit 1.31** - Dashboard framework
- **Plotly** - Interactive charts
- **Docker Compose** - Container orchestration
- **Pandas** - Data manipulation
- **psycopg2** - PostgreSQL adapter

## 📁 Project Structure
```
realtime-data-quality-monitor/
├── producer/              # Kafka producer
│   ├── kafka_producer.py  # Producer logic
│   ├── data_generator.py  # Order generation with quality issues
│   ├── config.py         # Configuration
│   └── Dockerfile
├── flink/                # Quality monitor (Python-based)
│   ├── src/
│   │   ├── kafka_consumer.py    # Kafka consumer
│   │   ├── quality_checker.py   # Quality check logic
│   │   └── postgres_writer.py   # Database writer
│   ├── config.py
│   └── Dockerfile
├── dashboard/            # Streamlit dashboard
│   ├── app.py           # Dashboard application
│   ├── config.py
│   └── Dockerfile
├── postgres/
│   └── init.sql         # Database schema
└── docker-compose.yml   # Orchestration
```

## 🎯 Use Cases

- **Data Pipeline Monitoring** - Track quality of streaming data pipelines
- **SLA Monitoring** - Ensure data quality meets service level agreements
- **Anomaly Detection** - Identify data quality issues in real-time
- **Compliance** - Demonstrate data quality for regulatory requirements
- **Debugging** - Quickly identify sources of bad data

## 📈 Future Enhancements

- [ ] Add data profiling statistics
- [ ] Implement alerting (email, Slack, PagerDuty)
- [ ] Add more quality dimensions (consistency, uniqueness)
- [ ] Schema evolution detection
- [ ] ML-based anomaly detection
- [ ] Export quality reports (PDF, Excel)
- [ ] Multi-tenant support
- [ ] Historical comparison views

## 🤝 Contributing

Built by [Pradeep Kalluri](https://github.com/kalluripradeep)

## 📄 License

MIT License

---

**⭐ If you find this project useful, please star it on GitHub!**