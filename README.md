# 🎯 Real-Time Data Quality Monitor

Real-time data quality monitoring system using Apache Kafka, Apache Flink, and Streamlit.

## 🏗️ Architecture
```
[Data Source] → [Kafka] → [Flink Processing] → [PostgreSQL] → [Streamlit Dashboard]
```

## ✨ Features

- 🔄 Real-time data quality monitoring
- 📊 Live dashboard with metrics
- 🚨 Automated anomaly detection
- 📈 Historical trend analysis
- 🐳 Easy Docker deployment

## 🛠️ Tech Stack

- **Apache Kafka** - Message streaming
- **Apache Flink** - Stream processing
- **PostgreSQL** - Metrics storage
- **Streamlit** - Interactive dashboard
- **Docker Compose** - Container orchestration

## 📋 Data Quality Metrics

1. **Completeness** - Missing values detection
2. **Timeliness** - Latency monitoring
3. **Accuracy** - Value range validation
4. **Consistency** - Duplicate detection
5. **Freshness** - Data age tracking

## 🚀 Quick Start
```bash
# Clone repository
git clone https://github.com/kalluripradeep/realtime-data-quality-monitor.git
cd realtime-data-quality-monitor

# Start all services
docker-compose up -d

# Access dashboard
http://localhost:8501
```

## 📁 Project Structure
```
realtime-data-quality-monitor/
├── kafka/              # Kafka configuration
├── producer/           # Data producer
├── flink/             # Flink jobs
├── dashboard/         # Streamlit app
├── docs/              # Documentation
└── docker-compose.yml # Docker setup
```

## 🎯 Use Cases

- Monitor data pipeline health
- Detect data quality issues in real-time
- Track SLA compliance
- Alert on anomalies
- Generate quality reports

## 📊 Dashboard Preview

_(Coming soon)_

## 🤝 Contributing

Built by [Pradeep Kalluri](https://github.com/kalluripradeep)

## 📄 License

MIT License

---

**Built with ❤️ for the Data Engineering community**
