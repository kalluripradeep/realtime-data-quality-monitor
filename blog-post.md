\# Building a Production-Grade ML System: 332K Orders, 3 Models, and 25 Days of Real-Time Data Quality Monitoring



\*How I built an ensemble ML system that processes streaming data with explainable AI and concept drift detection\*



---



\## The Problem



Data quality issues are expensive. A single bad record can cascade through your entire data pipeline, breaking dashboards, corrupting reports, and eroding trust in your data platform.



Traditional approaches to data quality monitoring follow a simple pattern:

1\. Define rules

2\. Check data against rules

3\. Alert when rules fail



This works... until it doesn't.



The challenge? \*\*You can't write rules for everything.\*\* 



Subtle patterns, unusual combinations, temporal anomalies—these slip through rule-based systems. By the time you notice something's wrong, bad data has already polluted your downstream systems.



\*\*What if your data quality system could learn what "normal" looks like and alert you when things deviate?\*\*



That's what I set out to build.



---



\## The Goal



Build a real-time data quality monitoring system that:

\- Processes streaming data (Apache Kafka)

\- Validates 6 quality dimensions simultaneously

\- Uses machine learning to detect anomalies

\- \*\*Explains\*\* why something is anomalous (not just that it is)

\- Detects concept drift and adapts over time

\- Runs in production 24/7 with minimal maintenance



And it needed to handle real production scale.



---



\## The Architecture



\### High-Level Design

```

Kafka Producer → Quality Monitor → PostgreSQL

&nbsp;                      ↓

&nbsp;                 ML Ensemble

&nbsp;                 (3 Models)

&nbsp;                      ↓

&nbsp;             ┌─────────┴─────────┐

&nbsp;             ↓                   ↓

&nbsp;        REST API            Dashboard

```



\### The Stack



\*\*Streaming:\*\* Apache Kafka (600K+ messages)  

\*\*Processing:\*\* Python 3.11  

\*\*ML:\*\* TensorFlow + Scikit-learn  

\*\*Storage:\*\* PostgreSQL  

\*\*API:\*\* FastAPI  

\*\*Visualization:\*\* Streamlit  

\*\*Deployment:\*\* Docker Compose  



---



\## Phase 1: The Baseline System



I started with a rule-based quality checker monitoring 6 dimensions:



1\. \*\*Completeness\*\* (25%) - No missing fields

2\. \*\*Timeliness\*\* (15%) - Data arrives on time

3\. \*\*Accuracy\*\* (20%) - Values in valid ranges

4\. \*\*Consistency\*\* (15%) - Formats are correct

5\. \*\*Uniqueness\*\* (10%) - No duplicates

6\. \*\*Validity\*\* (15%) - Business rules satisfied



Each dimension gets a score (0-100%), and records are classified by severity:

\- \*\*Critical\*\* - Multiple dimensions failing

\- \*\*High\*\* - Important dimension failing

\- \*\*Medium\*\* - Minor issues

\- \*\*Low\*\* - Edge cases



\*\*The baseline worked well for obvious issues. But it missed subtle patterns.\*\*



That's when I realized: We need machine learning.



---



\## Phase 2: Adding Machine Learning



\### The Challenge



ML-based anomaly detection sounds straightforward:

1\. Train a model on "normal" data

2\. Flag anything unusual



But production reality is messier:



\*\*Challenge 1: What's "normal"?\*\*  

Data quality patterns change. What's anomalous today might be normal tomorrow.



\*\*Challenge 2: Explainability\*\*  

"The ML model flagged this record" doesn't help anyone fix the problem.



\*\*Challenge 3: False positives\*\*  

Too many alerts = alert fatigue = ignored alerts = missed real issues.



\### The Solution: Isolation Forest



I started with \*\*Isolation Forest\*\*—an unsupervised algorithm perfect for anomaly detection:



\*\*Why Isolation Forest?\*\*

\- No labeled data needed

\- Fast training (<1 minute on 24 hours of data)

\- Sub-10ms inference

\- Handles multi-dimensional data naturally

\- Works well in production environments



\*\*How it works:\*\*  

The algorithm isolates anomalies by randomly selecting features and split values. Anomalies are easier to isolate (fewer splits needed) than normal points.



\*\*Training strategy:\*\*

```python

\# Every 2 hours, retrain on last 24 hours of data

historical\_data = get\_metrics(hours=24)

model.fit(historical\_data)

```



\*\*Results:\*\*

\- 93%+ detection accuracy

\- <10ms inference latency

\- Caught anomalies the rules missed



But we still had the explainability problem.



---



\## Phase 3: Making ML Explainable



\### The Problem



Manager: "Why did the model flag this record?"  

Me: "The anomaly score was -0.234..."  

Manager: "That doesn't help me fix it."



\*\*Explainability isn't optional in production.\*\*



\### The Solution: SHAP (SHapley Additive exPlanations)



SHAP provides feature importance for every prediction:

```python

from shap import TreeExplainer



explainer = TreeExplainer(model)

shap\_values = explainer.shap\_values(data\_point)



\# Get top contributing features

explanation = {

&nbsp;   'validity\_score': -0.045,  # Decreased anomaly likelihood

&nbsp;   'timeliness\_score': 0.032,  # Increased anomaly likelihood

&nbsp;   'accuracy\_score': 0.018

}

```



\*\*Human-readable output:\*\*

```

🤖 ML ANOMALY DETECTED! Score: -0.234

📊 Primary driver: validity\_score = 45.2 (30% below baseline)

&nbsp;  Contributing factors:

&nbsp;  - timeliness\_score = 78.3 (15% below baseline)

&nbsp;  - accuracy\_score = 82.1 (8% below baseline)

```



\*\*Now every alert is actionable.\*\*



The team could see exactly which quality dimension caused the alert and by how much it deviated from normal.



---



\## Phase 4: Detecting Concept Drift



\### The Problem



After 2 weeks in production, I noticed the model's accuracy degrading. Why?



\*\*The data had changed.\*\*



New products, seasonal patterns, business rule updates—all shifted what "normal" looked like. The model was still checking against outdated baselines.



This is called \*\*concept drift\*\*, and it's a silent killer of ML systems in production.



\### The Solution: Statistical Drift Detection



Implemented \*\*Kolmogorov-Smirnov (KS) tests\*\* on 24-hour windows:

```python

from scipy.stats import ks\_2samp



\# Compare current distribution to reference

reference\_data = get\_baseline(feature, hours=24)

current\_data = get\_recent(feature, hours=1)



statistic, p\_value = ks\_2samp(reference\_data, current\_data)



if p\_value < 0.01:

&nbsp;   trigger\_alert("CRITICAL: Drift detected")

&nbsp;   schedule\_model\_retrain()

elif p\_value < 0.05:

&nbsp;   trigger\_alert("WARNING: Potential drift")

```



\*\*Monitored across all dimensions:\*\*

\- Completeness score distribution

\- Timeliness score distribution

\- Accuracy score distribution

\- Consistency score distribution

\- Uniqueness score distribution

\- Validity score distribution



\*\*Result:\*\*  

The system now automatically detects when data patterns shift and triggers retraining. Model accuracy stays above 93%.



Built a 59K+ sample baseline for accurate drift detection.



---



\## Phase 5: The Ensemble Approach



\### The Insight



After running the Isolation Forest for a few weeks, I noticed something interesting:



\*\*Different types of anomalies need different detection approaches.\*\*



\- \*\*Isolation Forest\*\* - Great for statistical outliers

\- \*\*LSTM\*\* - Excels at temporal patterns

\- \*\*Autoencoder\*\* - Catches unusual feature combinations



\*\*Why pick one when you can use all three?\*\*



\### The Ensemble Architecture

```

┌─────────────────────────────────────────┐

│         Ensemble Detector               │

│  ┌──────────┐  ┌──────┐  ┌───────────┐│

│  │Isolation │  │ LSTM │  │Autoencoder││

│  │  Forest  │  │      │  │           ││

│  │  (40%)   │  │(30%) │  │   (30%)   ││

│  └──────────┘  └──────┘  └───────────┘│

│         ↓          ↓           ↓       │

│  ┌─────────────────────────────────┐  │

│  │   Weighted Voting System        │  │

│  │   Anomaly if: score > 0.5 OR    │  │

│  │              2+ models agree     │  │

│  └─────────────────────────────────┘  │

└─────────────────────────────────────────┘

```



\*\*Model 1: Isolation Forest\*\*

\- Trains on 98 aggregated samples (24 hours)

\- Detects statistical outliers

\- Fast and reliable baseline

\- Weight: 40% (proven performer)



\*\*Model 2: LSTM (Long Short-Term Memory)\*\*

\- Trains on 940 time series sequences

\- Predicts next quality scores based on history

\- Catches temporal anomalies (sudden changes, trends)

\- Weight: 30% (temporal specialist)



\*\*Model 3: Autoencoder\*\*

\- Trains on 500 recent samples

\- Learns to reconstruct "normal" patterns

\- High reconstruction error = anomaly

\- Weight: 30% (pattern specialist)



\*\*Voting Strategy:\*\*

```python

ensemble\_score = (

&nbsp;   isolation\_score \* 0.4 +

&nbsp;   lstm\_score \* 0.3 +

&nbsp;   autoencoder\_score \* 0.3

)



is\_anomaly = (

&nbsp;   ensemble\_score > 0.5 or

&nbsp;   sum(model\_predictions) >= 2

)

```



\*\*The magic:\*\* If any 2 models agree, we flag it as an anomaly. This dramatically reduces false positives.



\*\*Results:\*\*

\- False positive rate dropped from 15% to <10%

\- Caught anomalies single models missed

\- More robust to edge cases



---



\## The Results: 25 Days in Production



\### By The Numbers

```

Orders Processed:     332,308

Quality Checks:       2,799,627

System Uptime:        603.7 hours (25.2 days)

Average Latency:      <10ms per record



Quality Scores (24h avg):

├── Overall:          92.5%

├── Completeness:     98.9%

├── Timeliness:       92.0%

├── Accuracy:         96.5%

├── Consistency:      97.5%

├── Uniqueness:       100%

└── Validity:         69.5%



Issues Detected:      417,570

├── Critical:         41,732  (10%)

├── High:             272,247 (65.2%)

├── Medium:           103,582 (24.8%)

└── Low:              9       (<0.1%)



ML Performance:

├── Ensemble Accuracy: 93%+

├── False Positives:   <10%

├── Avg Inference:     <5ms

└── Drift Checks:      Every 2 hours

```



\### Real Anomalies Caught



\*\*Example 1: Temporal Pattern Break\*\*

\- LSTM detected unusual drop in completeness scores

\- Happened gradually over 6 hours

\- Isolation Forest missed it (within normal range at each point)

\- Root cause: Upstream API started returning partial data



\*\*Example 2: Multi-Dimensional Anomaly\*\*

\- Ensemble flagged combination of metrics

\- Individually, each metric was acceptable

\- Together, the pattern was anomalous

\- Root cause: New data source with different characteristics



\*\*Example 3: Concept Drift\*\*

\- Validity scores shifted 10% over 3 days

\- Drift detector triggered retraining

\- Post-retrain: Accuracy restored to 93%+

\- Root cause: Business rule change in upstream system



---



\## Key Learnings



\### 1. Ensemble > Single Model



\*\*Before Ensemble:\*\*

\- Isolation Forest alone: 15% false positive rate

\- Missed temporal anomalies

\- Struggled with complex patterns



\*\*After Ensemble:\*\*

\- 3 models together: <10% false positive rate

\- Caught 30% more real anomalies

\- Different models excel at different anomaly types



\*\*The lesson:\*\* Diversity in detection methods wins.



\### 2. Explainability Is Non-Negotiable



SHAP adds ~2ms latency but makes alerts actionable. \*\*Worth it every time.\*\*



Before SHAP:

\- "ML says this is bad"

\- Team ignores 40% of alerts

\- No way to debug



After SHAP:

\- "Validity score 30% below normal"

\- Team action rate increased to 85%

\- Can debug and fix root causes



\*\*Every production ML system should answer: "Why did the model decide this?"\*\*



\### 3. Drift Detection Prevents Silent Degradation



Without drift monitoring:

\- Model accuracy degraded from 93% to 78% over 2 weeks

\- We didn't notice until manual audit

\- Had to retrain and backfill missed anomalies



With drift detection:

\- System automatically detects distribution shifts

\- Triggers retraining before accuracy degrades

\- Maintains 93%+ accuracy continuously



\*\*Monitor your monitoring system.\*\*



\### 4. Start Simple, Iterate Fast



Evolution timeline:

\- \*\*Week 1:\*\* Rules-based system (baseline)

\- \*\*Week 2:\*\* Add Isolation Forest (first ML model)

\- \*\*Week 3:\*\* Add SHAP explainability (make it actionable)

\- \*\*Week 4:\*\* Add drift detection (prevent decay)

\- \*\*Week 5:\*\* Add LSTM + Autoencoder (ensemble power)



Each phase built on previous learnings. \*\*Perfect is the enemy of done.\*\*



\### 5. Production ML Needs More Than Just Models



Building production ML systems requires:



✅ \*\*Fast Inference\*\* - <10ms per prediction  

✅ \*\*Explainability\*\* - Teams need to understand decisions  

✅ \*\*Robustness\*\* - Ensembles outperform single models  

✅ \*\*Adaptability\*\* - Drift detection keeps models current  

✅ \*\*Monitoring\*\* - Track model performance over time  

✅ \*\*Operability\*\* - Easy deployment, clear alerts  



The model is maybe 30% of the work. The other 70% is making it production-ready.



---



\## The Architecture Deep Dive



\### Performance Optimizations



\*\*Challenge:\*\* Process 600K+ messages with <10ms latency



\*\*Solutions implemented:\*\*



1\. \*\*Batch Database Operations\*\*

```python

\# Instead of: cursor.execute() for each record

\# Do: cursor.executemany() with batches

cursor.executemany("INSERT...", batch)

\# 10-100x faster

```



2\. \*\*Connection Pooling\*\*

```python

\# Reuse database connections

db\_pool = create\_pool(min\_size=5, max\_size=20)

\# Eliminates connection overhead

```



3\. \*\*Async Processing\*\*

```python

\# Process quality checks while writing to DB

with ThreadPoolExecutor(max\_workers=4) as executor:

&nbsp;   executor.submit(write\_to\_db, metrics)

\# Overlap I/O operations

```



\*\*Result:\*\* Sustained 550+ orders/hour with <10ms latency per record



\### Reliability Features



\*\*Auto-recovery:\*\*

\- Kafka consumer auto-reconnects on failure

\- Database connection retries with exponential backoff

\- Model retraining continues even if one model fails



\*\*Alerting:\*\*

\- Critical alerts for quality drops >10%

\- Warning alerts for drift detection

\- System health checks every 60 seconds



\*\*Data persistence:\*\*

\- All metrics stored in PostgreSQL

\- 30-day retention for historical analysis

\- Automatic table partitioning by date



---



\## The Code



The entire system is \*\*open source\*\*:



🔗 \*\*GitHub:\*\* https://github.com/kalluripradeep/realtime-data-quality-monitor



\### Project Structure

```

realtime-data-quality-monitor/

├── flink/src/

│   ├── ensemble\_detector.py      # 3-model ensemble

│   ├── explainability.py         # SHAP integration

│   ├── drift\_detector.py         # KS-test implementation

│   ├── lstm\_detector.py          # Temporal anomaly detection

│   ├── autoencoder\_detector.py   # Reconstruction-based detection

│   ├── performance\_monitor.py    # Latency \& metrics tracking

│   └── kafka\_consumer.py         # Main processing loop

├── api/

│   └── main.py                   # FastAPI REST endpoints

├── dashboard/

│   └── app.py                    # Streamlit visualization

└── docker-compose.yml            # One-command deployment

```



\### Quick Start

```bash

\# Clone the repository

git clone https://github.com/kalluripradeep/realtime-data-quality-monitor.git

cd realtime-data-quality-monitor



\# Start all services

docker compose up -d



\# Access dashboard

open http://localhost:8502



\# Access API docs

open http://localhost:8000/docs

```



\*\*That's it.\*\* Everything runs in Docker.



---



\## What's Next?



\### Immediate Improvements

\- \[ ] Email/Slack integrations for alerts

\- \[ ] Advanced dashboard with drill-down capabilities

\- \[ ] Prometheus metrics export

\- \[ ] Model performance tracking UI

\- \[ ] A/B testing framework for model improvements



\### Future Exploration

\- \*\*Transformer-based anomaly detection\*\* - Attention mechanisms for complex patterns

\- \*\*Federated learning\*\* - Multi-tenant scenarios with privacy

\- \*\*Causal inference\*\* - Root cause analysis automation

\- \*\*Active learning\*\* - Reduce false positives with human feedback

\- \*\*Real-time feature importance\*\* - Dynamic SHAP explanations



---



\## Common Questions



\*\*Q: Why not just use rules-based validation?\*\*  

A: Rules catch known issues. ML catches unknown patterns and adapts to change. We use both.



\*\*Q: How do you handle false positives?\*\*  

A: Ensemble voting (2/3 models must agree) + SHAP explainability helps teams quickly identify false alarms.



\*\*Q: What about model retraining costs?\*\*  

A: Models retrain in <2 minutes every 2 hours. Minimal compute cost, huge accuracy benefit.



\*\*Q: Can this scale to billions of records?\*\*  

A: Current bottleneck is database writes. Solutions: Batch larger, use time-series DB (InfluxDB), or use Flink for stateful processing.



\*\*Q: How do you prevent alert fatigue?\*\*  

A: Severity-based routing, alert aggregation, and high threshold for CRITICAL alerts (only 10% of issues).



---



\## Conclusion



Building production ML systems is hard. Really hard.



But it's also incredibly rewarding when you see your system:

\- Processing 332K+ orders

\- Running 24/7 for 25+ days

\- Catching real anomalies teams would have missed

\- Explaining every decision clearly



\*\*The key insights:\*\*



1\. \*\*Start with rules, add ML where it helps\*\*

2\. \*\*Explainability builds trust\*\*

3\. \*\*Ensembles reduce false positives\*\*

4\. \*\*Drift detection prevents decay\*\*

5\. \*\*Iterate fast, deploy often\*\*



The entire system is open source. Fork it, adapt it, improve it.



If this helps even one team build better data quality systems, it was worth sharing.



---



\## Try It Yourself



🔗 \*\*GitHub:\*\* https://github.com/kalluripradeep/realtime-data-quality-monitor



📊 \*\*Live Demo:\*\* Dashboard + API available after `docker compose up`



💬 \*\*Questions?\*\* Open an issue on GitHub or reach out!



---



\## About the Author



\*\*Pradeep Kalluri\*\* is a Data Engineer specializing in real-time data platforms and ML systems. Currently contributing to Apache Airflow and building production data infrastructure at NatWest Bank.



\- \*\*LinkedIn:\*\* \[linkedin.com/in/pradeepkalluri](https://linkedin.com/in/pradeepkalluri)

\- \*\*Medium:\*\* \[@kalluripradeep99](https://medium.com/@kalluripradeep99)

\- \*\*Portfolio:\*\* \[kalluripradeep.github.io](https://kalluripradeep.github.io)

\- \*\*GitHub:\*\* \[github.com/kalluripradeep](https://github.com/kalluripradeep)



---



\*If you found this helpful, please share it! 🙌\*



\*\*Happy to discuss:\*\* ML in production, data quality strategies, real-time systems, or open source contributions.



---



\*\*Tags:\*\* #DataEngineering #MachineLearning #DataQuality #Python #ApacheKafka #MLOps #RealTime #Production #OpenSource

