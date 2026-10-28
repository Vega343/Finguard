FinGuard – Real-Time Credit Card Fraud Detection

An end-to-end real-time streaming data engineering project built using Databricks, Apache Kafka, PySpark, Lakeflow Declarative Pipelines, and Delta Lake.

FinGuard processes live credit card transactions, detects suspicious activity using customer data and fraud watchlists, sends email alerts, and provides a near-real-time monitoring dashboard.

Architecture
<img width="2000" height="1040" alt="image" src="https://github.com/user-attachments/assets/55f6f132-9815-408f-bed4-3dda77da4233" />

Key Features

Real-time transaction ingestion using Confluent Cloud Kafka.

Streaming JSON ingestion using Databricks Auto Loader.

Incremental customer data ingestion using Lakeflow Connect.

Bronze, Silver, and Gold medallion architecture.

Stream-static and stream-stream joins for fraud detection.

Watermarks and tumbling/sliding window aggregations.

Data quality validation using Lakeflow Declarative Pipelines.

Email alerts for suspicious transactions.

Near-real-time monitoring using Databricks AI/BI Dashboards.

Lakeflow Pipeline Graph:
<img width="1568" height="577" alt="image" src="https://github.com/user-attachments/assets/8754941a-0b3b-4833-bbc6-b4c6862bef17" />

The pipeline processes data through Bronze, Silver, and Gold layers, combining ingestion, data cleansing, fraud detection, and transaction analytics.

Dashboard

<img width="1512" height="806" alt="image" src="https://github.com/user-attachments/assets/5526f844-3f3e-4556-99c7-f45b9b2d4361" />


The dashboard provides visibility into high-value transactions, fraud-watchlist matches, and transaction-volume trends.

Technology Stack

Databricks | Apache Kafka | PySpark | Lakeflow Declarative Pipelines | Lakeflow Connect | Auto Loader | Delta Lake | Unity Catalog | PostgreSQL | Lakeflow Jobs

Fraud Detection Scenarios

<table>
  <thead>
    <tr>
      <th>Scenario</th>
      <th>Processing Technique</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>High-value transactions</td>
      <td>Stream-static join</td>
    </tr>
    <tr>
      <td>Fraud-watchlist matches</td>
      <td>Stream-stream join with a 5-minute watermark</td>
    </tr>
    <tr>
      <td>Transaction counts per minute</td>
      <td>1-minute tumbling window</td>
    </tr>
    <tr>
      <td>Rolling transaction volume</td>
      <td>5-minute sliding window with a 1-minute slide</td>
    </tr>
  </tbody>
</table>

Connect with Me

- LinkedIn: https://www.linkedin.com/in/venkat666/
- GitHub:https://github.com/Vega343)



