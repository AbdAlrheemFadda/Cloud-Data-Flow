# Cloud-Based Distributed Data Processing Service using Databricks

A cloud-based distributed data processing service that enables users to upload datasets, perform descriptive analytics, and execute parallel machine learning tasks using Apache Spark (PySpark) on Databricks.

---

## Features

- Web-based interface for dataset upload and job monitoring
- Support for multiple file formats (CSV, JSON, Text, PDF)
- Descriptive statistics (counts, data types, missing values, basic metrics)
- Machine learning tasks:
  - Linear Regression
  - Decision Tree Regression
  - K-Means Clustering
  - Association Rule Mining (FP-Growth)
  - Time-Series Aggregation
- Parallel execution using Databricks clusters
- Performance benchmarking and scalability analysis

---

## Technologies Used

- Backend: Flask (Python)
- Data Processing: Apache Spark (PySpark)
- Cloud Platform: Databricks
- Frontend: HTML, CSS, JavaScript
- Storage: DBFS / Cloud Storage

---

## Requirements

- Python 3.8+
- Databricks Workspace
- Databricks Runtime 11.0+
- Node.js & npm (for frontend)

---

## Installation & Setup

```bash
git clone https://github.com/AbdAlrheemFadda/Cloud-Based-Data-Processing-Service-Design.git
cd Cloud-Based-Data-Processing-Service-Design
pip install -r requirements.txt
npm install --prefix frontend
```
