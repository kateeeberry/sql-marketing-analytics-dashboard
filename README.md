# 📊 Global Email Marketing Performance Dashboard

## 📝 Project Overview
This project focuses on analyzing global email marketing data using **Google BigQuery** for data transformation and **Looker Studio** for interactive visualization. The goal was to identify top-performing markets, calculate key engagement metrics, and visualize daily activity trends.

## 🚀 Key Features
- **Dynamic Market Analysis:** A Top-10 countries ranking based on account volume and messaging activity.
- **Interactive Visualization:** Treemap for market share distribution and Time-series charts for trend analysis.
- **Advanced Filtering:** Users can drill down by specific countries and date ranges.

## 🛠 Tech Stack
- **Database:** Google BigQuery (SQL)
- **BI Tool:** Looker Studio
- **Data Transformation:** SQL CTEs, Window Functions, and Unpivot operations.

## 💡 Technical Deep Dive: Why UNPIVOT?
To make the dashboard scalable and efficient, I implemented the `UNPIVOT` operator. This transformed multiple metric columns into a **Long Format**, allowing:
1. **Single-Metric Filtering:** Easily switching between "Sent", "Open", and "Visit" metrics on a single graph.
2. **Simplified Model:** Reduced the need for multiple complex joins in the visualization layer.

## 🗄 SQL Logic Summary
The core SQL script handles:
- **DENSE_RANK():** To accurately rank countries even if they have identical metric values.
- **Window Functions:** Calculating total country-level metrics while maintaining daily granularity.
- **Data Normalization:** Preparing the final dataset for seamless BI integration.
