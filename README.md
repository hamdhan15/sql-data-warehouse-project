# SQL Data Warehouse Project

## 📌 Project Overview
This project demonstrates the design and implementation of a **Data Warehouse** using **Microsoft SQL Server**.  
It includes **ETL (Extract, Transform, Load) processes**, basic **data transformations**, and loading data into a structured **data warehouse** for analytics and reporting.

This project is created to showcase practical knowledge of **data warehousing concepts** and **SQL-based ETL workflows**.

---

## 🏗️ Architecture
- Source Data (Raw tables / CSV files)
- Staging Tables
- Data Transformation Layer
- Data Warehouse (Fact and Dimension Tables)

---

## 🧰 Technologies Used
- Microsoft SQL Server
- T-SQL
- SQL Server Management Studio (SSMS)
- GitHub

---

## 🔄 ETL Process
### 1. Extract
- Data is extracted from source tables or flat files
- Raw data is loaded into **staging tables**

### 2. Transform
- Handling NULL values
- Removing duplicates
- Data type conversions
- Standardizing values
- Applying basic business rules

### 3. Load
- Data is loaded into **dimension tables**
- Fact tables are populated using surrogate keys
- Relationships are maintained between fact and dimension tables

---

## 🗃️ Data Warehouse Design
- **Fact Tables**
  - Store business metrics and measures

- **Dimension Tables**
  - Store descriptive attributes like date, product, customer, etc.

- Schema used:
  - Star Schema / Snowflake Schema (based on requirement)

---

