# Data Warehouse and Analytics Project

 Hi, Hello, Welcome to the **Data Warehouse** repository! 🏄🏼‍♂️  
 This project shows advanced data warehousing and analytics solution, from building a data warehouse to generating actionable insights.  
#
## 🏭 Data Architecture

The data architecture that i chose for this project follows Medallion Architecture with 3 layers: **Bronze**, **Silver** and **Gold** layers:
<img width="1176" height="606" alt="image" src="https://github.com/user-attachments/assets/2da555cb-4c0a-47b2-b5dd-9fb053b2404b" />  
1. **Bronze Layer**: Stores raw data as-is from the source systems. Data is ingested from CSV files into SQL Server Database.
2. **Silver Layer**: This layer includes data cleaning, standardization and normalization processes to prepare data for analysis in final layer.
3. **Gold Layer**: This layer includes business-ready data modeled with star schema that meets the requirments for reporting and analytics.
#

