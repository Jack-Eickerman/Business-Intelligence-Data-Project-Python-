# Business-Intelligence-Data-Project-Python
Analysis of Landfill Data in the United States. Dataset from Kaggle.
# US Landfills & LFG Energy Projects – MySQL ETL Pipeline

**Business Intelligence Data Project**  
@author: jackeickerman  
Created: March 2025

This Python script performs **ETL (Extract, Transform, Load)** on a dataset of municipal solid waste (MSW) landfills and landfill gas (LFG) energy projects across the United States. It cleans the data, imputes missing values with column means, standardizes string fields for database compatibility, and loads the cleaned records into a MySQL database table.

The goal is to prepare reliable, query-ready data for downstream business intelligence, reporting, or analysis (e.g., emission reduction trends, energy project performance, regional landfill insights).

## Data Source
- **Dataset**: "Landfills_in_America.csv"  
- **Original Source**: U.S. EPA Landfill Methane Outreach Program (LMOP) – [LMOP Landfill and Project Database](https://www.epa.gov/lmop/lmop-landfill-and-project-database)  
  (EPA provides updated Excel/CSV-like exports of landfill locations, LFG collection/flaring, energy project details, MW generation, and methane emission reductions. Data covers operational, candidate, and historical landfills.)
- **Key Fields** (after cleaning):  
  - Landfill Name  
  - City, County, State  
  - Landfill Owner Organization(s)  
  - LFG Energy Project Type  
  - Project Type Category  
  - LFG Collected (mmscfd), Flared (mmscfd), Flow to Project (mmscfd)  
  - Actual MW Generation, Rated MW Capacity  
  - Current Year Emission Reductions (MMTCO2e/yr – Direct & Avoided)  
  - Project Start/Finish Date, Current Project Status, Ownership Type, etc.

**Note**: The dataset is not exhaustive of every U.S. landfill; it focuses on those tracked by LMOP for methane outreach and energy utilization.

## Features of the Script
- Removes duplicates based on "Landfill Name"
- Drops irrelevant/project-specific columns (e.g., RNG Delivery Method, End User(s), Project Developer(s))
- Imputes missing/zero values in key numeric columns with column means (e.g., LFG Collected, MW Generation, Emission Reductions)
- Aggressively sanitizes string columns for MySQL safety:  
  - Replaces spaces with underscores  
  - Removes commas, periods, apostrophes, parentheses, hashtags, etc.  
  - Ensures non-numeric fields are quoted properly for INSERT statements
- Drops remaining rows with any NaN values
- Builds and executes a dynamic multi-row `INSERT INTO` query
- Connects to local MySQL (`root` user – change credentials in production!)
- Prints success message on completion
