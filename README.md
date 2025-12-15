# 🌍 World Bank Economic Indicators ETL Pipeline

![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)
![MySQL](https://img.shields.io/badge/MySQL-8.0+-orange.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)
![Status](https://img.shields.io/badge/Status-Active-success.svg)

A production-ready ETL (Extract, Transform, Load) pipeline that fetches economic indicators from the World Bank API, processes the data, and stores it in MySQL for advanced analytics.

## 📊 Project Overview

This pipeline automates the collection and analysis of key economic indicators for major global economies. It demonstrates real-world data engineering practices including API integration, data transformation, database design, and error handling.

### **Key Features**
- 🔄 Automated data extraction from World Bank REST API
- 🧹 Robust data cleaning and transformation with pandas
- 💾 Normalized MySQL database schema with 4 tables
- 📈 Year-over-year analysis and trend calculations
- 📝 Comprehensive logging and error handling
- 🔍 15+ pre-built analytical SQL queries
- ⏰ Scheduler-ready for automated runs

## 🎯 Data Tracked

### Countries (7)
🇮🇳 India | 🇺🇸 USA | 🇨🇳 China | 🇬🇧 UK | 🇯🇵 Japan | 🇩🇪 Germany | 🇧🇷 Brazil

### Economic Indicators (8)
- **GDP (current US$)** - Total economic output
- **GDP per capita** - Economic output per person
- **GDP growth (annual %)** - Year-over-year growth rate
- **Population (total)** - Total population count
- **Inflation rate (%)** - Consumer price inflation
- **Unemployment rate (%)** - Labor force unemployment
- **Exports (% of GDP)** - Export intensity
- **Imports (% of GDP)** - Import dependency

### Data Range
📅 **2010 - 2023** (13+ years of historical data)

## 🏗️ Architecture

```
┌─────────────────┐
│  World Bank API │
│   (REST API)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    EXTRACT      │
│  Python/pandas  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   TRANSFORM     │
│  Data Cleaning  │
│  Aggregations   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│      LOAD       │
│   MySQL DB      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    ANALYZE      │
│  SQL Queries    │
└─────────────────┘
```

## 📁 Project Structure

```
worldbank-etl-pipeline/
│
├── main.py                      # Main ETL pipeline script
├── requirements.txt             # Python dependencies
├── setup.sql                    # Database setup queries
├── analysis_queries.sql         # Pre-built analytical queries
├── README.md                    # This file
│
├── logs/                        # Log files directory
│   └── worldbank_etl.log
│
└── screenshots/                 # Project screenshots (optional)
    ├── pipeline_run.png
    └── database_schema.png
```

## 🚀 Quick Start

### Prerequisites
- Python 3.8 or higher
- MySQL 8.0 or higher
- Internet connection (for API access)

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/yourusername/worldbank-etl-pipeline.git
cd worldbank-etl-pipeline
```

2. **Create virtual environment** (recommended)
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```

4. **Setup MySQL database**
```bash
mysql -u root -p
```
```sql
CREATE DATABASE worldbank_etl;
exit;
```

5. **Configure database connection**

Edit `main.py` (lines 23-28):
```python
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'YOUR_PASSWORD',  # Change this
    'database': 'worldbank_etl'
}
```

6. **Run the pipeline**
```bash
python main.py
```

### Expected Output
```
============================================================
Starting World Bank Economic Indicators ETL Pipeline
============================================================
STEP 1: EXTRACT
[1/56] Extracting India - GDP (current US$)
[2/56] Extracting India - GDP per capita (current US$)
...
Extracted 784 records
STEP 2: TRANSFORM
Removed 0 duplicates
Transformation complete. Final records: 784
STEP 3: LOAD
Successfully connected to MySQL
All tables created successfully
Loaded 784 records to economic_indicators table
============================================================
Pipeline completed successfully in 307.45 seconds
Total records processed: 784
============================================================
```

## 💾 Database Schema

### Tables

#### `economic_indicators` (Main table)
| Column | Type | Description |
|--------|------|-------------|
| id | INT | Primary key |
| country_code | VARCHAR(3) | ISO country code |
| country_name | VARCHAR(100) | Full country name |
| indicator_code | VARCHAR(50) | World Bank indicator ID |
| indicator_name | VARCHAR(255) | Indicator description |
| year | INT | Data year |
| value | DECIMAL(20,2) | Indicator value |
| decade | INT | Decade grouping |
| period | VARCHAR(20) | Time period category |
| continent | VARCHAR(50) | Geographic region |
| extracted_at | TIMESTAMP | Data extraction time |
| loaded_at | TIMESTAMP | Database insertion time |

#### `latest_indicators`
Stores the most recent value for each country-indicator combination.

#### `yoy_changes`
Contains year-over-year percentage changes for trend analysis.

#### `country_summary`
Aggregated statistics for each country including latest GDP, population, and average growth rates.

## 📊 Sample Queries

### Compare India vs China GDP
```sql
SELECT 
    year,
    MAX(CASE WHEN country_code = 'IND' THEN value END) as India_GDP,
    MAX(CASE WHEN country_code = 'CHN' THEN value END) as China_GDP
FROM economic_indicators
WHERE indicator_code = 'NY.GDP.MKTP.CD'
    AND country_code IN ('IND', 'CHN')
GROUP BY year
ORDER BY year DESC;
```

### Top 3 Fastest Growing Economies
```sql
SELECT 
    country_name,
    AVG(value) as avg_gdp_growth
FROM economic_indicators
WHERE indicator_code = 'NY.GDP.MKTP.KD.ZG'
    AND year >= YEAR(CURDATE()) - 10
GROUP BY country_name
ORDER BY avg_gdp_growth DESC
LIMIT 3;
```

### Trade Balance Analysis
```sql
SELECT 
    e.country_name,
    e.value as exports_pct_gdp,
    i.value as imports_pct_gdp,
    (e.value - i.value) as trade_balance
FROM economic_indicators e
JOIN economic_indicators i 
    ON e.country_code = i.country_code 
    AND e.year = i.year
WHERE e.indicator_code = 'NE.EXP.GNFS.ZS'
    AND i.indicator_code = 'NE.IMP.GNFS.ZS'
    AND e.year = (SELECT MAX(year) FROM economic_indicators)
ORDER BY trade_balance DESC;
```

More queries available in `analysis_queries.sql`

## 🔧 Configuration

### Add More Countries
Edit the `COUNTRIES` dictionary in `main.py`:
```python
COUNTRIES = {
    'IND': 'India',
    'USA': 'United States',
    'FRA': 'France',      # Add new country
    'CAN': 'Canada',      # Add new country
    'AUS': 'Australia'    # Add new country
}
```

### Add More Indicators
Edit the `INDICATORS` dictionary in `main.py`:
```python
INDICATORS = {
    'NY.GDP.MKTP.CD': 'GDP (current US$)',
    'EN.ATM.CO2E.PC': 'CO2 emissions (metric tons per capita)',  # New
    'SE.ADT.LITR.ZS': 'Literacy rate, adult total (% of people ages 15 and above)'  # New
}
```

Find more indicators at: [World Bank Indicators](https://data.worldbank.org/indicator)

### Change Date Range
Modify the `extract_indicator_data` method in `main.py`:
```python
def extract_indicator_data(self, country_code, indicator_code, start_year=2000, end_year=2024):
```

## ⏰ Scheduling (Optional)

### Using Cron (Linux/Mac)
```bash
# Edit crontab
crontab -e

# Run daily at 6 AM
0 6 * * * cd /path/to/worldbank-etl-pipeline && /path/to/venv/bin/python main.py
```

### Using Task Scheduler (Windows)
1. Open Task Scheduler
2. Create Basic Task
3. Set trigger: Daily at 6:00 AM
4. Action: Start a program
5. Program: `python.exe`
6. Arguments: `C:\path\to\main.py`

### Using Python Schedule
Add to `main.py`:
```python
import schedule
import time

schedule.every().day.at("06:00").do(run_etl_pipeline)

while True:
    schedule.run_pending()
    time.sleep(60)
```

## 🐛 Troubleshooting

<details>
<summary><b>Access denied for user</b></summary>

**Solution:** Verify MySQL credentials in `DB_CONFIG`. Ensure user has CREATE, INSERT, UPDATE privileges.
```sql
GRANT ALL PRIVILEGES ON worldbank_etl.* TO 'your_user'@'localhost';
FLUSH PRIVILEGES;
```
</details>

<details>
<summary><b>No data extracted / API timeout</b></summary>

**Possible causes:**
- No internet connection
- World Bank API is temporarily down
- Firewall blocking API requests

**Solution:** Check connection and retry. The API is free and doesn't require authentication.
</details>

<details>
<summary><b>Slow extraction (>10 minutes)</b></summary>

**Normal behavior:** First run takes 5-10 minutes to fetch 56 API endpoints (7 countries × 8 indicators).

**To speed up:**
- Reduce number of countries/indicators
- Increase sleep time: `time.sleep(1)` instead of `time.sleep(0.5)`
</details>

<details>
<summary><b>Table already exists error</b></summary>

**Solution:** The pipeline uses `CREATE TABLE IF NOT EXISTS` and `ON DUPLICATE KEY UPDATE`, so this shouldn't occur. If it does, check MySQL permissions.
</details>

## 📈 Future Enhancements

- [ ] Add interactive dashboard with Streamlit/Plotly
- [ ] Implement Apache Airflow for orchestration
- [ ] Add data quality monitoring and alerts
- [ ] Export reports to CSV/Excel automatically
- [ ] Deploy to cloud (AWS RDS, GCP Cloud SQL)
- [ ] Add unit tests with pytest
- [ ] Create Docker container for easy deployment
- [ ] Add CI/CD pipeline with GitHub Actions
- [ ] Implement data versioning

## 🛠️ Tech Stack

- **Language:** Python 3.8+
- **Database:** MySQL 8.0+
- **Libraries:** 
  - `pandas` - Data manipulation
  - `requests` - API calls
  - `mysql-connector-python` - Database connectivity
- **API:** World Bank Open Data API (REST)

## 📚 Learning Resources

- [World Bank API Documentation](https://datahelpdesk.worldbank.org/knowledgebase/articles/889392)
- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [MySQL Documentation](https://dev.mysql.com/doc/)
- [ETL Best Practices](https://www.integrate.io/blog/etl-best-practices/)

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the project
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 👤 Author

**Your Name**
- GitHub: [@yourusername](https://github.com/yourusername)
- LinkedIn: [Your LinkedIn](https://linkedin.com/in/yourprofile)
- Portfolio: [yourwebsite.com](https://yourwebsite.com)

## 🙏 Acknowledgments

- World Bank for providing free access to economic data
- Python and MySQL communities
- All contributors to this project

## ⭐ Star History

If you find this project useful, please consider giving it a star!

[![Star History Chart](https://api.star-history.com/svg?repos=yourusername/worldbank-etl-pipeline&type=Date)](https://star-history.com/#yourusername/worldbank-etl-pipeline&Date)

---

<div align="center">

**Made with ❤️ for the data engineering community**

[Report Bug](https://github.com/yourusername/worldbank-etl-pipeline/issues) · [Request Feature](https://github.com/yourusername/worldbank-etl-pipeline/issues)

</div>
