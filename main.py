"""
World Bank Economic Indicators ETL Pipeline
Extracts economic data for multiple countries and indicators
"""

import requests
import pandas as pd
import mysql.connector
from mysql.connector import Error
import logging
from datetime import datetime
import time

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/worldbank_etl.log'),
        logging.StreamHandler()
    ]
)

# ==================== CONFIGURATION ====================
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Neha123',  # Change this
    'database': 'worldbank_etl'
}

# Countries to track (ISO codes)
COUNTRIES = {
    'IND': 'India',
    'USA': 'United States',
    'CHN': 'China',
    'GBR': 'United Kingdom',
    'JPN': 'Japan',
    'DEU': 'Germany',
    'BRA': 'Brazil'
}

# Economic Indicators
INDICATORS = {
    'NY.GDP.MKTP.CD': 'GDP (current US$)',
    'NY.GDP.PCAP.CD': 'GDP per capita (current US$)',
    'NY.GDP.MKTP.KD.ZG': 'GDP growth (annual %)',
    'SP.POP.TOTL': 'Population, total',
    'FP.CPI.TOTL.ZG': 'Inflation, consumer prices (annual %)',
    'SL.UEM.TOTL.ZS': 'Unemployment, total (% of labor force)',
    'NE.EXP.GNFS.ZS': 'Exports of goods and services (% of GDP)',
    'NE.IMP.GNFS.ZS': 'Imports of goods and services (% of GDP)'
}


# ==================== EXTRACT ====================
class WorldBankExtractor:
    def __init__(self):
        self.base_url = 'https://api.worldbank.org/v2'
        
    def extract_indicator_data(self, country_code, indicator_code, start_year=2010, end_year=2023):
        """Extract data for a specific country and indicator"""
        url = f"{self.base_url}/country/{country_code}/indicator/{indicator_code}"
        params = {
            'format': 'json',
            'date': f'{start_year}:{end_year}',
            'per_page': 500
        }
        
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            # World Bank API returns [metadata, data]
            if len(data) > 1 and data[1]:
                return data[1]
            return []
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching {country_code}/{indicator_code}: {e}")
            return []
    
    def extract_all_data(self):
        """Extract data for all countries and indicators"""
        all_records = []
        total = len(COUNTRIES) * len(INDICATORS)
        current = 0
        
        logging.info(f"Starting extraction for {len(COUNTRIES)} countries and {len(INDICATORS)} indicators")
        
        for country_code, country_name in COUNTRIES.items():
            for indicator_code, indicator_name in INDICATORS.items():
                current += 1
                logging.info(f"[{current}/{total}] Extracting {country_name} - {indicator_name}")
                
                data = self.extract_indicator_data(country_code, indicator_code)
                
                for record in data:
                    if record['value'] is not None:  # Skip null values
                        all_records.append({
                            'country_code': record['country']['id'],
                            'country_name': record['country']['value'],
                            'indicator_code': record['indicator']['id'],
                            'indicator_name': record['indicator']['value'],
                            'year': int(record['date']),
                            'value': float(record['value']),
                            'extracted_at': datetime.now()
                        })
                
                # Rate limiting - be nice to the API
                time.sleep(0.5)
        
        logging.info(f"Extracted {len(all_records)} total records")
        return pd.DataFrame(all_records)


# ==================== TRANSFORM ====================
class EconomicDataTransformer:
    def __init__(self, df):
        self.df = df
    
    def transform(self):
        """Clean and transform extracted data"""
        logging.info("Starting transformation")
        
        # Remove duplicates
        initial_count = len(self.df)
        self.df = self.df.drop_duplicates(
            subset=['country_code', 'indicator_code', 'year']
        )
        logging.info(f"Removed {initial_count - len(self.df)} duplicates")
        
        # Handle missing values
        self.df = self.df.dropna(subset=['value'])
        
        # Add derived fields
        self.df['decade'] = (self.df['year'] // 10) * 10
        
        # Categorize years
        self.df['period'] = pd.cut(
            self.df['year'],
            bins=[2000, 2010, 2015, 2020, 2025],
            labels=['2000s', '2010-2015', '2015-2020', '2020s'],
            include_lowest=True
        )
        
        # Add continent information
        continent_map = {
            'IND': 'Asia', 'CHN': 'Asia', 'JPN': 'Asia',
            'USA': 'North America', 'BRA': 'South America',
            'GBR': 'Europe', 'DEU': 'Europe'
        }
        self.df['continent'] = self.df['country_code'].map(continent_map)
        
        # Round values for better readability
        self.df['value'] = self.df['value'].round(2)
        
        # Data quality checks
        assert not self.df['country_code'].isnull().any(), "Null country codes found"
        assert not self.df['year'].isnull().any(), "Null years found"
        assert (self.df['year'] >= 2000).all(), "Invalid years found"
        
        logging.info(f"Transformation complete. Final records: {len(self.df)}")
        return self.df
    
    def create_aggregations(self):
        """Create summary statistics"""
        summaries = {}
        
        # Latest values by country and indicator
        latest = self.df.sort_values('year').groupby(
            ['country_code', 'indicator_code']
        ).last().reset_index()
        summaries['latest_values'] = latest
        
        # Year-over-year changes
        yoy = self.df.sort_values(['country_code', 'indicator_code', 'year'])
        yoy['yoy_change'] = yoy.groupby(['country_code', 'indicator_code'])['value'].pct_change() * 100
        summaries['yoy_changes'] = yoy[yoy['yoy_change'].notna()]
        
        # Average by decade
        decade_avg = self.df.groupby(
            ['country_code', 'indicator_code', 'decade']
        )['value'].mean().reset_index()
        summaries['decade_averages'] = decade_avg
        
        return summaries


# ==================== LOAD ====================
class MySQLLoader:
    def __init__(self, config):
        self.config = config
        self.connection = None
    
    def connect(self):
        """Create MySQL connection"""
        try:
            self.connection = mysql.connector.connect(**self.config)
            if self.connection.is_connected():
                logging.info("Successfully connected to MySQL")
                return True
        except Error as e:
            logging.error(f"Error connecting to MySQL: {e}")
            return False
    
    def create_tables(self):
        """Create database tables"""
        try:
            cursor = self.connection.cursor()
            
            # Main economic data table
            create_main_table = """
            CREATE TABLE IF NOT EXISTS economic_indicators (
                id INT AUTO_INCREMENT PRIMARY KEY,
                country_code VARCHAR(3) NOT NULL,
                country_name VARCHAR(100) NOT NULL,
                indicator_code VARCHAR(50) NOT NULL,
                indicator_name VARCHAR(255) NOT NULL,
                year INT NOT NULL,
                value DECIMAL(20, 2),
                decade INT,
                period VARCHAR(20),
                continent VARCHAR(50),
                extracted_at TIMESTAMP,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY unique_record (country_code, indicator_code, year),
                INDEX idx_country (country_code),
                INDEX idx_indicator (indicator_code),
                INDEX idx_year (year)
            )
            """
            
            # Latest values table
            create_latest_table = """
            CREATE TABLE IF NOT EXISTS latest_indicators (
                id INT AUTO_INCREMENT PRIMARY KEY,
                country_code VARCHAR(3) NOT NULL,
                indicator_code VARCHAR(50) NOT NULL,
                latest_year INT,
                latest_value DECIMAL(20, 2),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY unique_latest (country_code, indicator_code)
            )
            """
            
            # Year-over-year changes table
            create_yoy_table = """
            CREATE TABLE IF NOT EXISTS yoy_changes (
                id INT AUTO_INCREMENT PRIMARY KEY,
                country_code VARCHAR(3) NOT NULL,
                indicator_code VARCHAR(50) NOT NULL,
                year INT NOT NULL,
                value DECIMAL(20, 2),
                yoy_change DECIMAL(10, 2),
                UNIQUE KEY unique_yoy (country_code, indicator_code, year)
            )
            """
            
            # Country summary table
            create_summary_table = """
            CREATE TABLE IF NOT EXISTS country_summary (
                id INT AUTO_INCREMENT PRIMARY KEY,
                country_code VARCHAR(3) NOT NULL,
                country_name VARCHAR(100),
                total_indicators INT,
                latest_gdp DECIMAL(20, 2),
                latest_population BIGINT,
                avg_gdp_growth DECIMAL(10, 2),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY unique_country (country_code)
            )
            """
            
            cursor.execute(create_main_table)
            cursor.execute(create_latest_table)
            cursor.execute(create_yoy_table)
            cursor.execute(create_summary_table)
            self.connection.commit()
            logging.info("All tables created successfully")
            
        except Error as e:
            logging.error(f"Error creating tables: {e}")
            raise
    
    def load_main_data(self, df):
        """Load main economic data"""
        try:
            cursor = self.connection.cursor()
            
            insert_query = """
            INSERT INTO economic_indicators (
                country_code, country_name, indicator_code, indicator_name,
                year, value, decade, period, continent, extracted_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                value = VALUES(value),
                extracted_at = VALUES(extracted_at)
            """
            
            for _, row in df.iterrows():
                values = (
                    row['country_code'], row['country_name'],
                    row['indicator_code'], row['indicator_name'],
                    int(row['year']), float(row['value']),
                    int(row['decade']), str(row['period']),
                    row['continent'], row['extracted_at']
                )
                cursor.execute(insert_query, values)
            
            self.connection.commit()
            logging.info(f"Loaded {len(df)} records to economic_indicators table")
            
        except Error as e:
            logging.error(f"Error loading main data: {e}")
            self.connection.rollback()
            raise
    
    def load_aggregations(self, summaries):
        """Load aggregated data"""
        try:
            cursor = self.connection.cursor()
            
            # Load latest values
            latest_df = summaries['latest_values']
            for _, row in latest_df.iterrows():
                cursor.execute("""
                INSERT INTO latest_indicators (country_code, indicator_code, latest_year, latest_value)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    latest_year = VALUES(latest_year),
                    latest_value = VALUES(latest_value)
                """, (row['country_code'], row['indicator_code'], int(row['year']), float(row['value'])))
            
            # Load YoY changes
            yoy_df = summaries['yoy_changes']
            for _, row in yoy_df.iterrows():
                if pd.notna(row['yoy_change']):
                    cursor.execute("""
                    INSERT INTO yoy_changes (country_code, indicator_code, year, value, yoy_change)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        value = VALUES(value),
                        yoy_change = VALUES(yoy_change)
                    """, (row['country_code'], row['indicator_code'], int(row['year']), 
                          float(row['value']), float(row['yoy_change'])))
            
            self.connection.commit()
            logging.info("Loaded all aggregations successfully")
            
        except Error as e:
            logging.error(f"Error loading aggregations: {e}")
            self.connection.rollback()
            raise
    
    def create_country_summary(self):
        """Create country summary statistics"""
        try:
            cursor = self.connection.cursor()
            
            summary_query = """
            INSERT INTO country_summary (
                country_code, country_name, total_indicators,
                latest_gdp, latest_population, avg_gdp_growth
            )
            SELECT 
                e.country_code,
                e.country_name,
                COUNT(DISTINCT e.indicator_code) as total_indicators,
                MAX(CASE WHEN e.indicator_code = 'NY.GDP.MKTP.CD' THEN e.value END) as latest_gdp,
                MAX(CASE WHEN e.indicator_code = 'SP.POP.TOTL' THEN e.value END) as latest_population,
                AVG(CASE WHEN e.indicator_code = 'NY.GDP.MKTP.KD.ZG' THEN e.value END) as avg_gdp_growth
            FROM (
                SELECT country_code, country_name, indicator_code, value,
                       ROW_NUMBER() OVER (PARTITION BY country_code, indicator_code ORDER BY year DESC) as rn
                FROM economic_indicators
            ) e
            WHERE e.rn = 1
            GROUP BY e.country_code, e.country_name
            ON DUPLICATE KEY UPDATE
                total_indicators = VALUES(total_indicators),
                latest_gdp = VALUES(latest_gdp),
                latest_population = VALUES(latest_population),
                avg_gdp_growth = VALUES(avg_gdp_growth)
            """
            
            cursor.execute(summary_query)
            self.connection.commit()
            logging.info("Country summary created successfully")
            
        except Error as e:
            logging.error(f"Error creating summary: {e}")
            raise
    
    def close(self):
        """Close connection"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logging.info("MySQL connection closed")


# ==================== MAIN PIPELINE ====================
def run_etl_pipeline():
    """Execute the complete ETL pipeline"""
    start_time = datetime.now()
    logging.info("=" * 60)
    logging.info("Starting World Bank Economic Indicators ETL Pipeline")
    logging.info("=" * 60)
    
    try:
        # EXTRACT
        logging.info("STEP 1: EXTRACT")
        extractor = WorldBankExtractor()
        raw_data = extractor.extract_all_data()
        
        if raw_data.empty:
            logging.error("No data extracted. Exiting.")
            return False
        
        logging.info(f"Extracted {len(raw_data)} records")
        
        # TRANSFORM
        logging.info("STEP 2: TRANSFORM")
        transformer = EconomicDataTransformer(raw_data)
        transformed_data = transformer.transform()
        summaries = transformer.create_aggregations()
        logging.info(f"Transformed {len(transformed_data)} records")
        
        # LOAD
        logging.info("STEP 3: LOAD")
        loader = MySQLLoader(DB_CONFIG)
        
        if loader.connect():
            loader.create_tables()
            loader.load_main_data(transformed_data)
            loader.load_aggregations(summaries)
            loader.create_country_summary()
            loader.close()
        
        # Execution summary
        execution_time = (datetime.now() - start_time).total_seconds()
        logging.info("=" * 60)
        logging.info(f"Pipeline completed successfully in {execution_time:.2f} seconds")
        logging.info(f"Total records processed: {len(transformed_data)}")
        logging.info("=" * 60)
        
        return True
        
    except Exception as e:
        logging.error(f"Pipeline failed: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return False


if __name__ == "__main__":
    run_etl_pipeline()