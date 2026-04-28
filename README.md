# Snowflake Data Pipeline

A comprehensive Python and SQL-based data pipeline for Snowflake that demonstrates ETL (Extract, Transform, Load) operations using Snowflake streams, tasks, and stored procedures.

## 🎯 Project Overview

This project showcases a complete end-to-end data pipeline implementation in Snowflake, including:
- Database and schema setup
- Data loading and ingestion
- Data transformation
- Stream-based change data capture (CDC)
- Automated task scheduling
- Stored procedures for data processing

## 📁 Project Structure

```
├── 01_setup.sql          # Database, schema, warehouse, and table creation
├── 02_load_data.sql      # Initial data loading and population
├── 03_transform.sql      # Data transformation and enrichment logic
├── 04_streams_tasks.sql  # Snowflake streams and automated tasks setup
├── 05_procedure.sql      # Stored procedures for data processing
└── run_pipeline.py       # Python orchestrator script
```

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- Snowflake account with credentials
- `snowflake-connector-python` package installed

### Installation

1. Clone the repository:
```bash
git clone https://github.com/kakarlamanisha1/Snowflake.git
cd Snowflake
```

2. Install dependencies:
```bash
pip install snowflake-connector-python cryptography
```

### Configuration

Set the following environment variables before running the pipeline:

```bash
export SNOWFLAKE_USERNAME="your_username"
export SNOWFLAKE_PASSWORD="your_password"  # or use key pair auth
export SNOWFLAKE_ACCOUNT="your_account_id"
export SNOWFLAKE_WAREHOUSE="demo_wh"
export SNOWFLAKE_DATABASE="demo_db"
export SNOWFLAKE_SCHEMA="demo_schema"
```

For **Key Pair Authentication**, also set:
```bash
export PRIVATE_KEY_PATH="/path/to/your/private/key.pem"
```

### Running the Pipeline

Execute the complete pipeline with:
```bash
python run_pipeline.py
```

## 📊 Pipeline Stages

### 1. Setup (01_setup.sql)
- Creates `demo_db` database
- Sets up `demo_schema` schema
- Configures `demo_wh` warehouse (XSMALL, auto-suspend after 60 minutes)
- Creates `customers` and `orders` tables

### 2. Data Loading (02_load_data.sql)
- Loads sample customer and order data
- Prepares data for transformation

### 3. Transformation (03_transform.sql)
- Applies business logic transformations
- Enriches data with calculated fields

### 4. Streams & Tasks (04_streams_tasks.sql)
- Sets up Snowflake Streams for change data capture
- Creates Tasks for automated pipeline scheduling
- Enables continuous data processing

### 5. Procedures (05_procedure.sql)
- Implements stored procedures for data processing
- Handles complex business logic at the database level

## 🔐 Authentication Methods

The pipeline supports two authentication methods:

### Password Authentication
Uses `SNOWFLAKE_PASSWORD` environment variable.

### Key Pair Authentication
Uses private key file specified in `PRIVATE_KEY_PATH` environment variable. The script automatically detects and switches between authentication methods.

## 🛠️ Features

- **Smart SQL Parser**: Handles complex SQL with dollar-quoted blocks ($$)
- **Error Handling**: Comprehensive error catching and reporting
- **Flexible Authentication**: Supports both password and key-pair authentication
- **Auto-commit**: Automatic transaction commits for reliable execution
- **Logging**: Detailed execution logs for debugging

## 📝 SQL Features Used

- **Streams**: Track data changes in real-time
- **Tasks**: Automate repetitive operations on a schedule
- **Stored Procedures**: Implement complex business logic
- **Transformations**: Data enrichment and aggregation

## 🔧 Troubleshooting

### Connection Issues
- Verify Snowflake account identifier and credentials
- Check network connectivity to Snowflake endpoints
- Ensure warehouse is active

### SQL Errors
- Review the error message in the output
- Check individual SQL files for syntax issues
- Verify required tables and objects exist

### Authentication Failures
- Confirm environment variables are set correctly
- For key pair auth, verify key file exists and is readable
- Check private key format (PEM format expected)

## 📚 Resources

- [Snowflake Documentation](https://docs.snowflake.com)
- [Snowflake Python Connector](https://docs.snowflake.com/developer-guide/python-connector/python-connector)
- [Snowflake Streams & Tasks](https://docs.snowflake.com/user-guide/streams)

## 📄 License

This project is open source and available under the MIT License.

## 👤 Author

**Manisha Kakarla** - [kakarlamanisha1](https://github.com/kakarlamanisha1)

## 🤝 Contributing

Contributions are welcome! Feel free to submit issues or pull requests to improve the pipeline.