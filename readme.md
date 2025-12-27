# CDC SCD Type 2 Pipeline - Databricks

A production-ready Change Data Capture (CDC) pipeline implementing Slowly Changing Dimension (SCD) Type 2 pattern using Databricks, Delta Lake, and Structured Streaming.

## 🎯 Features

- **Auto Loader Integration**: Automatically ingests JSON CDC events from cloud storage
- **SCD Type 2 Implementation**: Tracks complete historical changes with temporal validity
- **Hash-based Change Detection**: Uses SHA-256 hashing for efficient change identification
- **Idempotent Processing**: Checkpoint-based recovery prevents duplicate processing
- **Medallion Architecture**: Bronze (raw) → Silver (curated) data flow
- **Parameterized Configuration**: Easy deployment across environments

## 🏗️ Architecture

```
Source (JSON Files)
    ↓
Bronze Layer (Auto Loader + Structured Streaming)
    ↓
Silver Layer (SCD Type 2 Merge)
    ↓
Historical Dimension Table
```

## 📁 Project Structure

```
cdc-scd-pipeline/
├── README.md
├── requirements.txt
├── config.yaml
├── src/
│   ├── __init__.py
│   ├── bronze_ingestion.py
│   ├── silver_transformation.py
│   ├── utils.py
│   └── main.py
├── notebooks/
│   ├── 01_setup_tables.py
│   └── 02_run_pipeline.py
└── sample_data/
    └── sample_cdc_events.json
```

## 🚀 Quick Start

### Prerequisites

- Databricks Runtime 11.3 LTS or higher
- Delta Lake enabled cluster
- Access to cloud storage (DBFS/S3/ADLS)

### Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/cdc-scd-pipeline.git
cd cdc-scd-pipeline
```

2. Install dependencies (for local development):
```bash
pip install -r requirements.txt
```

3. Configure your environment:
```bash
cp config.yaml
# Edit config.yaml with your paths and settings
```

### Usage

#### Option 1: Using Databricks Notebooks

Upload the notebooks from the `notebooks/` folder to your Databricks workspace and run them in order.

#### Option 2: Using Python Scripts

```python
from src.main import run_cdc_pipeline

# Run with default config
run_cdc_pipeline(config_path="config.yaml")
```

## 📊 Data Schema

### Bronze Layer Schema
```
- customer_id: string
- name: string
- email: string
- address: string
- op_type: string (I/U/D)
- event_time: string
- FileName: string (added by Auto Loader)
```

### Silver Layer Schema (SCD Type 2)
```
- customer_id: string (PK with effective_from)
- name: string
- email: string
- address: string
- row_hash: string (SHA-256 hash)
- is_current: boolean
- effective_from: timestamp
- effective_to: timestamp (null for current records)
```

## 🔧 Configuration

Edit `config.yaml`:

```yaml
# Source paths
source_path: "dbfs:/FileStore/landing/cdc_json"
bronze_table_path: "dbfs:/FileStore/tables/cdc_bronze"
silver_table_path: "dbfs:/FileStore/tables/cdc_silver"

# Checkpoint locations
bronze_checkpoint: "dbfs:/FileStore/checkpoints/bronze"
silver_checkpoint: "dbfs:/FileStore/checkpoints/silver"

# Schema location for Auto Loader
schema_location: "dbfs:/FileStore/schemas/cdc"

# Processing settings
trigger_interval: "30 seconds"
hash_columns: ["name", "email", "address"]
```

## 🎓 Key Concepts

### SCD Type 2 Pattern

This implementation maintains complete history by:
1. **Closing old records**: Setting `is_current=false` and `effective_to=event_time`
2. **Inserting new versions**: Creating new records with `is_current=true`
3. **Tracking changes**: Using hash-based change detection

### Example Timeline

```
Customer ID: 123
┌─────────────────────────────────────────────────────────────────┐
│ Version 1: email=old@email.com                                  │
│ effective_from: 2024-01-01, effective_to: 2024-02-01           │
│ is_current: false                                               │
└─────────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────────┐
│ Version 2: email=new@email.com                                  │
│ effective_from: 2024-02-01, effective_to: null                 │
│ is_current: true                                                │
└─────────────────────────────────────────────────────────────────┘
```

## 📝 Sample Data

Generate test data:

```python
from src.utils import generate_sample_data

generate_sample_data(
    output_path="dbfs:/FileStore/landing/cdc_json",
    num_records=100
)
```


## 🔍 Monitoring

Query current snapshot:
```sql
SELECT * FROM delta.`dbfs:/FileStore/tables/cdc_silver`
WHERE is_current = true;
```

Track change frequency:
```sql
SELECT 
    customer_id,
    COUNT(*) - 1 as number_of_changes
FROM delta.`dbfs:/FileStore/tables/cdc_silver`
GROUP BY customer_id
HAVING COUNT(*) > 1;
```

## 🛠️ Troubleshooting

### Common Issues

1. **Checkpoint conflicts**: Delete checkpoint folders if schema changes
2. **Duplicate records**: Ensure unique customer_id in each micro-batch


### Optimizations

```sql
-- Optimize Silver table
OPTIMIZE delta.`dbfs:/FileStore/tables/cdc_silver`
ZORDER BY (customer_id);

-- Vacuum old files (7 days retention)
VACUUM delta.`dbfs:/FileStore/tables/cdc_silver` RETAIN 168 HOURS;
```

## 📚 Additional Resources

- [Delta Lake SCD Documentation](https://docs.delta.io/latest/delta-update.html)
- [Databricks Auto Loader](https://docs.databricks.com/ingestion/auto-loader/index.html)
- [Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.


## 👤 Author

**Barath Aravind**
- GitHub: [@BA-RATH](https://github.com/BA-RATH)
- LinkedIn: [itzzbarath](https://linkedin.com/in/itzzbarath)
- Email: bharatharavind3103@gmail.com

## 🙏 Acknowledgments

- Built using Databricks Platform
- Implements industry-standard SCD Type 2 pattern
- Follows Medallion Architecture best practices