# RPA SUS - PA/RD Data Processing & Indexing System

## Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Main Scripts (2008+)](#main-scripts-2008)
  - [PA Script (send_elastic_dbf_pa_2008_plus.py)](#pa-script-send_elastic_dbf_pa_2008_pluspy)
  - [RD Script (send_elastic_dbf_rd_2008_plus.py)](#rd-script-send_elastic_dbf_rd_2008_pluspy)
- [Legacy Scripts](#legacy-scripts)
- [Configuration](#configuration)
- [Docker & Elasticsearch Setup](#docker--elasticsearch-setup)
- [Usage](#usage)
- [Performance Tuning](#performance-tuning)

---

## Overview

This project is a **Brazilian Healthcare Data Processing System** that extracts, transforms, and indexes SUS (Sistema Único de Saúde) data from DBF files into Elasticsearch for analysis and visualization.

### Data Sources
- **PA (Produção Ambulatorial)**: Outpatient production data
- **RD (Registro de Internação)**: Hospital admission/inpatient records

### Main Functionality
1. Reads DBF files from specified directories
2. Processes data in parallel batches
3. Cleans and transforms records
4. Indexes to Elasticsearch cluster (3-node setup)
5. Provides real-time monitoring via Kibana

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     DBF Files Directory                      │
│              (PA or RD files - .dbf format)                  │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│              Python Processing Scripts                       │
│  ┌────────────────────┐      ┌────────────────────┐        │
│  │  PA 2008+ Script   │      │  RD 2008+ Script   │        │
│  │  (Main Backoffice) │      │  (Main Backoffice) │        │
│  └────────────────────┘      └────────────────────┘        │
│                                                              │
│  • Parallel Processing (ProcessPoolExecutor)                │
│  • Data Cleaning & Transformation                           │
│  • Batch Processing (5000 records/chunk)                    │
│  • Retry Logic & Error Handling                             │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│            Elasticsearch Cluster (3 nodes)                   │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐             │
│  │  ES01    │    │  ES02    │    │  ES03    │             │
│  │ 10GB RAM │    │ 10GB RAM │    │ 10GB RAM │             │
│  │ 8GB Heap │    │ 8GB Heap │    │ 8GB Heap │             │
│  └──────────┘    └──────────┘    └──────────┘             │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                   Kibana (Port 5601)                         │
│              Visualization & Analysis Dashboard              │
└─────────────────────────────────────────────────────────────┘
```

---

## Main Scripts (2008+)

These are the **primary scripts used in the backoffice project** for processing modern data formats (2008 onwards).

### PA Script (send_elastic_dbf_pa_2008_plus.py)

**Location**: `rpa_sus/functions/dbf/pa/send_elastic_dbf_pa_2008_plus.py`

#### Purpose
Processes **Outpatient Production (Produção Ambulatorial)** data from 2008 onwards, indexing healthcare service records into Elasticsearch.

#### How It Works

##### 1. **Initialization & Connection**
```python
# Connects to Elasticsearch cluster with SSL/TLS
es_client = Elasticsearch(
    [ELASTICSEARCH_HOST],  # https://localhost:9200
    basic_auth=(ELASTIC_USERNAME, ELASTIC_PASSWORD),
    retry_on_timeout=True,
    verify_certs=False,
    max_retries=5,
).options(request_timeout=120)
```
- Establishes secure connection to ES cluster
- Sets 120-second timeout for large operations
- Configures automatic retry on failures

##### 2. **Data Reading & Batching**
```python
def read_in_batches(dbf_file_path, batch_size):
    """Reads DBF files in chunks to manage memory efficiently"""
    reader = DBF(dbf_file_path)
    data_batch = []
    for record in reader:
        data_batch.append(record)
        if len(data_batch) == batch_size:
            yield data_batch
            data_batch = []
```
- Opens DBF files using `dbfread` library
- Yields batches of 5000 records (configurable via `CHUNK_SIZE`)
- Prevents memory overflow on large files

##### 3. **Data Cleaning & Transformation**
```python
def prepare_es_doc(record, index_name, fields_to_include):
    # Step 1: Clean whitespace and null values
    cleaned_record = clean_column_data(record)

    # Step 2: Filter to only include relevant columns
    cleaned_record = {k: v for k, v in cleaned_record.items()
                      if k in fields_to_include}

    # Step 3: Date conversion (PA_CMP: YYYYMM → YYYY-MM-DDTHH:MM:SS)
    pa_cmp = cleaned_record.get("PA_CMP", "")
    if pa_cmp:
        date_value = handle_date_conversion(pa_cmp)
        cleaned_record["@DATA"] = date_value

    # Step 4: Generate unique document ID (MD5 hash)
    doc_id = generate_document_id(cleaned_record)

    # Step 5: Calculate total value
    cleaned_record["VAL_GERAL"] = cleaned_record['PA_VALAPR']

    # Step 6: Add metadata
    cleaned_record["CNPJ_CPF"] = cleaned_record.get("PA_CNPJCPF", "")
    cleaned_record["SOURCE"] = 'SIA'  # Sistema de Informação Ambulatorial

    return {
        '_index': index_name,
        '_id': doc_id,
        '_source': cleaned_record
    }
```

**Key Transformations:**
- **PA_CMP** (Competency Date): Converted from `202401` → `2024-01-15T00:00:00`
- **VAL_GERAL**: Created from PA_VALAPR (approved value)
- **SOURCE**: Tagged as 'SIA' (Ambulatorial Information System)
- **Document ID**: MD5 hash of unique fields + random component

##### 4. **Columns Processed (PA 2008+)**
```python
COLUNS_TO_WATCH_PA_2008_PLUS = [
    "PA_CODUNI",    # Unit code (healthcare facility)
    "PA_GESTAO",    # Management entity
    "PA_UFMUN",     # Municipality code (UF+Municipality)
    "PA_CNPJCPF",   # CNPJ/CPF of provider
    "PA_CNPJMNT",   # CNPJ of maintainer
    "PA_MVM",       # Movement month
    "PA_CMP",       # Competency date (YYYYMM)
    "PA_PROC_ID",   # Procedure ID
    "PA_TPFIN",     # Financing type
    "PA_SUBFIN",    # Sub-financing type
    "PA_CBOCOD",    # CBO code (occupation)
    "PA_CIDPRI",    # Primary ICD code
    "PA_QTDPRO",    # Quantity presented
    "PA_QTDAPR",    # Quantity approved
    "PA_VALPRO",    # Value presented
    "PA_VALAPR",    # Value approved
]
```

##### 5. **Parallel Processing Flow**
```python
def parallel_bulk_index(dbf_directory):
    # Get all DBF files
    dbf_files = sorted([f for f in os.listdir(dbf_directory)
                        if f.endswith('.dbf')])

    # Process in batches of 12 files
    batch_size = 12

    # Calculate optimal workers (uses 8 CPU cores)
    worker_count = min(cpu_count, 8)

    for batch in file_batches:
        # Pre-create indices
        for dbf_file in batch:
            index_name = f"{ES_INDEX_NAME_PREFIX}{dbf_file}".lower()
            ensure_index_exists(index_name)

        # Process each file with parallel workers
        with ProcessPoolExecutor(max_workers=worker_count) as executor:
            for chunk in read_in_batches(dbf_file_path, 5000):
                future = executor.submit(process_chunk_with_retry,
                                        chunk, index_name)
                futures.append(future)

        # Wait for all chunks to complete
        for future in as_completed(futures):
            total_processed += future.result()
```

**Processing Strategy:**
- **Batch File Processing**: 12 files at a time (optimized for 64GB RAM)
- **Parallel Workers**: 8 workers (matching CPU cores)
- **Chunk Size**: 5000 records per chunk
- **Memory Management**: Garbage collection after each batch

##### 6. **Error Handling & Retry Logic**
```python
@retry(stop=stop_after_attempt(5),
       wait=wait_exponential(multiplier=1, min=4, max=10))
def process_chunk_with_retry(data_chunk, index_name, chunk_index, dbf_file):
    return process_chunk(data_chunk, index_name, chunk_index, dbf_file)
```

**Features:**
- **Document Validation**: Checks required fields, size limits, data types
- **Automatic Retries**: Up to 5 attempts with exponential backoff
- **Circuit Breaker Handling**: Detects Elasticsearch overload (429 errors)
- **Error Analysis**: Categorizes and logs error types
- **Failed Document Logging**: Saves samples to JSON for debugging

##### 7. **Index Configuration**
```python
def ensure_index_exists(index_name):
    es_client.indices.create(
        index=index_name,
        body={
            "settings": {
                "number_of_shards": 2,
                "number_of_replicas": 0,
                "refresh_interval": "60s",
                "index.max_result_window": 100000,
                "index.mapping.total_fields.limit": 2000,
            }
        },
        ignore=[400, 404]
    )
```

**Index Settings:**
- **Shards**: 2 (balanced for 3-node cluster)
- **Replicas**: 0 (no replication for faster indexing)
- **Refresh Interval**: 60s (reduces index overhead)
- **Max Result Window**: 100,000 documents
- **Field Limit**: 2,000 fields per index

##### 8. **Performance Monitoring**
The script logs:
- Total files processed
- Records indexed per file
- Processing time per chunk
- Average indexing rate (records/second)
- Memory usage after garbage collection

---

### RD Script (send_elastic_dbf_rd_2008_plus.py)

**Location**: `rpa_sus/functions/dbf/rd/send_elastic_dbf_rd_2008_plus.py`

#### Purpose
Processes **Hospital Admission Records (Registro de Internação)** from 2008 onwards, indexing inpatient hospitalization data.

#### How It Works

The RD script follows the **same architecture as the PA script** with key differences in data structure:

##### 1. **Columns Processed (RD 2008+)**
```python
COLUNS_TO_WATCH_2008_plus = [
    "UF_ZI",        # State code
    "ANO_CMPT",     # Year of competency
    "MES_CMPT",     # Month of competency
    "ESPEC",        # Medical specialty
    "CGC_HOSP",     # Hospital CNPJ
    "N_AIH",        # AIH number (hospitalization authorization)
    "IDENT",        # Patient identifier
    "UTI_MES_TO",   # Total ICU months
    "QT_DIARIAS",   # Daily quantities (2008+)
    "PROC_REA",     # Procedure performed
    "VAL_SH",       # Hospital services value
    "VAL_SP",       # Professional services value
    "VAL_SADT",     # Diagnostic/therapeutic support value
    "VAL_TOT",      # Total value
    "VAL_UTI",      # ICU value
    "DT_INTER",     # Admission date
    "DT_SAIDA",     # Discharge date
    "DIAG_PRINC",   # Primary diagnosis (ICD)
    "COBRANCA",     # Billing type
    "NATUREZA",     # Nature of service
    "GESTAO",       # Management entity
    "MUNIC_MOV",    # Municipality of movement
    "DIAS_PERM",    # Length of stay (days)
    "CNES",         # National health facility code
    "CNPJ_MANT",    # Maintainer CNPJ (2008+)
    "COMPLEX",      # Complexity level (2008+)
    "FINANC",       # Financing type (2008+)
    "FAEC_TP"       # FAEC type (2008+)
]
```

##### 2. **Date Handling (RD Specific)**
```python
def handle_data_conversion_rd_97_03(ANO, MES):
    """Converts separate year/month fields to datetime"""
    ano_cmpt = int(ANO)
    mes_cmpt = int(MES)

    # Create datetime: YYYY-MM-15T00:00:00
    dt = datetime(ano_cmpt, mes_cmpt, 15)
    return dt.strftime("%Y-%m-%dT%H:%M:%S")
```

**Date Conversions:**
- **ANO_CMPT + MES_CMPT**: `2024` + `01` → `2024-01-15T00:00:00`
- **DT_INTER** (Admission): Native date field
- **DT_SAIDA** (Discharge): Native date field

##### 3. **Unique Document ID Generation**
```python
def generate_document_id(record):
    """Generate unique ID for RD records"""
    unique_fields = [
        record.get("UF_ZI", ""),
        record.get("ANO_CMPT", ""),
        record.get("MES_CMPT", ""),
        record.get("CGC_HOSP", ""),
        record.get("N_AIH", "")  # AIH number is critical
    ]

    base_id = "_".join(str(field) for field in unique_fields if field)
    random_component = str(random.randint(1000, 99999999))
    combined_id = f"{base_id}_{random_component}"

    return hashlib.md5(combined_id.encode()).hexdigest()
```

**Key Identifier:** `N_AIH` (Autorização de Internação Hospitalar number)

##### 4. **Data Transformations (RD Specific)**
```python
def prepare_es_doc(record, index_name, fields_to_include):
    cleaned_record = clean_column_data(record)

    # Filter columns
    if fields_to_include:
        cleaned_record = {k: v for k, v in cleaned_record.items()
                          if k in fields_to_include}

    # Combine year and month for timestamp
    ano = cleaned_record.get("ANO_CMPT", "")
    mes = cleaned_record.get("MES_CMPT", "")
    if ano and mes:
        date_value = handle_data_conversion_rd_97_03(ano, mes)
        cleaned_record["@DATA"] = date_value

    # Generate unique ID
    doc_id = generate_document_id(cleaned_record)

    # Add metadata
    cleaned_record["VAL_GERAL"] = cleaned_record.get('VAL_TOT', '')
    cleaned_record["CNPJ_CPF"] = cleaned_record.get("CGC_HOSP", "")
    cleaned_record["SOURCE"] = 'SIH'  # Sistema de Informação Hospitalar

    return {
        '_index': index_name,
        '_id': doc_id,
        '_source': cleaned_record
    }
```

**Key Differences from PA:**
- **SOURCE**: Tagged as 'SIH' (Hospital Information System) instead of 'SIA'
- **VAL_GERAL**: Uses VAL_TOT (total hospitalization value)
- **CNPJ_CPF**: Uses CGC_HOSP (hospital CNPJ)
- **Date Construction**: Combines ANO_CMPT + MES_CMPT

##### 5. **Processing Configuration**
Both PA and RD scripts share the same processing parameters:
- **Worker Count**: 8 parallel processes
- **Batch Size**: 12 files per batch
- **Chunk Size**: 5000 records
- **Retry Attempts**: 5 with exponential backoff
- **Request Timeout**: 120 seconds

---

## Legacy Scripts

These scripts handle older data formats (pre-2008). They have **similar architecture** but process different column sets and date formats.

### PA Legacy Scripts

#### 1. **send_elastic_dbf_94-99_pa.py**
- **Period**: 1994-1999
- **Columns**: Basic set (10 fields)
- **Date Format**: `PA_DATPR`, `PA_DATREF` (individual date fields)
- **Key Difference**: No PA_CMP field, uses separate date references

#### 2. **send_elastic_dbf_99-03_pa.py**
- **Period**: 1999-2003
- **Columns**: Same as 94-99
- **Date Format**: Transitional format
- **Key Difference**: Bridges old and new formats

#### 3. **send_elastic_dbf_03-08_pa.py**
- **Period**: 2003-2008
- **Columns**: Same as 94-99
- **Date Format**: Moving towards YYYYMM format
- **Key Difference**: Pre-standardization period

### RD Legacy Scripts

#### 1. **send_elastic_dbf_rd_92-97.py**
- **Period**: 1992-1997
- **Columns**: 19 fields (no CNES, CNPJ_MANT, COMPLEX, FINANC, FAEC_TP)
- **Date Fields**: ANO_CMPT, MES_CMPT, DT_INTER, DT_SAIDA
- **Key Difference**: `UTI_TOTAL` instead of `UTI_MES_TO`

#### 2. **send_elastic_dbf_rd_98-03.py**
- **Period**: 1998-2003
- **Columns**: Transitional set
- **Date Format**: ANO_CMPT + MES_CMPT
- **Key Difference**: Pre-CNES era

#### 3. **send_elastic_dbf_rd_03-07.py**
- **Period**: 2003-2007
- **Columns**: Moving towards modern format
- **Date Format**: ANO_CMPT + MES_CMPT
- **Key Difference**: CNES introduced

### CSV Processing Scripts

#### sendElasticUnique.py
- **Purpose**: Process single DBF files with custom configuration
- **Use Case**: Testing, small datasets, manual processing
- **Features**: Same transformation logic as batch scripts

#### sendElasticMultipleFilesCSV.py
- **Purpose**: Process CSV files instead of DBF
- **Data Format**: Semicolon-delimited (`;`)
- **Columns**: Flexible (reads from CSV headers)
- **Use Case**: When data is already converted from DBF to CSV

**Key Differences from DBF Scripts:**
```python
df = pd.read_csv(filepath, sep=';',
                 dtype={'column1': str, 'column14': str},
                 low_memory=False)
```
- Uses pandas for CSV reading
- Handles NaN values differently
- No DBF-specific encoding issues

---

## Configuration

### Environment Variables (.env)
```bash
# Elasticsearch Connection
ELASTIC_USERNAME=elastic
DATABASE_ELASTIC_PASSWORD=nicolasprojetoflavio22

# Index Naming
ES_INDEX_NAME_PREFIX=sus_data_

# Optional: Override defaults
# CHUNK_SIZE=5000
# NUM_PROCESSES=8
# MAX_RETRIES=5
```

### Database Configuration (rpa_sus/configs/database.py)
```python
ELASTICSEARCH_HOST = "https://localhost:9200"
ES_INDEX_NAME_PREFIX = os.getenv('ES_INDEX_NAME_PREFIX')
CHUNK_SIZE = 5000          # Records per chunk
ELASTIC_USERNAME = os.getenv('ELASTIC_USERNAME')
ELASTIC_PASSWORD = os.getenv('DATABASE_ELASTIC_PASSWORD')
MAX_RETRIES = 5            # Retry attempts
RETRY_DELAY = 3            # Seconds between retries
NUM_PROCESSES = 8          # Parallel workers (CPU cores)
```

### Constants Configuration (rpa_sus/configs/constants.py)
Contains mappings for:
- **State Codes**: Brazilian states (UF codes)
- **Financial Codes**: Financing types
- **FAEC Codes**: Strategic Actions Fund types
- **CARATEND Codes**: Service nature types

Example:
```python
state_codes = {
    "Rondônia": 11,
    "Acre": 12,
    "Amazonas": 13,
    # ... all Brazilian states
    "Distrito Federal": 53,
}
```

---

## Docker & Elasticsearch Setup

### Cluster Architecture
- **3 Elasticsearch Nodes**: es01, es02, es03
- **1 Kibana Instance**: Port 5601
- **SSL/TLS Enabled**: Certificate-based security
- **Cluster Name**: docker-cluster

### Memory Configuration

**Current Setup (Per Node):**
```yaml
# Elasticsearch Node Configuration
mem_limit: 10737418240        # 10GB total container memory
ES_JAVA_OPTS: -Xms8g -Xmx8g  # 8GB JVM heap (80% of mem_limit)
```

**Kibana Configuration:**
```yaml
mem_limit: 6442450944  # 6GB total container memory
```

### 🔧 **Scaling Memory - IMPORTANT**

When upgrading server memory, you **MUST** maintain the proportion:

**Formula:** `ES_JAVA_OPTS heap = 80% of mem_limit`

**Examples:**

| Server RAM | mem_limit | ES_JAVA_OPTS | Calculation |
|------------|-----------|--------------|-------------|
| 10GB       | 10GB      | -Xms8g -Xmx8g | 10GB × 0.8 = 8GB |
| 16GB       | 16GB      | -Xms12g -Xmx12g | 16GB × 0.8 = 12GB |
| 32GB       | 32GB      | -Xms25g -Xmx25g | 32GB × 0.8 = 25GB |
| 64GB       | 64GB      | -Xms51g -Xmx51g | 64GB × 0.8 = 51GB |

**To Upgrade to 16GB per node:**
```yaml
environment:
  - ES_JAVA_OPTS=-Xms12g -Xmx12g   # 80% of 16GB
mem_limit: 17179869184              # 16GB in bytes
```

**To Convert GB to Bytes:**
```
GB × 1024 × 1024 × 1024 = bytes
16 × 1024³ = 17179869184
```

### Starting the Cluster

```bash
# Navigate to docker directory
cd elastic-start-local

# Start all services
docker-compose up -d

# Check cluster health
curl -k -u elastic:nicolasprojetoflavio22 https://localhost:9200/_cluster/health?pretty

# Access Kibana
# Open browser: http://localhost:5601
# Username: elastic
# Password: nicolasprojetoflavio22
```

### Stopping the Cluster

```bash
# Stop services (keep data)
docker-compose down

# Stop and remove data (CAUTION!)
docker-compose down -v
```

---

## Usage

### Running PA Script (2008+)

```bash
# Edit the script to set your directory
# File: rpa_sus/functions/dbf/pa/send_elastic_dbf_pa_2008_plus.py
# Line 47: dbf_directory = '/mnt/volume_nyc1_01/nicolas/PA/Ceara'

# Run the script
cd rpa_sus/functions/dbf/pa
python send_elastic_dbf_pa_2008_plus.py
```

**Expected Output:**
```
2024-10-07 10:15:23,045 - INFO - Connected to Elasticsearch!
2024-10-07 10:15:23,123 - INFO - Starting parallel indexing process.
2024-10-07 10:15:23,145 - INFO - Found 156 DBF files to process
2024-10-07 10:15:23,156 - INFO - Using 8 worker processes for batch processing of 12 files at a time
2024-10-07 10:15:24,234 - INFO - Processing batch 1: files 1-12 of 156
2024-10-07 10:15:25,345 - INFO - Reading DBF file: PAAC2401.dbf (1/156)
2024-10-07 10:15:26,456 - INFO - Processing chunk 1 for file 'PAAC2401.dbf' (Index: 'sus_data_paac2401', Size: 5000)
...
2024-10-07 12:45:30,789 - INFO - Total records indexed: 1,234,567
2024-10-07 12:45:30,790 - INFO - Total time: 150 minutes and 7.78 seconds
2024-10-07 12:45:30,791 - INFO - Average indexing rate: 136.97 records/second
```

### Running RD Script (2008+)

```bash
# Edit the script to set your directory
# File: rpa_sus/functions/dbf/rd/send_elastic_dbf_rd_2008_plus.py
# Line 44: dbf_directory = '/mnt/volume_nyc1_01/nicolas/RD_2008+'

# Run the script
cd rpa_sus/functions/dbf/rd
python send_elastic_dbf_rd_2008_plus.py
```

### Processing CSV Files

```bash
# Edit directory path
# File: rpa_sus/sendElasticMultipleFilesCSV.py
# Line 119: csv_directory = './data/csv'

python rpa_sus/sendElasticMultipleFilesCSV.py
```

---

## Performance Tuning

### System Requirements
- **CPU**: 8 cores (minimum 4)
- **RAM**: 64GB (minimum 32GB)
- **Storage**: SSD recommended for Elasticsearch data
- **Network**: 1Gbps+ for cluster communication

### Optimization Tips

#### 1. **Adjust Chunk Size**
```python
# In database.py
CHUNK_SIZE = 5000  # Increase for more RAM, decrease for stability
```
- **Larger chunks**: Faster but more memory
- **Smaller chunks**: Slower but more stable

#### 2. **Worker Count**
```python
NUM_PROCESSES = 8  # Match your CPU cores
```
- Don't exceed CPU core count
- Leave 1-2 cores for system

#### 3. **Batch File Processing**
```python
# In script (line ~599)
batch_size = 12  # Files processed simultaneously
```
- Increase with more RAM
- Decrease if running out of memory

#### 4. **Elasticsearch Circuit Breakers**
```yaml
# In docker-compose.yml
indices.breaker.total.limit=85%      # Total memory limit
indices.breaker.request.limit=60%    # Per-request limit
indices.breaker.fielddata.limit=60%  # Field data limit
```

#### 5. **Index Refresh Interval**
```python
"refresh_interval": "60s"  # Higher = faster indexing
```
- Set to `"60s"` during bulk indexing
- Set to `"1s"` for near real-time search

---

## Troubleshooting

### Common Issues

#### 1. **Out of Memory Errors**
```
Solution 1: Reduce CHUNK_SIZE to 2500
Solution 2: Reduce batch_size to 6 files
Solution 3: Increase Docker mem_limit
```

#### 2. **Circuit Breaker Errors**
```
[elasticsearch] circuit_breaking_exception
```
**Fix:**
- Increase ES_JAVA_OPTS heap size
- Reduce chunk size in Python script
- Add delay between batches

#### 3. **Connection Timeout**
```
ConnectionTimeout: Connection timeout after 120s
```
**Fix:**
```python
# Increase timeout
).options(request_timeout=240)  # 4 minutes
```

#### 4. **Too Many Requests (429)**
```
elasticsearch.exceptions.ConnectionError: 429, Too Many Requests
```
**Fix:** Script automatically retries with backoff
- Check Elasticsearch CPU/memory usage
- Reduce NUM_PROCESSES

#### 5. **SSL Certificate Errors**
```
ssl.SSLError: [SSL: CERTIFICATE_VERIFY_FAILED]
```
**Fix:** Already disabled in scripts:
```python
verify_certs=False
```

---

## Monitoring & Verification

### Check Elasticsearch Indices
```bash
# List all indices
curl -k -u elastic:nicolasprojetoflavio22 \
  https://localhost:9200/_cat/indices?v

# Check specific index
curl -k -u elastic:nicolasprojetoflavio22 \
  https://localhost:9200/sus_data_paac2401/_count
```

### Kibana Dashboards
1. Open http://localhost:5601
2. Navigate to **Management** → **Stack Management** → **Index Patterns**
3. Create pattern: `sus_data_*`
4. Go to **Analytics** → **Discover** to explore data

### Script Logs
All scripts log to console with timestamps:
- **INFO**: Normal operations
- **WARNING**: Recoverable issues
- **ERROR**: Failed operations with details

---

## Data Dictionary

### PA (Ambulatorial) Fields
| Field | Type | Description | Example |
|-------|------|-------------|---------|
| PA_CODUNI | keyword | Healthcare unit code | "2082586" |
| PA_GESTAO | keyword | Management entity | "230440" |
| PA_UFMUN | keyword | State name (converted) | "Ceará" |
| PA_CMP | keyword | Competency month | "202401" |
| PA_PROC_ID | integer | Procedure ID | 0301060010 |
| PA_VALAPR | float | Approved value (R$) | 150.50 |
| @DATA | date | Indexed timestamp | "2024-01-15T00:00:00" |
| SOURCE | keyword | Data source | "SIA" |

### RD (Hospitalar) Fields
| Field | Type | Description | Example |
|-------|------|-------------|---------|
| UF_ZI | keyword | State code | "23" |
| ANO_CMPT | keyword | Competency year | "2024" |
| MES_CMPT | keyword | Competency month | "01" |
| N_AIH | keyword | Authorization number | "0123456789012" |
| CGC_HOSP | keyword | Hospital CNPJ | "12345678000190" |
| VAL_TOT | float | Total value (R$) | 5432.10 |
| DIAS_PERM | integer | Length of stay (days) | 7 |
| @DATA | date | Indexed timestamp | "2024-01-15T00:00:00" |
| SOURCE | keyword | Data source | "SIH" |

---

## License

This project processes public healthcare data from DATASUS (Brazilian Ministry of Health).

---

## Support

For questions about the data processing scripts, refer to:
- [**Anotações Gerais**](#) - Project notes and additional documentation
- Script source code comments
- [Elasticsearch Documentation](https://www.elastic.co/guide/)

---

**Last Updated**: October 2025
**Version**: 2.0 (2008+ Scripts)
