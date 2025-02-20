## Project Overview 
This project is an ElasticSearch Data Mining Tool designed to interact with an ElasticSearch cluster. It provides functionalities to create indices, load data, query data, export data, and perform various operations on the ElasticSearch cluster. The project is structured into several Python files, each serving a specific purpose.

## File Descriptions

### 1. .env
The .env file contains environment variables used to configure the connection to the ElasticSearch cluster. These variables include the host, port, username, password, and the default index name.

### 2. config.py
The config.py file is responsible for loading the environment variables from the .env file using the `dotenv` library. It sets up the configuration parameters required to connect to the ElasticSearch cluster.

```python
import os
from dotenv import load_dotenv

load_dotenv()

ES_HOST = os.getenv("ES_HOST", "localhost")
ES_PORT = int(os.getenv("ES_PORT", 9200))
ES_USERNAME = os.getenv("ES_USERNAME", "")
ES_PASSWORD = os.getenv("ES_PASSWORD", "")
ES_INDEX = os.getenv("ES_INDEX", "my_index")
```

### 3. data_loader.py
The data_loader.py file contains the `DataLoader` class, which is used to load data into ElasticSearch. It supports loading data from JSON and CSV files and generating sample product data.

```python
import json
import csv
import random
from datetime import datetime, timedelta
from es_connector import ElasticSearchConnector
from config import ES_INDEX
import logging

class DataLoader:
    """Class for loading sample data into ElasticSearch"""
    
    def __init__(self, index_name=ES_INDEX):
        """Initialize with connection to ElasticSearch"""
        self.logger = logging.getLogger(__name__)
        self.es_connector = ElasticSearchConnector()
        self.index_name = index_name
    
    def load_from_json(self, filepath):
        """Load data from a JSON file"""
        # Implementation here...
    
    def load_from_csv(self, filepath, id_field=None):
        """Load data from a CSV file"""
        # Implementation here...
    
    def generate_sample_products(self, count=1000):
        """Generate sample product data"""
        # Implementation here...

# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    loader = DataLoader()
    loader.generate_sample_products(1000)
```

### 4. data_miner.py
The data_miner.py file contains the `ElasticSearchMiner` class, which is used to mine data from ElasticSearch. It provides methods to get index mappings, retrieve all documents, scan large datasets, query documents, get document counts, and export data to CSV and JSON files.

```python
from elasticsearch.helpers import scan
from es_connector import ElasticSearchConnector
from config import ES_INDEX
import logging

class ElasticSearchMiner:
    """Class for mining data from ElasticSearch"""
    
    def __init__(self, index_name=ES_INDEX):
        """Initialize the miner with connection to ElasticSearch"""
        self.logger = logging.getLogger(__name__)
        self.index_name = index_name
        self.es_connector = ElasticSearchConnector()
        self.es = self.es_connector.es
    
    def get_index_mapping(self):
        """Get the mapping of an index"""
        # Implementation here...
    
    def get_all_documents(self, size=1000):
        """Get all documents from an index"""
        # Implementation here...
    
    def scan_all_documents(self):
        """Scan all documents using the scan helper (for large datasets)"""
        # Implementation here...
    
    def query_documents(self, query_dict):
        """Query documents with a properly formatted query dictionary"""
        # Implementation here...
    
    def get_document_count(self):
        """Get the total number of documents in the index"""
        # Implementation here...
    
    def export_to_csv(self, filepath, query=None, fields=None):
        """Export search results to CSV file"""
        # Implementation here...
    
    def export_to_json(self, filepath, query=None):
        """Export search results to JSON file"""
        # Implementation here...
    
    def aggregate_data(self, agg_query):
        """Run aggregation queries"""
        # Implementation here...
```

### 5. es_connector.py
The es_connector.py file contains the `ElasticSearchConnector` class, which handles the connection to the ElasticSearch cluster and provides methods for common ElasticSearch operations such as creating indices, indexing documents, bulk indexing, searching, and deleting indices.

```python
from elasticsearch import Elasticsearch
import logging
from config import ES_HOST, ES_PORT, ES_USERNAME, ES_PASSWORD

class ElasticSearchConnector:
    """A connector class for ElasticSearch operations"""
    
    def __init__(self):
        """Initialize ElasticSearch connection"""
        self.logger = logging.getLogger(__name__)
        # Implementation here...
    
    def create_index(self, index_name, mapping=None):
        """Create a new index with optional mapping"""
        # Implementation here...
    
    def index_document(self, index_name, doc_id, document):
        """Index a document into ElasticSearch"""
        # Implementation here...
    
    def bulk_index(self, index_name, documents):
        """Bulk index multiple documents"""
        # Implementation here...
    
    def search(self, index_name, query):
        """Search documents in ElasticSearch"""
        # Implementation here...
    
    def delete_index(self, index_name):
        """Delete an index"""
        # Implementation here...
```

### 6. main.py
The main.py file is the entry point of the application. It sets up logging, parses command-line arguments, and executes the requested operations using the `ElasticSearchConnector` and `ElasticSearchMiner` classes.

```python
import logging
import argparse
import json
from es_connector import ElasticSearchConnector
from data_miner import ElasticSearchMiner
from config import ES_INDEX

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("elastic_search_app.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def setup_argparse():
    """Set up command line arguments"""
    # Implementation here...

def execute_operation(args):
    """Execute the requested operation"""
    # Implementation here...

if __name__ == "__main__":
    args = setup_argparse()
    execute_operation(args)
```

This project provides a comprehensive toolset for interacting with an ElasticSearch cluster, making it easier to manage and analyze data stored in ElasticSearch.

### 7. Run the project
##### Step 1: Create a virtual environment
- *Install ElasticSearch*
Get the Username and password.

##### Step 2: Create a virtual environment
- *Open Command Prompt and run*
```python
python -m venv es_project_env
es_project_env\Scripts\activate
```

##### Step 3: Install required packages
- *Install the required packages in the env*
```python
pip install elasticsearch python-dotenv pandas requests
```

##### Step 4: Run Data-Mining Operation

- *Count documents in the index*
```powershell
python main.py --operation count
```

- *Export data to CSV*
```powershell
python main.py --operation export_csv --output products.csv
```

- *Query specific documents (price > 500)*
```powershell
python main.py --operation query --query '{\"query\": {\"range\": {\"price\": {\"gt\": 500}}}}'
```

- *Export specific fields to CSV*
```powershell
python main.py --operation export_csv --output expensive_products.csv --fields "product_id,name,price,category" --query '{\"query\": {\"range\": {\"price\": {\"gt\": 500}}}}'
```

## 8. License
This project is licensed under the MIT License.