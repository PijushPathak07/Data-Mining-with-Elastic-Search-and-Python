# data_loader.py
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
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
            
            if isinstance(data, list):
                self.es_connector.bulk_index(self.index_name, data)
                self.logger.info(f"Loaded {len(data)} documents from {filepath}")
            else:
                self.es_connector.index_document(self.index_name, '1', data)
                self.logger.info(f"Loaded 1 document from {filepath}")
                
            return True
        except Exception as e:
            self.logger.error(f"Error loading from JSON: {str(e)}")
            raise
    
    def load_from_csv(self, filepath, id_field=None):
        """Load data from a CSV file"""
        try:
            documents = []
            
            with open(filepath, 'r', newline='') as f:
                reader = csv.DictReader(f)
                for i, row in enumerate(reader):
                    # Clean the row data
                    doc = {k: v for k, v in row.items() if v}
                    
                    # Use specified field as ID or generate one
                    if id_field and id_field in doc:
                        doc_id = doc[id_field]
                    else:
                        doc_id = str(i)
                    
                    documents.append({"_id": doc_id, **doc})
            
            self.es_connector.bulk_index(self.index_name, documents)
            self.logger.info(f"Loaded {len(documents)} documents from {filepath}")
            return True
        except Exception as e:
            self.logger.error(f"Error loading from CSV: {str(e)}")
            raise
    
    def generate_sample_products(self, count=1000):
        """Generate sample product data"""
        try:
            products = []
            categories = ["Electronics", "Clothing", "Home & Kitchen", "Books", "Sports"]
            
            for i in range(count):
                product = {
                    "product_id": f"PROD-{i+1000}",
                    "name": f"Product {i+1}",
                    "category": random.choice(categories),
                    "price": round(random.uniform(10, 1000), 2),
                    "in_stock": random.choice([True, False]),
                    "rating": round(random.uniform(1, 5), 1),
                    "created_at": (datetime.now() - timedelta(days=random.randint(1, 365))).isoformat()
                }
                products.append(product)
            
            # Create product mapping
            mapping = {
                "mappings": {
                    "properties": {
                        "product_id": {"type": "keyword"},
                        "name": {"type": "text"},
                        "category": {"type": "keyword"},
                        "price": {"type": "float"},
                        "in_stock": {"type": "boolean"},
                        "rating": {"type": "float"},
                        "created_at": {"type": "date"}
                    }
                }
            }
            
            # Create index with mapping
            self.es_connector.create_index(self.index_name, mapping)
            
            # Bulk index the products
            self.es_connector.bulk_index(self.index_name, products)
            self.logger.info(f"Generated and indexed {len(products)} sample products")
            return True
        except Exception as e:
            self.logger.error(f"Error generating sample data: {str(e)}")
            raise

# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    loader = DataLoader()
    loader.generate_sample_products(1000)