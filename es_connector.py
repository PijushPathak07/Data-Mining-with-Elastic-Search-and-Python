from elasticsearch import Elasticsearch
import logging
from config import ES_HOST, ES_PORT, ES_USERNAME, ES_PASSWORD

class ElasticSearchConnector:
    """A connector class for ElasticSearch operations"""
    
    def __init__(self):
        """Initialize ElasticSearch connection"""
        self.logger = logging.getLogger(__name__)

        # Connect to ElasticSearch using HTTPS
        connection_params = {
            'hosts': [f'https://{ES_HOST}:{ES_PORT}'],
            'verify_certs': False  # Disable SSL verification for local setup
        }

        # Add authentication if provided
        if ES_USERNAME and ES_PASSWORD:
            connection_params['basic_auth'] = (ES_USERNAME, ES_PASSWORD)

        try:
            self.es = Elasticsearch(**connection_params)
            if not self.es.ping():
                self.logger.error("Could not connect to ElasticSearch")
                raise ConnectionError("Failed to connect to ElasticSearch")
            self.logger.info("Successfully connected to ElasticSearch")
        except Exception as e:
            self.logger.error(f"Error connecting to ElasticSearch: {str(e)}")
            raise

    
    def create_index(self, index_name, mapping=None):
        """Create a new index with optional mapping"""
        try:
            if not self.es.indices.exists(index=index_name):
                if mapping:
                    self.es.indices.create(index=index_name, body=mapping)
                else:
                    self.es.indices.create(index=index_name)
                self.logger.info(f"Index '{index_name}' created successfully")
                return True
            self.logger.info(f"Index '{index_name}' already exists")
            return False
        except Exception as e:
            self.logger.error(f"Error creating index '{index_name}': {str(e)}")
            raise
    
    def index_document(self, index_name, doc_id, document):
        """Index a document into ElasticSearch"""
        try:
            response = self.es.index(index=index_name, id=doc_id, document=document)
            self.logger.info(f"Document indexed with ID: {doc_id}")
            return response
        except Exception as e:
            self.logger.error(f"Error indexing document: {str(e)}")
            raise
    
    def bulk_index(self, index_name, documents):
        """Bulk index multiple documents"""
        try:
            body = []
            for i, doc in enumerate(documents):
                body.append({"index": {"_index": index_name, "_id": str(i) if '_id' not in doc else doc['_id']}})
                body.append(doc)
            
            response = self.es.bulk(operations=body)
            self.logger.info(f"Bulk indexed {len(documents)} documents")
            return response
        except Exception as e:
            self.logger.error(f"Error during bulk indexing: {str(e)}")
            raise
    
    def search(self, index_name, query):
        """Search documents in ElasticSearch"""
        try:
            response = self.es.search(index=index_name, body=query)
            self.logger.info(f"Search executed successfully, found {len(response['hits']['hits'])} results")
            return response
        except Exception as e:
            self.logger.error(f"Error during search: {str(e)}")
            raise
    
    def delete_index(self, index_name):
        """Delete an index"""
        try:
            if self.es.indices.exists(index=index_name):
                self.es.indices.delete(index=index_name)
                self.logger.info(f"Index '{index_name}' deleted successfully")
                return True
            self.logger.info(f"Index '{index_name}' does not exist")
            return False
        except Exception as e:
            self.logger.error(f"Error deleting index '{index_name}': {str(e)}")
            raise