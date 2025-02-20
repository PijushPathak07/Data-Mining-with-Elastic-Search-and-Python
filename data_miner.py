# data_miner.py
import json
import pandas as pd
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
        try:
            mapping = self.es.indices.get_mapping(index=self.index_name)
            return mapping
        except Exception as e:
            self.logger.error(f"Error getting mapping: {str(e)}")
            raise
    
    def get_all_documents(self, size=1000):
        """Get all documents from an index"""
        try:
            query = {"match_all": {}}
            response = self.es_connector.search(self.index_name, query={"query": query, "size": size})
            return response['hits']['hits']
        except Exception as e:
            self.logger.error(f"Error retrieving all documents: {str(e)}")
            raise
    
    def scan_all_documents(self):
        """Scan all documents using the scan helper (for large datasets)"""
        try:
            documents = []
            scan_result = scan(
                client=self.es,
                index=self.index_name,
                query={"query": {"match_all": {}}}
            )
            
            for doc in scan_result:
                documents.append(doc)
                
            self.logger.info(f"Scanned {len(documents)} documents from index {self.index_name}")
            return documents
        except Exception as e:
            self.logger.error(f"Error scanning documents: {str(e)}")
            raise
    
    def query_documents(self, query_dict):
        """Query documents with a properly formatted query dictionary"""
        try:
            # Ensure the query_dict is structured correctly
            if "query" not in query_dict:
                query_dict = {"query": query_dict}  # Wrap it properly

            response = self.es.search(index=self.index_name, body=query_dict)  # Fix: Use 'body=' instead of 'query='
            return response['hits']['hits']
        except Exception as e:
            self.logger.error(f"Error querying documents: {str(e)}")
            raise

    
    def get_document_count(self):
        """Get the total number of documents in the index"""
        try:
            count = self.es.count(index=self.index_name)
            return count['count']
        except Exception as e:
            self.logger.error(f"Error getting document count: {str(e)}")
            raise
    
    def export_to_csv(self, filepath, query=None, fields=None):
        """Export search results to CSV file"""
        try:
            if query is None:
                query = {"match_all": {}}  # Correct default query

            search_query = {"query": query}  # Wrap it correctly

            if fields:
                search_query["_source"] = fields  # Include only selected fields

            docs = self.query_documents(query)
            df = pd.DataFrame([doc['_source'] for doc in docs])

            df.to_csv(filepath, index=False)
            self.logger.info(f"Exported {len(df)} documents to {filepath}")
            return df
        except Exception as e:
            self.logger.error(f"Error exporting to CSV: {str(e)}")
            raise

    
    def export_to_json(self, filepath, query=None):
        """Export search results to JSON file"""
        try:
            if query is None:
                query = {"match_all": {}}  # Default query
                
            search_query = {"query": query}  # Correct structure
    
            docs = self.query_documents(query)
            results = [doc['_source'] for doc in docs]
    
            with open(filepath, 'w') as f:
                json.dump(results, f, indent=2)
    
            self.logger.info(f"Exported {len(results)} documents to {filepath}")
            return results
        except Exception as e:
            self.logger.error(f"Error exporting to JSON: {str(e)}")
            raise
            
    def aggregate_data(self, agg_query):
        """Run aggregation queries"""
        try:
            response = self.es.search(index=self.index_name, body={"size": 0, "aggs": agg_query})
            return response['aggregations']
        except Exception as e:
            self.logger.error(f"Error running aggregation: {str(e)}")
            raise