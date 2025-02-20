# main.py
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
    parser = argparse.ArgumentParser(description='ElasticSearch Data Mining Tool')
    parser.add_argument('--operation', required=True, choices=[
        'create_index', 'export_csv', 'export_json', 'query', 'count', 'delete_index'
    ], help='Operation to perform')
    
    parser.add_argument('--index', default=ES_INDEX, help='Index name to work with')
    parser.add_argument('--query', help='JSON query string (for query operation)')
    parser.add_argument('--output', help='Output file path for export operations')
    parser.add_argument('--fields', help='Comma-separated fields to export (for CSV export)')
    
    return parser.parse_args()

def execute_operation(args):
    """Execute the requested operation"""
    try:
        # Create connector and miner
        es_connector = ElasticSearchConnector()
        miner = ElasticSearchMiner(index_name=args.index)
        
        # Execute the requested operation
        if args.operation == 'create_index':
            es_connector.create_index(args.index)
            logger.info(f"Index '{args.index}' created successfully")
            
        elif args.operation == 'export_csv':
            if not args.output:
                args.output = f"{args.index}_export.csv"
            
            query = json.loads(args.query) if args.query else None
            fields = args.fields.split(',') if args.fields else None
            
            miner.export_to_csv(args.output, query, fields)
            logger.info(f"Data exported to CSV: {args.output}")
            
        elif args.operation == 'export_json':
            if not args.output:
                args.output = f"{args.index}_export.json"
                
            query = json.loads(args.query) if args.query else None
            
            miner.export_to_json(args.output, query)
            logger.info(f"Data exported to JSON: {args.output}")
            
        elif args.operation == 'query':
            if not args.query:
                logger.error("Query string is required for query operation")
                return
                
            query_dict = json.loads(args.query)
            results = miner.query_documents(query_dict)
            
            print(f"Found {len(results)} documents")
            for i, doc in enumerate(results[:5]):
                print(f"\nDocument {i+1}:")
                print(json.dumps(doc, indent=2))
            
            if len(results) > 5:
                print(f"\n... and {len(results) - 5} more documents")
                
        elif args.operation == 'count':
            count = miner.get_document_count()
            print(f"Total documents in index '{args.index}': {count}")
            
        elif args.operation == 'delete_index':
            es_connector.delete_index(args.index)
            logger.info(f"Index '{args.index}' deleted successfully")
            
    except Exception as e:
        logger.error(f"Error executing operation: {str(e)}")
        raise

if __name__ == "__main__":
    args = setup_argparse()
    execute_operation(args)