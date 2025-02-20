import os
from dotenv import load_dotenv

load_dotenv()

ES_HOST = os.getenv("ES_HOST", "localhost")
ES_PORT = int(os.getenv("ES_PORT", 9200))
ES_USERNAME = os.getenv("ES_USERNAME", "")
ES_PASSWORD = os.getenv("ES_PASSWORD", "")
ES_INDEX = os.getenv("ES_INDEX", "my_index")