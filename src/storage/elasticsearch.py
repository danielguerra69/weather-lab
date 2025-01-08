from elasticsearch import Elasticsearch, helpers
from datetime import datetime
import time
import json
import logging

class ElasticsearchStorage:
    def __init__(self, es_url, retry_delay=10):
        self.es_url = es_url
        self.retry_delay = retry_delay
        self.es = None
        self.connect()

    def connect(self):
        while True:
            try:
                logging.info(f"Attempting to connect to Elasticsearch at {self.es_url}")
                self.es = Elasticsearch(
                    [self.es_url],
                    headers={"Content-Type": "application/json"},
                    connections_per_node=10
                )
            except Exception as e:
                logging.error(f"Connection attempt failed: {e}")
            else:
                return
            time.sleep(self.retry_delay)

    def index_data(self, data):
        logging.info("index_data method called")
        try:
            doc_id = f"{data['latitude']}-{data['longitude']}-{data['timestamp']}"
            timestamp = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
            index_name = f"weather_data-{timestamp.strftime('%Y-%m-%d')}"
            logging.info(f"Indexing record with _id: {doc_id} to index: {index_name}")
            self.es.index(index=index_name, id=doc_id, body=data)
            logging.info(f"Indexed record with _id: {doc_id}")
        except Exception as e:
            logging.error(f"Error indexing data: {e}")

    def bulk_index_data(self, data_list):
        # logging.info("bulk_index_data method called")
        actions = []
        for data in data_list:
            try:
                doc_id = f"{data['latitude']}-{data['longitude']}-{data['timestamp']}"
                timestamp = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
                index_name = f"weather_data-{timestamp.strftime('%Y-%m-%d')}"
                action = {
                    "_index": index_name,
                    "_id": doc_id,
                    "_source": data
                }
                actions.append(action)
            except Exception as e:
                logging.error(f"Error preparing data for bulk indexing: {e}")

        if actions:
            try:
                helpers.bulk(self.es, actions)
                logging.info(f"Bulk indexed {len(actions)} records")
            except Exception as e:
                logging.error(f"Error during bulk indexing: {e}")

    def create_index(self, index_name):
        try:
            if not self.es.indices.exists(index=index_name):
                self.es.indices.create(index=index_name)
                logging.info(f"Created index: {index_name}")
        except Exception as e:
            logging.error(f"Error creating index: {e}")

    def delete_index(self, index_name):
        try:
            if self.es.indices.exists(index=index_name):
                self.es.indices.delete(index=index_name)
                logging.info(f"Deleted index: {index_name}")
        except Exception as e:
            logging.error(f"Error deleting index: {e}")