from elasticsearch_dsl import Document, Date, Integer, Keyword, Text
from elasticsearch_dsl.connections import connections

# Connexion à Elasticsearch avec gestion d'erreur et retry
import time

def connect_elasticsearch(max_retries=3, delay=5):
    retries = 0
    while retries < max_retries:
        try:
            connections.create_connection(hosts=['http://localhost:9200'])
            print("Connexion à Elasticsearch réussie!")
            return True
        except Exception as e:
            retries += 1
            print(f"Tentative {retries}/{max_retries} - Elasticsearch n'est pas disponible: {str(e)}")
            if retries < max_retries:
                print(f"Nouvelle tentative dans {delay} secondes...")
                time.sleep(delay)
    return False

ELASTICSEARCH_AVAILABLE = connect_elasticsearch()

class TransactionDocument(Document):
    # Champs pour la recherche full-text
    transaction_id = Keyword()
    payment_method = Text()
    country = Text()
    product_category = Text()
    status = Keyword()
    amount = Integer()
    customer_rating = Integer()
    timestamp = Date()
    user_id = Keyword()
    user_name = Text()
    product_id = Keyword()
    dataset_source = Keyword()  
    version = Keyword()  

    class Index:
        name = 'transactions'
        settings = {
            'number_of_shards': 1,
            'number_of_replicas': 0
        }

    def save(self, **kwargs):
        return super().save(**kwargs)

# Création de l'index
def init_elasticsearch():
    TransactionDocument.init()
