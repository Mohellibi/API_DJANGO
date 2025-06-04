from django.core.management.base import BaseCommand
from myapp.documents import TransactionDocument, init_elasticsearch
from myapp.models import DataLakeVersion
import os
import json
from datetime import datetime

class Command(BaseCommand):
    help = 'Index all data from the data lake into Elasticsearch'

    def handle(self, *args, **options):
        # Initialiser l'index Elasticsearch
        init_elasticsearch()
        self.stdout.write('Initialized Elasticsearch index')

        # Parcourir toutes les versions du data lake
        versions = DataLakeVersion.objects.all()
        for version in versions:
            self.stdout.write(f'Processing version: {version.name}')
            
            # Parcourir tous les datasets dans cette version
            base_path = version.path
            for dataset_name in os.listdir(base_path):
                dataset_path = os.path.join(base_path, dataset_name)
                if not os.path.isdir(dataset_path):
                    continue

                self.stdout.write(f'Indexing dataset: {dataset_name}')
                
                # Parcourir tous les fichiers JSON dans le dataset
                for filename in os.listdir(dataset_path):
                    if not filename.endswith('.json'):
                        continue

                    file_path = os.path.join(dataset_path, filename)
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                            if isinstance(data, list):
                                items = data
                            else:
                                items = [data]

                            # Indexer chaque transaction
                            for item in items:
                                # Convertir le timestamp si présent
                                timestamp = item.get('TIMESTAMP')
                                if timestamp:
                                    timestamp = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")

                                # Créer le document Elasticsearch
                                doc = TransactionDocument(
                                    meta={'id': f"{version.name}_{dataset_name}_{item.get('TRANSACTION_ID', '')}"},
                                    transaction_id=item.get('TRANSACTION_ID', ''),
                                    payment_method=item.get('PAYMENT_METHOD', ''),
                                    country=item.get('LOCATION', {}).get('COUNTRY', ''),
                                    product_category=item.get('PRODUCT_CATEGORY', ''),
                                    status=item.get('STATUS', ''),
                                    amount=float(item.get('AMOUNT', 0)),
                                    customer_rating=item.get('CUSTOMER_RATING', 0),
                                    timestamp=timestamp,
                                    user_id=item.get('USER_ID', ''),
                                    user_name=item.get('USER_NAME', ''),
                                    product_id=item.get('PRODUCT_ID', ''),
                                    dataset_source=dataset_name,
                                    version=version.name
                                )
                                doc.save()

                    except Exception as e:
                        self.stdout.write(self.style.ERROR(f'Error processing file {file_path}: {str(e)}'))

        self.stdout.write(self.style.SUCCESS('Successfully indexed all data'))
