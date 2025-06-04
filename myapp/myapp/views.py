from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.response import Response
from rest_framework.authentication import BasicAuthentication
from rest_framework.permissions import IsAuthenticated
from rest_framework import viewsets, filters, status
from django_filters.rest_framework import DjangoFilterBackend, FilterSet
from django_filters import rest_framework
import os
import json
from django.conf import settings
from django.utils import timezone
from datetime import timedelta, datetime
from django.db.models import Sum, Count
from django.db.models.functions import Coalesce
from django.db.models import FloatField, DecimalField
from elasticsearch_dsl import Q
from . import documents

from .models import AccessRight, AccessRightSerializer, AccessLog, Transaction, TransactionSerializer, DataLakeVersion, DetailedAccessLog

def log_access(request):
    AccessLog.objects.create(
        user=request.user,
        request_path=request.path,
        request_method=request.method,
        request_body=request.body.decode('utf-8') if request.body else ''
    )

@api_view(['GET'])
@authentication_classes([BasicAuthentication])
@permission_classes([IsAuthenticated])
def access_right_list(request):
    log_access(request)
    access_rights = AccessRight.objects.select_related('user').all()
    serializer = AccessRightSerializer(access_rights, many=True)
    return Response(serializer.data)

def check_dataset_access(user, dataset_name):
    """Vérifie si l'utilisateur a accès au dataset"""
    return AccessRight.objects.filter(user=user, dataset_name=dataset_name).exists()

@api_view(['GET'])
@authentication_classes([BasicAuthentication])
@permission_classes([IsAuthenticated])
def retrieve_all(request):
    log_access(request)
    
    # Récupérer tous les datasets auxquels l'utilisateur a accès
    user_datasets = AccessRight.objects.filter(user=request.user).values_list('dataset_name', flat=True)
    if not user_datasets:
        return Response({"detail": "You don't have access to any datasets"}, status=status.HTTP_403_FORBIDDEN)
    
    authorized_data = []
    for dataset in user_datasets:
        data = load_data_for_dataset(dataset)
        if data:
            authorized_data.extend(data if isinstance(data, list) else [data])
    
    # Récupérer le paramètre ?page= (int, par défaut 1)
    page = int(request.query_params.get('page', 1))
    start = (page - 1) * PAGE_SIZE
    end = start + PAGE_SIZE
    
    # Si hors bornes, renvoyer 404 ou liste vide
    if start >= len(authorized_data):
        return Response({"detail": "Page out of range"}, status=status.HTTP_404_NOT_FOUND)
    
    page_data = authorized_data[start:end]
    return Response({
        "page": page,
        "page_size": PAGE_SIZE,
        "total": len(authorized_data),
        "results": page_data
    })

DATA_LAKE_PATH = "C:/Users/yanis/OneDrive/Documents/M1 Data/Data integration/TP2_API/data_lake"
PAGE_SIZE = 10

def load_all_data():
    all_data = []
    for folder_name in os.listdir(DATA_LAKE_PATH):
        folder_path = os.path.join(DATA_LAKE_PATH, folder_name)
        if os.path.isdir(folder_path):
            for filename in os.listdir(folder_path):
                if filename.endswith('.json'):
                    file_path = os.path.join(folder_path, filename)
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            content = json.load(f)
                            if isinstance(content, list):
                                all_data.extend(content)
                            else:
                                all_data.append(content)
                    except Exception as e:
                        # ignore ou log error
                        pass
    return all_data

def load_data_for_dataset(dataset_name, base_path=None):
    """Version modifiée de load_data_for_dataset qui accepte un chemin de base"""
    if base_path is None:
        base_path = DATA_LAKE_PATH
        
    folder_path = os.path.join(base_path, dataset_name)
    if not os.path.exists(folder_path) or not os.path.isdir(folder_path):
        return None

    data = []
    for filename in os.listdir(folder_path):
        if filename.endswith('.json'):
            file_path = os.path.join(folder_path, filename)
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = json.load(f)
                    if isinstance(content, list):
                        data.extend(content)
                    else:
                        data.append(content)
            except Exception:
                # Ignorer ou logger l'erreur
                pass
    return data

@api_view(['GET'])
@authentication_classes([BasicAuthentication])
@permission_classes([IsAuthenticated])
def retrieve_projection(request, dataset_name):
    log_access(request)
    
    user_access = AccessRight.objects.filter(user=request.user, dataset_name=dataset_name)
    if not user_access.exists():
        return Response(
            {"detail": f"Access Denied: You don't have permission to access dataset '{dataset_name}'."}, 
            status=status.HTTP_403_FORBIDDEN
        )
    
    data = load_data_for_dataset(dataset_name)
    if data is None:
        return Response({"detail": f"Dataset '{dataset_name}' not found."}, status=status.HTTP_404_NOT_FOUND)

    page = int(request.query_params.get('page', 1))
    start = (page - 1) * PAGE_SIZE
    end = start + PAGE_SIZE

    if start >= len(data):
        return Response({"detail": "Page out of range"}, status=status.HTTP_404_NOT_FOUND)

    page_data = data[start:end]
    return Response({
        "dataset": dataset_name,
        "page": page,
        "page_size": PAGE_SIZE,
        "total": len(data),
        "results": page_data
    })

class TransactionFilter(FilterSet):
    amount_gt = rest_framework.NumberFilter(field_name='amount', lookup_expr='gt')
    amount_lt = rest_framework.NumberFilter(field_name='amount', lookup_expr='lt')
    amount_exact = rest_framework.NumberFilter(field_name='amount', lookup_expr='exact')
    
    rating_gt = rest_framework.NumberFilter(field_name='customer_rating', lookup_expr='gt')
    rating_lt = rest_framework.NumberFilter(field_name='customer_rating', lookup_expr='lt')
    rating_exact = rest_framework.NumberFilter(field_name='customer_rating', lookup_expr='exact')

    class Meta:
        model = Transaction
        fields = {
            'payment_method': ['exact', 'icontains'],
            'country': ['exact', 'icontains'],
            'product_category': ['exact', 'icontains'],
            'status': ['exact'],
            'user_id': ['exact'],
            'user_name': ['exact', 'icontains'],
            'product_id': ['exact'],
        }

class TransactionViewSet(viewsets.ModelViewSet):
    queryset = Transaction.objects.all()
    serializer_class = TransactionSerializer
    filter_backends = [DjangoFilterBackend, filters.SearchFilter, filters.OrderingFilter]
    filterset_class = TransactionFilter
    search_fields = ['payment_method', 'country', 'product_category', 'status', 'user_name']
    ordering_fields = ['amount', 'customer_rating', 'timestamp']
    
    def list(self, request, *args, **kwargs):
        if not check_dataset_access(request.user, 'TRANSACTIONS_COMPLETED'):
            return Response(
                {"detail": "Access Denied: You don't have permission to access TRANSACTIONS_COMPLETED"},
                status=status.HTTP_403_FORBIDDEN
            )
        
        log_access(request)
        return super().list(request, *args, **kwargs)
    
    def retrieve(self, request, *args, **kwargs):
        if not check_dataset_access(request.user, 'TRANSACTIONS_COMPLETED'):
            return Response(
                {"detail": "Access Denied: You don't have permission to access TRANSACTIONS_COMPLETED"},
                status=status.HTTP_403_FORBIDDEN
            )
        
        log_access(request)
        return super().retrieve(request, *args, **kwargs)

    def get_queryset(self):
        if not check_dataset_access(self.request.user, 'TRANSACTIONS_COMPLETED'):
            return Transaction.objects.none()
        
        if not Transaction.objects.exists():
            self.load_data_from_lake()
        return Transaction.objects.all()

    def load_data_from_lake(self):
        if not check_dataset_access(self.request.user, 'TRANSACTIONS_COMPLETED'):
            print("L'utilisateur n'a pas accès aux transactions complétées")
            return
            
        completed_transactions_path = os.path.join(DATA_LAKE_PATH, 'TRANSACTIONS_COMPLETED')
        
        if not os.path.exists(completed_transactions_path):
            print("Le dossier TRANSACTIONS_COMPLETED n'existe pas")
            return

        for filename in os.listdir(completed_transactions_path):
            if filename.endswith('.json'):
                file_path = os.path.join(completed_transactions_path, filename)
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        self.process_transaction_data(data)
                except json.JSONDecodeError:
                    print(f"Erreur lors de la lecture du fichier {filename}")
                except Exception as e:
                    print(f"Erreur inattendue avec le fichier {filename}: {str(e)}")

    def process_transaction_data(self, data):
        if isinstance(data, list):
            for item in data:
                self.create_transaction_from_data(item)
        else:
            self.create_transaction_from_data(data)

    def create_transaction_from_data(self, item):
        try:
            location = item.get('LOCATION', {})
            country = location.get('COUNTRY', 'Unknown')

            customer_rating = item.get('CUSTOMER_RATING')
            if customer_rating is None:
                customer_rating = 0

            timestamp = item.get('TIMESTAMP')
            if timestamp:
                from datetime import datetime
                timestamp = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
            else:
                timestamp = datetime.now()

            Transaction.objects.create(
                payment_method=item.get('PAYMENT_METHOD', 'Unknown'),
                country=country,
                product_category=item.get('PRODUCT_CATEGORY', 'Unknown'),
                status=item.get('STATUS', 'Unknown'),
                amount=float(item.get('AMOUNT', 0)),
                customer_rating=customer_rating,
                timestamp=timestamp,
                user_id=item.get('USER_ID', 'Unknown'),
                user_name=item.get('USER_NAME', 'Unknown'),
                product_id=item.get('PRODUCT_ID', 'Unknown')
            )
        except Exception as e:
            print(f"Erreur lors de la création de la transaction: {str(e)}")

@api_view(['GET'])
@authentication_classes([BasicAuthentication])
@permission_classes([IsAuthenticated])
def last_5_minutes_spent(request):
    log_access(request)
    
    if not check_dataset_access(request.user, 'TRANSACTIONS_COMPLETED'):
        return Response(
            {"detail": "Access Denied: You don't have permission to access TRANSACTIONS_COMPLETED"},
            status=status.HTTP_403_FORBIDDEN
        )
    
    five_minutes_ago = timezone.now() - timedelta(minutes=5)
    total_spent = Transaction.objects.filter(
        timestamp__gte=five_minutes_ago
    ).aggregate(
        total=Coalesce(Sum('amount', output_field=FloatField()), 0.0, output_field=FloatField())
    )['total']
    
    return Response({
        "time_range": "5 minutes",
        "start_time": five_minutes_ago,
        "end_time": timezone.now(),
        "total_spent": float(total_spent)
    })

@api_view(['GET'])
@authentication_classes([BasicAuthentication])
@permission_classes([IsAuthenticated])
def total_spent_by_user_type(request):
    log_access(request)
    
    if not check_dataset_access(request.user, 'TRANSACTIONS_COMPLETED'):
        return Response(
            {"detail": "Access Denied: You don't have permission to access TRANSACTIONS_COMPLETED"},
            status=status.HTTP_403_FORBIDDEN
        )
    
    try:
        # S'assurer que les données sont chargées
        if not Transaction.objects.exists():
            # Créer une instance du viewset pour utiliser sa méthode de chargement
            viewset = TransactionViewSet()
            viewset.request = request
            viewset.load_data_from_lake()
        
        stats = Transaction.objects.values(
            'user_id', 'payment_method', 'status'
        ).annotate(
            total_spent=Sum('amount', output_field=FloatField()),
            transaction_count=Count('id')
        ).order_by('user_id', 'status')
        
        # Convertir les valeurs décimales en float pour la sérialisation JSON        # Organiser les statistiques par utilisateur
        user_stats = {}
        for stat in stats:
            user_id = stat['user_id']
            if user_id not in user_stats:
                user_stats[user_id] = {
                    'user_id': user_id,
                    'total_spent': 0,
                    'total_transactions': 0,
                    'transactions_by_status': {},
                    'transactions_by_payment': {}
                }
            
            status = stat['status']
            payment = stat['payment_method']
            amount = float(stat['total_spent'] if stat['total_spent'] is not None else 0)
            count = stat['transaction_count']
            
            # Mettre à jour les totaux
            user_stats[user_id]['total_spent'] += amount
            user_stats[user_id]['total_transactions'] += count
            
            # Ajouter les stats par status
            if status not in user_stats[user_id]['transactions_by_status']:
                user_stats[user_id]['transactions_by_status'][status] = {
                    'count': 0,
                    'total': 0
                }
            user_stats[user_id]['transactions_by_status'][status]['count'] += count
            user_stats[user_id]['transactions_by_status'][status]['total'] += amount
            
            # Ajouter les stats par méthode de paiement
            if payment not in user_stats[user_id]['transactions_by_payment']:
                user_stats[user_id]['transactions_by_payment'][payment] = {
                    'count': 0,
                    'total': 0
                }
            user_stats[user_id]['transactions_by_payment'][payment]['count'] += count
            user_stats[user_id]['transactions_by_payment'][payment]['total'] += amount
        
        return Response({
            "total_users": len(user_stats),
            "users": list(user_stats.values())
        })
    except Exception as e:
        return Response({
            "error": str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
@authentication_classes([BasicAuthentication])
@permission_classes([IsAuthenticated])
def top_products(request):
    log_access(request)
    
    if not check_dataset_access(request.user, 'TRANSACTIONS_COMPLETED'):
        return Response(
            {"detail": "Access Denied: You don't have permission to access TRANSACTIONS_COMPLETED"},
            status=status.HTTP_403_FORBIDDEN
        )
    
    try:
        limit = int(request.query_params.get('limit', 10))
        if limit <= 0:
            return Response(
                {"error": "Limit must be a positive integer"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        top_products = Transaction.objects.values(
            'product_id', 'product_category'
        ).annotate(
            total_bought=Count('id'),
            total_spent=Sum('amount', output_field=FloatField())
        ).order_by('-total_bought')[:limit]
        
        # Convertir les valeurs décimales en float pour la sérialisation JSON
        products_list = [{
            'product_id': prod['product_id'],
            'product_category': prod['product_category'],
            'total_bought': prod['total_bought'],
            'total_spent': float(prod['total_spent'] if prod['total_spent'] is not None else 0)
        } for prod in top_products]
        
        return Response({
            "limit": limit,
            "products": products_list
        })
    except ValueError:
        return Response({
            "error": "Invalid limit parameter"
        }, status=status.HTTP_400_BAD_REQUEST)
    except Exception as e:
        return Response({
            "error": str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
@authentication_classes([BasicAuthentication])
@permission_classes([IsAuthenticated])
def list_data_lake_resources(request):
    """Liste toutes les ressources disponibles dans le data lake avec leurs versions"""
    log_access(request)
    
    versions = DataLakeVersion.objects.all()
    resources = {}
    
    for version in versions:
        path = version.path
        if not os.path.exists(path):
            continue
            
        resources[version.name] = {
            'version_info': {
                'name': version.name,
                'created_at': version.created_at,
                'is_active': version.is_active
            },
            'datasets': []
        }
        
        # Liste tous les dossiers dans cette version du data lake
        for item in os.listdir(path):
            item_path = os.path.join(path, item)
            if os.path.isdir(item_path):
                resources[version.name]['datasets'].append(item)
    
    return Response(resources)

@api_view(['GET'])
@authentication_classes([BasicAuthentication])
@permission_classes([IsAuthenticated])
def get_dataset_version(request, dataset_name, version_name):
    """Obtient une version spécifique d'un dataset"""
    log_access(request)
    
    try:
        version = DataLakeVersion.objects.get(name=version_name)
    except DataLakeVersion.DoesNotExist:
        DetailedAccessLog.objects.create(
            user=request.user,
            dataset_name=dataset_name,
            access_type='version_check',
            success=False,
            error_message=f"Version {version_name} not found"
        )
        return Response(
            {"error": f"Version {version_name} not found"},
            status=status.HTTP_404_NOT_FOUND
        )
    
    # Vérifier les droits d'accès
    access_right = AccessRight.objects.filter(
        user=request.user,
        dataset_name=dataset_name
    ).first()
    
    if not access_right:
        DetailedAccessLog.objects.create(
            user=request.user,
            dataset_name=dataset_name,
            version=version,
            access_type='read',
            success=False,
            error_message="Access denied"
        )
        return Response(
            {"error": "Access denied"},
            status=status.HTTP_403_FORBIDDEN
        )
    
    if not (access_right.can_access_all_versions or version in access_right.allowed_versions.all()):
        DetailedAccessLog.objects.create(
            user=request.user,
            dataset_name=dataset_name,
            version=version,
            access_type='read',
            success=False,
            error_message=f"No access to version {version_name}"
        )
        return Response(
            {"error": f"You don't have access to version {version_name}"},
            status=status.HTTP_403_FORBIDDEN
        )
    
    # Charger les données
    folder_path = os.path.join(version.path, dataset_name)
    if not os.path.exists(folder_path) or not os.path.isdir(folder_path):
        DetailedAccessLog.objects.create(
            user=request.user,
            dataset_name=dataset_name,
            version=version,
            access_type='read',
            success=False,
            error_message="Dataset not found"
        )
        return Response(
            {"error": "Dataset not found"},
            status=status.HTTP_404_NOT_FOUND
        )
    
    data = load_data_for_dataset(dataset_name, base_path=version.path)
    
    DetailedAccessLog.objects.create(
        user=request.user,
        dataset_name=dataset_name,
        version=version,
        access_type='read',
        success=True
    )
    
    return Response({
        "version": version_name,
        "dataset": dataset_name,
        "data": data
    })

@api_view(['GET'])
@authentication_classes([BasicAuthentication])
@permission_classes([IsAuthenticated])
def get_dataset_access_history(request, dataset_name):
    """Obtient l'historique des accès à un dataset"""
    log_access(request)
    
    if not request.user.is_staff:
        return Response(
            {"error": "Only staff members can view access history"},
            status=status.HTTP_403_FORBIDDEN
        )
    
    access_logs = DetailedAccessLog.objects.filter(
        dataset_name=dataset_name
    ).order_by('-timestamp').values(
        'user__username',
        'timestamp',
        'version__name',
        'access_type',
        'success',
        'error_message'
    )
    
    return Response({
        "dataset": dataset_name,
        "access_history": list(access_logs)
    })

@api_view(['GET'])
@authentication_classes([BasicAuthentication])
@permission_classes([IsAuthenticated])
def full_text_search(request):
    log_access(request)
    
    if not hasattr(documents, 'ELASTICSEARCH_AVAILABLE') or not documents.ELASTICSEARCH_AVAILABLE:
        return Response(
            {
                "error": "Elasticsearch n'est pas disponible. Veuillez installer et démarrer Elasticsearch avant d'utiliser cette fonctionnalité.",
                "installation_steps": [
                    "1. Télécharger Elasticsearch depuis https://www.elastic.co/downloads/elasticsearch",
                    "2. Décompresser le fichier",
                    "3. Aller dans le dossier bin",
                    "4. Exécuter elasticsearch.bat",
                    "5. Attendre que le service démarre (port 9200)",
                    "6. Redémarrer le serveur Django"
                ]
            },
            status=status.HTTP_503_SERVICE_UNAVAILABLE
        )
    
    # Récupération des paramètres
    query = request.query_params.get('query', '')
    from_date = request.query_params.get('from_date')
    
    if not query:
        return Response(
            {"error": "Le paramètre 'query' est requis"},
            status=status.HTTP_400_BAD_REQUEST
        )
    
    try:
        # Construction de la requête Elasticsearch
        search = documents.TransactionDocument.search()
        
        # Ajout des conditions de recherche
        must_conditions = []
        
        # Recherche dans tous les champs textuels
        text_query = Q('multi_match', query=query, fields=[
            'transaction_id', 
            'payment_method',
            'country',
            'product_category',
            'user_name',
            'user_id',
            'product_id'
        ])
        must_conditions.append(text_query)
        
        # Filtre par date si spécifié
        if from_date:
            try:
                date_obj = datetime.strptime(from_date, '%Y-%m-%d')
                must_conditions.append(Q('range', timestamp={'gte': date_obj}))
            except ValueError:
                return Response(
                    {"error": "Format de date invalide. Utilisez YYYY-MM-DD"},
                    status=status.HTTP_400_BAD_REQUEST
                )
        
        # Exécution de la recherche
        search = search.query('bool', must=must_conditions)
        response = search.execute()
        
        # Organisation des résultats par dataset
        results_by_dataset = {}
        for hit in response:
            dataset = hit.dataset_source
            if dataset not in results_by_dataset:
                results_by_dataset[dataset] = {
                    'version': hit.version,
                    'items': []
                }
            
            results_by_dataset[dataset]['items'].append({
                'transaction_id': hit.transaction_id,
                'payment_method': hit.payment_method,
                'country': hit.country,
                'product_category': hit.product_category,
                'status': hit.status,
                'amount': hit.amount,
                'customer_rating': hit.customer_rating,
                'timestamp': hit.timestamp,
                'user_id': hit.user_id,
                'user_name': hit.user_name,
                'product_id': hit.product_id
            })
        
        return Response({
            'query': query,
            'from_date': from_date,
            'total_hits': response.hits.total.value,
            'results': results_by_dataset
        })
    
    except Exception as e:
        return Response(
            {"error": f"Erreur lors de la recherche: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )