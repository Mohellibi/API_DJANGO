from django.urls import path, include
from rest_framework.routers import DefaultRouter
from . import views

router = DefaultRouter()
router.register(r'transactions', views.TransactionViewSet, basename='transaction')

urlpatterns = [
    path('', include(router.urls)),
    path('access/', views.access_right_list, name='access-right-list'),
    path('retrieve_all/', views.retrieve_all, name='retrieve-all'),
    path('retrieve_projection/<str:dataset_name>/', views.retrieve_projection, name='retrieve-projection'),
    path('stats/last_5_minutes/', views.last_5_minutes_spent, name='last-5-minutes-spent'),
    path('stats/total_by_user/', views.total_spent_by_user_type, name='total-by-user'),
    path('stats/top_products/', views.top_products, name='top-products'),
    path('data_lake/resources/', views.list_data_lake_resources, name='list-resources'),
    path('data_lake/<str:dataset_name>/version/<str:version_name>/', views.get_dataset_version, name='get-dataset-version'),
    path('data_lake/<str:dataset_name>/access_history/', views.get_dataset_access_history, name='dataset-access-history'),
    path('search/full-text/', views.full_text_search, name='full-text-search'),
]
