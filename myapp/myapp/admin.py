from django.contrib import admin
from .models import AccessRight, AccessLog, Transaction, DataLakeVersion, DetailedAccessLog

@admin.register(DataLakeVersion)
class DataLakeVersionAdmin(admin.ModelAdmin):
    list_display = ('name', 'path', 'is_active', 'created_at')
    list_filter = ('is_active',)
    search_fields = ('name', 'path')
    ordering = ('name',)

@admin.register(AccessRight)
class AccessRightAdmin(admin.ModelAdmin):
    list_display = ('user', 'dataset_name', 'can_access_all_versions')
    list_filter = ('dataset_name', 'user', 'can_access_all_versions', 'allowed_versions')
    search_fields = ('user__username', 'dataset_name')
    ordering = ('dataset_name', 'user__username')
    filter_horizontal = ('allowed_versions',)

@admin.register(AccessLog)
class AccessLogAdmin(admin.ModelAdmin):
    list_display = ('user', 'timestamp', 'request_method', 'request_path')
    list_filter = ('request_method', 'user')
    search_fields = ('user__username', 'request_path')
    ordering = ('-timestamp',)
