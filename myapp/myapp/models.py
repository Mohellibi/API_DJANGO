from django.db import models
from django.contrib.auth.models import User
from rest_framework import serializers

class DataLakeVersion(models.Model):
    name = models.CharField(max_length=50)  # ex: "V1", "V2"
    path = models.CharField(max_length=255)  # ex: "data_lake" ou "data_lake V2"
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.name} ({self.path})"

class AccessRight(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    dataset_name = models.CharField(max_length=100)
    allowed_versions = models.ManyToManyField(DataLakeVersion, blank=True)
    can_access_all_versions = models.BooleanField(default=False)
    
    class Meta:
        unique_together = ('user', 'dataset_name')

    def __str__(self):
        return f"{self.user.username} - {self.dataset_name}"

class AccessRightSerializer(serializers.ModelSerializer):
    allowed_versions = serializers.StringRelatedField(many=True)
    
    class Meta:
        model = AccessRight
        fields = ['user', 'dataset_name', 'allowed_versions', 'can_access_all_versions']

class AccessLog(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    timestamp = models.DateTimeField(auto_now_add=True)
    request_path = models.CharField(max_length=255)
    request_method = models.CharField(max_length=10)
    request_body = models.TextField(blank=True)

class DetailedAccessLog(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    timestamp = models.DateTimeField(auto_now_add=True)
    dataset_name = models.CharField(max_length=100)
    version = models.ForeignKey(DataLakeVersion, on_delete=models.SET_NULL, null=True)
    access_type = models.CharField(max_length=20)  # 'read', 'list', 'version_check'
    success = models.BooleanField(default=True)
    error_message = models.TextField(blank=True)

    def __str__(self):
        return f"{self.user.username} - {self.dataset_name} ({self.version})"

class Transaction(models.Model):
    payment_method = models.CharField(max_length=50)
    country = models.CharField(max_length=100)
    product_category = models.CharField(max_length=100)
    status = models.CharField(max_length=50)
    amount = models.DecimalField(max_digits=10, decimal_places=2)
    customer_rating = models.IntegerField(null=True)
    timestamp = models.DateTimeField()
    user_id = models.CharField(max_length=50, null=True, blank=True)
    user_name = models.CharField(max_length=100, null=True, blank=True)
    product_id = models.CharField(max_length=50, null=True, blank=True)

    def __str__(self):
        return f"{self.user_name or 'Unknown'} - {self.product_category} - {self.amount} ({self.status})"

class TransactionSerializer(serializers.ModelSerializer):
    class Meta:
        model = Transaction
        fields = '__all__'

