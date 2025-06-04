**Mohellibi Yanis**  
**Yu Thomas**

# API Documentation

## Authentification

Pour chaque requête, une authentification est requise. Vous pouvez utiliser :

- **Postman** :
    - Auth type = Basic Auth
    - Username = yanis
    - Password = nerf22003018

- **UI Django** : 
    - Mêmes identifiants que ci-dessus

## Endpoints disponibles

### 1. Accès aux droits et log
```
http://127.0.0.1:8000/myapp/access/
```
![Interface d'accès aux droits](image-2.png)


![Sauvegarde des logs de l'API](image-6.png)

![exemple d'acces refuser car je n'ai pas ajouter les droits a cette table](image-8.png)
### 2. Récupération de toutes les données
```
http://127.0.0.1:8000/myapp/retrieve_all/
```
Avec pagination :
```
http://127.0.0.1:8000/myapp/retrieve_all/?page=20
```
![Exemple de pagination](image-3.png)

### 3. Projection par dataset
```
http://127.0.0.1:8000/myapp/retrieve_projection/TRANSACTIONS_COMPLETED/
```

Exemple de datasets disponibles pour les tests :
- TRANSACTIONS_TRANSFORMEES
- TRANSACTIONS_IMPORTANTES_700
- TRANSACTIONS_COMPLETED
- TOP_TROIS_PRODUIT

![Exemple de projection](image.png)

### 4. Filtrage des transactions

Pour voir toutes les transactions :
```
http://127.0.0.1:8000/myapp/transactions/
```

Exemple de filtrage :
```
http://127.0.0.1:8000/myapp/transactions/?payment_method=bank_transfer&country=India
```
![Exemple de filtrage des transactions](image-1.png)

#### Colonnes disponibles pour le filtrage :
- payment_method
- country
- product_category
- status
- amount (avec les suffixes _gt, _lt, _exact)
- customer_rating (avec les suffixes _gt, _lt, _exact)
- timestamp
- user_name
- user_id
- product_id



#### III - Metrics 
http://127.0.0.1:8000/myapp/stats/total_by_user/
![Total par id user](image-5.png)

http://127.0.0.1:8000/myapp/stats/last_5_minutes/


http://127.0.0.1:8000/myapp/stats/top_products/?limit=5
![top_products](image-7.png)

#### IV - Gestion des versions du Data Lake

![Versionning pour chaque acces right possibilité de choisir](image-9.png)

### 1. Liste des ressources disponibles
```
http://127.0.0.1:8000/myapp/data_lake/resources/
```
Cette route permet d'obtenir la liste de toutes les ressources disponibles dans le data lake avec leurs différentes versions.

### 2. Accès à une version spécifique
```
http://127.0.0.1:8000/myapp/data_lake/TRANSACTIONS_COMPLETED/version/V2/
```
Exemples de versions disponibles :
- Pour la version 1 (data_lake) : V1
- Pour la version 2 (data_lake V2) : V2

### 3. Historique des accès

Permet aux administrateurs de consulter l'historique des accès à un dataset spécifique.
Exemple :
```
http://127.0.0.1:8000/myapp/data_lake/TRANSACTIONS_COMPLETED/access_history/
```

#### V - Recherche avancée

### 1. Recherche Full-Text
```
http://127.0.0.1:8000/myapp/search/full-text/?query=transaction_123&from_date=2025-01-01
```

Cette fonctionnalité permet de rechercher des informations dans tous les datasets du data lake. Elle utilise Elasticsearch pour des performances optimales.

Paramètres :
- `query` : Texte à rechercher (requis)
- `from_date` : Date de début de la recherche (optionnel, format: YYYY-MM-DD)

La recherche s'effectue sur tous les champs :
- ID de transaction
- Méthode de paiement
- Pays
- Catégorie de produit
- Nom d'utilisateur
- ID utilisateur
- ID produit
