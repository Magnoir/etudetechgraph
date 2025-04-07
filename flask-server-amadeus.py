from flask import Flask, jsonify
from azure.cosmos import CosmosClient
from dotenv import load_dotenv

load_dotenv()
app = Flask(__name__)

# Informations de connexion
import os

url = os.getenv("URL_AZURE_COSMOS")
key = os.getenv("KEY_AZURE_COSMOS")

if not url or not key:
    raise ValueError("Environment variables URL_AZURE_COSMOS and KEY_AZURE_COSMOS must be set.")
client = CosmosClient(url, credential=key)

# Liste les bases et conteneurs pour vérification
databases = list(client.list_databases())
print("Bases disponibles :", databases)

for db in databases:
    db_client = client.get_database_client(db['id'])
    containers = list(db_client.list_containers())
    print(f"Conteneurs dans la base {db['id']} :", containers)

# Exemple : accéder à la base "test" et au conteneur "test-d"
database = client.get_database_client("test")
container = database.get_container_client("test-d")

@app.route('/data', methods=['GET'])
def get_data():
    query = "SELECT * FROM c"
    items = list(container.query_items(query=query, enable_cross_partition_query=True))
    return jsonify(items)

if __name__ == '__main__':
    app.run(debug=False)
