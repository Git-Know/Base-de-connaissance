from kafka import KafkaConsumer
from pymongo import MongoClient
import json

# Connexion à MongoDB sans user/password
client = MongoClient("mongodb://localhost:27017/")
db = client["maBase"]
collection_summary = db["projects"]
collection_contributors = db["contributors"]
collection_matching = db["matching"]

# Paramètres Kafka
TOPICS = ["github-summary", "contributors-merged", "matching-topic"]
BOOTSTRAP_SERVERS = "localhost:9092"
GROUP_ID = "mongo"

# Création du consommateur Kafka
consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    group_id=GROUP_ID,
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

print("[📡] En attente des messages Kafka...")

for msg in consumer:
    topic = msg.topic
    data = msg.value

    if topic == "github-summary":
        repo_name = data.get("repository")
        if repo_name:
            # Vérifie s’il existe déjà un projet avec ce nom
            if collection_summary.count_documents({"repository": repo_name}) == 0:
                try:
                    collection_summary.insert_one(data)
                    print(f"[✅] Projet inséré (repo: {repo_name})")
                except Exception as e:
                    print(f"[❌] Erreur insertion github-summary: {e}")
            else:
                print(f"[ℹ️] Projet déjà existant ignoré : {repo_name}")
        else:
            print("[⚠️] Donnée invalide (pas de repository)")

    elif topic == "contributors-merged":
        try:
            collection_contributors.insert_one(data)
            print(f"[✅] Contributeur inséré (repo: {data.get('repository', 'inconnu')})")
        except Exception as e:
            print(f"[❌] Erreur insertion contributors-merged: {e}")
    elif topic == "matching-topic":
        developer = data.get("developer")
        repository = data.get("repository")
        if developer and repository:
            exists = collection_matching.find_one({
                "developer": developer,
                "repository": repository
            })
            if not exists:
                collection_matching.insert_one(data)
                print(f"[✅] Appariement inséré : {developer} -> {repository}")
            else:
                print(f"[⚠️] Appariement déjà existant : {developer} -> {repository}")
        else:
            print("[⚠️] Clé 'developer' ou 'repository' manquante dans matching-topic")
    else:
        print(f"[⚠️] Topic non géré : {topic}")
