from kafka import KafkaConsumer
import json
from utils import clean_text, extract_entities, save_json
import os

TOPIC_NAME = "github-readme"
BOOTSTRAP_SERVERS = "localhost:9092"
GROUP_ID = "readme-consumer-group"
BASE_OUTPUT_DIR = "output"  # correspond au dossier utilisé par producer.py

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    group_id=GROUP_ID,
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

print("[📡] En attente des messages depuis Kafka...")

for msg in consumer:
    print(f"[📥] Message reçu depuis le topic: {msg.topic}")

    raw_readme = msg.value.get("content", "")
    repo_name = msg.value.get("repository", "unknown-repo")

    if not raw_readme:
        print(f"[⚠️] Aucun contenu dans le message du repo {repo_name}. Ignoré.")
        continue

    cleaned = clean_text(raw_readme)
    result = extract_entities(cleaned)
    result["repository"] = repo_name  # utile pour suivi dans le fichier JSON

    # Nettoyage du nom du repo pour être sûr qu'il est compatible avec le nom de dossier
    safe_repo_name = repo_name.replace("/", "_").replace("\\", "_")

    # Création du dossier si nécessaire
    repo_output_dir = os.path.join(BASE_OUTPUT_DIR, safe_repo_name)
    os.makedirs(repo_output_dir, exist_ok=True)

    # Générer le fichier d'entités dans le dossier du repo
    output_path = os.path.join(repo_output_dir, "entities.json")
    save_json(result, output_path)

    print(f"[✅] Entités extraites sauvegardées dans : {output_path}")
