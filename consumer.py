from kafka import KafkaConsumer
import json
from utils import clean_text, extract_entities, save_json, generate_summary_nlp
import os

TOPIC_NAME = "github-readme"
BOOTSTRAP_SERVERS = "localhost:9092"
GROUP_ID = "readme-consumer-group"
BASE_OUTPUT_DIR = "output"

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

    # Nettoyage et extraction
    cleaned = clean_text(raw_readme)
    entities = extract_entities(cleaned)
    summary = generate_summary_nlp(cleaned, project_name=repo_name)

    entities["repository"] = repo_name
    entities["summary"] = summary

    # Sauvegarde JSON
    safe_repo_name = repo_name.replace("/", "_").replace("\\", "_")
    repo_output_dir = os.path.join(BASE_OUTPUT_DIR, safe_repo_name)
    os.makedirs(repo_output_dir, exist_ok=True)
    output_path = os.path.join(repo_output_dir, "entities.json")
    save_json(entities, output_path)

    print(f"[✅] Résumé + entités sauvegardés dans : {output_path}")
