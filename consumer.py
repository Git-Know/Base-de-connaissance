from kafka import KafkaConsumer
import json
from utils import clean_text, extract_entities, save_json
import os

TOPIC_NAME = "github-readme"
BOOTSTRAP_SERVERS = "localhost:9092"
GROUP_ID = "readme-consumer-group"
OUTPUT_DIR = "entities_outputs"

os.makedirs(OUTPUT_DIR, exist_ok=True)

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
    result["repository"] = repo_name  # facultatif : pour rappel dans le JSON

    # Nettoyer le nom de repo (au cas où il contient des caractères interdits)
    safe_repo_name = repo_name.replace("/", "_").replace("\\", "_")

    output_path = os.path.join(OUTPUT_DIR, f"{safe_repo_name}_entities.json")
    save_json(result, output_path)
