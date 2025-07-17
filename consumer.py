from kafka import KafkaConsumer, KafkaProducer
import json
from utils import clean_text, extract_entities, generate_summary_nlp

# Configuration Kafka
TOPIC_NAME = "github-readme"
OUTPUT_TOPIC = "github-summary"
BOOTSTRAP_SERVERS = "localhost:9092"
GROUP_ID = "readme-consumer-group"

# Consumer Kafka
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    group_id=GROUP_ID,
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

# Producer Kafka
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
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

    # Envoi du résultat dans un topic Kafka
    producer.send(OUTPUT_TOPIC, entities)
    producer.flush()
    print(f"[✅] Résumé + entités envoyés dans le topic Kafka : {OUTPUT_TOPIC}")
