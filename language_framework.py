from kafka import KafkaConsumer, KafkaProducer
import json
import time

TOPICS = ['contributors-technologies', 'frameworks-topic']
BOOTSTRAP_SERVERS = 'localhost:9092'
MERGED_TOPIC = 'contributors-merged'

# Initialiser le producteur Kafka
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Initialiser le consommateur Kafka
consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='merge',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

contributors_dict = {}
frameworks_dict = {}

print("🚀 Démarrage de la consommation...")

for message in consumer:
    topic = message.topic
    data = message.value

    if topic == 'contributors-technologies':
        name = data["author_name"].strip().lower()
        contributors_dict[name] = {
            "languages": data["languages"],
            "contributions": data["total_contributions"],
            "last_commit": data["last_commit"]
        }
        print(f"📥 [Technologies] {name}")

    elif topic == 'frameworks-topic':
        for key, frameworks in data.items():
            name = key.strip().lower()
            frameworks_dict[name] = frameworks
            print(f"📥 [Frameworks] {name}")

    # Fusionner les données s'il y a correspondance
    common_names = set(contributors_dict) & set(frameworks_dict)
    for name in common_names:
        merged = {
            "author": name,
            "languages": contributors_dict[name]["languages"],
            "frameworks": frameworks_dict[name],
            "contributions": contributors_dict[name]["contributions"],
            "last_commit": contributors_dict[name]["last_commit"]
        }

        # Envoi vers Kafka
        producer.send(MERGED_TOPIC, value=merged)
        producer.flush()

        print(f"✅ Fusion envoyée vers '{MERGED_TOPIC}' pour {name}")
        
        # Supprimer pour éviter les doublons
        del contributors_dict[name]
        del frameworks_dict[name]
