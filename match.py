from kafka import KafkaConsumer, KafkaProducer
import json
from utils import match_developer_to_project

consumer = KafkaConsumer(
    'contributors-merged',
    'github-summary',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='matching-group'
)

producer_matching = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

developers = []
projects = []
processed_projects = set()

print("En attente des messages sur les topics 'contributors-merged' et 'github-summary'...")

for msg in consumer:
    topic = msg.topic
    data = msg.value

    if topic == 'contributors-merged':
        if "author" in data:
            developers.append(data)
            print(f"Chargé développeur: {data['author']}")

    elif topic == 'github-summary':
        repo_name = data.get('repository', '').lower()
        if not repo_name:
            print("⚠️ Projet sans repository:", data)
            continue

        if repo_name in processed_projects:
            print(f"⏭️ Projet déjà traité : {repo_name}")
            continue

        print(f"\nNouveau projet: {repo_name}")
        processed_projects.add(repo_name)

        for dev in developers:
            match_res = match_developer_to_project(dev, data)
            producer_matching.send('matching-topic', match_res)

        producer_matching.flush()
        print(f"Résultats de matching envoyés pour le projet '{repo_name}'.")

print("Matching terminé.")
