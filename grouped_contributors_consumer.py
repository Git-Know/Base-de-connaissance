from kafka import KafkaConsumer, KafkaProducer
import json
from collections import defaultdict

# Initialiser le consommateur Kafka
consumer = KafkaConsumer(
    'contributor_skill_summary',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='contributor-grouper-test',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Initialiser le producteur Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Dictionnaire pour grouper les contributeurs
grouped_contributors = defaultdict(lambda: {
    "languages": set(),
    "total_contributions": 0,
    "last_commit": None
})

print("⏳ Consommation en cours... Appuyez sur Ctrl+C pour arrêter.")
consumed_count = 0

try:
    for message in consumer:
        contributor = message.value

        # Affichage pour debug
        print("📥 Message reçu :", contributor)

        try:
            name = contributor["author_name"]
            lang = contributor["dominant_language"]
            contribs = contributor["total_contributions"]
            last_commit = contributor["last_commit"]
        except KeyError as e:
            print(f"⚠️ Champ manquant dans le message : {e}")
            continue

        data = grouped_contributors[name]
        data["languages"].add(lang)
        data["total_contributions"] += contribs

        if data["last_commit"] is None or last_commit > data["last_commit"]:
            data["last_commit"] = last_commit

        consumed_count += 1

except KeyboardInterrupt:
    print("\n🛑 Arrêt manuel. Sauvegarde et envoi des données...")

# Vérification : a-t-on consommé des données ?
if consumed_count == 0:
    print("⚠️ Aucun message consommé. Vérifiez le topic Kafka ou les producteurs.")
else:
    final_result = []
    for name, data in grouped_contributors.items():
        contributor_data = {
            "author_name": name,
            "languages": list(data["languages"]),
            "total_contributions": data["total_contributions"],
            "last_commit": data["last_commit"],
        }
        final_result.append(contributor_data)

        # Envoi à Kafka topic 'contributors-technologies'
        producer.send('contributors-technologies', contributor_data)

    producer.flush()

    # Sauvegarde locale
    with open("grouped_contributors.json", "w", encoding="utf-8") as f:
        json.dump(final_result, f, indent=2, ensure_ascii=False)

    print(f"✅ {len(final_result)} contributeurs sauvegardés dans grouped_contributors.json")
    print("🚀 Données envoyées au topic Kafka 'contributors-technologies'")
