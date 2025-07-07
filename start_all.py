import subprocess
import time
from kafka import KafkaProducer,KafkaConsumer
import sys
import json


DOCKER_COMPOSE_CMD = ["docker-compose", "up", "-d"]
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
PRODUCER_SCRIPT = "producer.py"
CONSUMER_SCRIPT = "consumer.py"
KAFKA_TIMEOUT = 60  


def start_docker():
    print("🛠️  Démarrage des services Docker...")
    try:
        subprocess.run(DOCKER_COMPOSE_CMD, check=True)
        print("✅ Docker Compose lancé avec succès.")
    except subprocess.CalledProcessError as e:
        print(f"❌ Erreur lors du démarrage Docker Compose : {e}")
        sys.exit(1)


def wait_for_kafka(timeout=KAFKA_TIMEOUT):
    print(f"⏳ Attente de Kafka sur {KAFKA_BOOTSTRAP_SERVERS} (max {timeout}s)...")
    start_time = time.time()
    while True:
        try:
            producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
            producer.close()
            print("✅ Kafka est prêt.")
            break
        except Exception:
            if time.time() - start_time > timeout:
                print("⛔ Timeout : Kafka ne répond pas.")
                sys.exit(1)
            time.sleep(2)


def run_producer_script():
    print("🚀 Lancement du script producer.py...")
    try:
        subprocess.run(["py", PRODUCER_SCRIPT], check=True)
    except subprocess.CalledProcessError as e:
        print(f"❌ Erreur lors de l'exécution de {PRODUCER_SCRIPT} : {e}")
        sys.exit(1)

def run_consumer_script():
    print("🚀 Lancement du script consumer.py...")
    try:
        # Utilise "py" sous Windows, sinon "python" selon ton système
        subprocess.run(["py", CONSUMER_SCRIPT], check=True)
    except subprocess.CalledProcessError as e:
        print(f"❌ Erreur lors de l'exécution de {CONSUMER_SCRIPT} : {e}")
        sys.exit(1)

def read_github_readme_topic():
    TOPIC_NAME = "github-readme"
    BOOTSTRAP_SERVERS = "localhost:9092"
    GROUP_ID = "readme-viewer"

    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        group_id=GROUP_ID,
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )

    print("📡 Lecture des messages du topic 'github-readme'...")

    try:
        for i, message in enumerate(consumer, 1):
            value = message.value
            repo_name = value.get("repository", "inconnu")
            print(f"\n📝 Message {i} reçu (repo: {repo_name}):")
            print(json.dumps(value, indent=2, ensure_ascii=False))
            print("-" * 60)
    except KeyboardInterrupt:
        print("\n❌ Arrêt manuel par l'utilisateur.")
    finally:
        consumer.close()

if __name__ == "__main__":
    start_docker()
    wait_for_kafka()
    run_producer_script()
    #read_github_readme_topic()
    run_consumer_script()
