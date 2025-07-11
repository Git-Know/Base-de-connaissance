import subprocess
import time
import os
import sys
from kafka import KafkaProducer
import argparse

DOCKER_COMPOSE_CMD = ["docker-compose", "up", "-d"]
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TIMEOUT = 60  # secondes
from kafka import KafkaProducer,KafkaConsumer



PRODUCER_SCRIPT = "producer.py"
CONSUMER_SCRIPT = "consumer.py"

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--script", default="producer.py", help="Nom du script à exécuter (par défaut: producer.py)")
    return parser.parse_args()


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
        except Exception as e:
            if time.time() - start_time > timeout:
                print(f"⛔ Timeout : Kafka ne répond pas. Dernière erreur : {e}")
                sys.exit(1)
            time.sleep(2)

def run_producer_script(script_name):
    if not os.path.exists(script_name):
        print(f"❌ Le script {script_name} n'existe pas.")
        sys.exit(1)

def run_consumer_script():
    print("🚀 Lancement du script consumer.py...")
    try:
        # Utilise "py" sous Windows, sinon "python" selon ton système
        subprocess.run(["py", CONSUMER_SCRIPT], check=True)
    except subprocess.CalledProcessError as e:
        print(f"❌ Erreur lors de l'exécution de {CONSUMER_SCRIPT} : {e}")
        sys.exit(1)

if __name__ == "__main__":
    args = get_args()
    start_docker()
    wait_for_kafka()
    run_producer_script()
    run_consumer_script()



