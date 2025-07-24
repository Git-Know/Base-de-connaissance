import subprocess
import time
import os
import sys
import argparse
from kafka import KafkaProducer

# Commande Docker

DOCKER_COMPOSE_CMD = ["docker-compose", "up", "-d"]
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TIMEOUT = 60  # secondes

PRODUCER_SCRIPT = "producer.py"
CONSUMER_SCRIPT = "consumer.py"

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--script", default=PRODUCER_SCRIPT, help="Nom du script producteur à exécuter")
    return parser.parse_args()

def start_docker():
    print("🛠️  Démarrage des services Docker...")
    try:
        subprocess.run(DOCKER_COMPOSE_CMD, check=True)
        print("✅ Docker Compose lancé avec succès.")
    except subprocess.CalledProcessError as e:
        print(f"❌ Erreur lors du démarrage Docker Compose : {e}")
        sys.exit(1)
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
        except Exception as e:
            if time.time() - start_time > timeout:
                print(f"⛔ Timeout : Kafka ne répond pas. Dernière erreur : {e}")
                sys.exit(1)
            time.sleep(2)

def run_script(script_name):
    if not os.path.exists(script_name):
        print(f"❌ Le script {script_name} n'existe pas.")

def run_producer_script():
    print("🚀 Lancement du script producer.py...")
    try:
        subprocess.run(["py", PRODUCER_SCRIPT], check=True)
    except subprocess.CalledProcessError as e:
        print(f"❌ Erreur lors de l'exécution de {PRODUCER_SCRIPT} : {e}")
        sys.exit(1)

    print(f"🚀 Lancement du script {script_name}...")
    try:
        command = ["py", script_name] if os.name == "nt" else ["python3", script_name]
        subprocess.run(command, check=True)
        print(f"✅ {script_name} exécuté avec succès.")
    except subprocess.CalledProcessError as e:
        print(f"❌ Erreur lors de l'exécution de {script_name} : {e}")
        sys.exit(1)

if __name__ == "__main__":
    args = get_args()
    # start_docker()
    start_docker()
    wait_for_kafka()
    run_script(args.script)          # Lancement du producteur (par défaut: producer.py)
    run_script(CONSUMER_SCRIPT)      # Lancement du consumer

    run_producer_script()
    run_consumer_script()