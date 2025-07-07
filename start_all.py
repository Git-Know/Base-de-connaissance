import subprocess
import time
import os
import sys
from kafka import KafkaProducer
import argparse

DOCKER_COMPOSE_CMD = ["docker-compose", "up", "-d"]
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TIMEOUT = 60  # secondes

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

    if not os.path.exists(".env"):
        print("⚠️  Fichier .env introuvable. Veuillez le créer pour fournir le GITHUB_TOKEN.")

    print(f"🚀 Lancement du script \033[1m{script_name}\033[0m ...")
    try:
        result = subprocess.run([sys.executable, script_name], capture_output=True, text=True)
        print(result.stdout)
        if result.returncode != 0:
            print(f"❌ Erreur lors de l'exécution de {script_name} :")
            print(result.stderr)
            sys.exit(result.returncode)
    except Exception as e:
        print(f"❌ Exception lors de l'exécution de {script_name} : {e}")
        sys.exit(1)

if __name__ == "__main__":
    args = get_args()
    start_docker()
    wait_for_kafka()
    run_producer_script(args.script)
