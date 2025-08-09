import subprocess
import time
from kafka import KafkaProducer
import sys

DOCKER_COMPOSE_CMD = ["docker","compose", "up", "--build","-d"]
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
PRODUCER_SCRIPT = "producer.py"
CONSUMER_SCRIPT = "consumer.py"
KAFKA_TIMEOUT = 60  
NEO4J_CONSUMER_SCRIPT = "neo4j_kafka_consumer.py"

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

def run_spark_job():
    print("🔥 Lancement du job Spark Datacleaning.py dans le conteneur Spark...")
    try:
        subprocess.run( [
    "docker", "exec",
    "--user", "root",           # ← run as root so Hadoop/Spark can resolve the user
    "spark-master",
    "/opt/bitnami/spark/bin/spark-submit",
    "--master", "local[*]",
    "--conf", "spark.jars.ivy=/tmp/.ivy2",
    "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    "/opt/bitnami/spark/work/Datacleaning.py"
]    , check=True)
    except subprocess.CalledProcessError as e:
        print(f"❌ Erreur lors de l'exécution du job Spark : {e}")
        sys.exit(1)

def run_producer_script():
    print("🚀 Lancement du script producer.py...")
    try:
        subprocess.run([sys.executable, PRODUCER_SCRIPT], check=True)
    except subprocess.CalledProcessError as e:
        print(f"❌ Erreur lors de l'exécution de {PRODUCER_SCRIPT} : {e}")
        sys.exit(1)

def run_consumer_script():
    print("🚀 Lancement du script consumer.py...")
    try:
        subprocess.run([sys.executable, CONSUMER_SCRIPT], check=True)
    except subprocess.CalledProcessError as e:
        print(f"❌ Erreur lors de l'exécution de {CONSUMER_SCRIPT} : {e}")
        sys.exit(1)
def run_neo4j_consumer_script():
    print("🚀 Lancement du script ne4j kafa consumer.py...")
    try:
        subprocess.run([sys.executable, NEO4J_CONSUMER_SCRIPT], check=True)
    except subprocess.CalledProcessError as e:
        print(f"❌ Erreur lors de l'exécution de {NEO4J_CONSUMER_SCRIPT} : {e}")
        sys.exit(1)
    
if __name__ == "__main__":
    start_docker()
    wait_for_kafka()
    run_producer_script()

    run_spark_job()
    run_neo4j_consumer_script()
    run_consumer_script()