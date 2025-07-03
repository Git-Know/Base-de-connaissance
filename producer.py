import requests
from kafka import KafkaProducer
import json
import logging
import time

# Configurer le logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration Kafka
try:
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',  # modifie si besoin
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
except Exception as e:
    logger.error(f"Erreur lors de la connexion à Kafka : {e}")
    raise

# Paramètres GitHub
owner = "Git-Know"
repo = "Base-de-connaissance"
commits_url = f"https://api.github.com/repos/{owner}/{repo}/commits"

def fetch_and_send_commits():
    logger.info("Début de la récupération des commits GitHub")
    try:
        response = requests.get(commits_url, timeout=10)
        response.raise_for_status()  # Lève une erreur HTTP si code != 200
    except requests.exceptions.RequestException as e:
        logger.error(f"Erreur lors de la requête API GitHub : {e}")
        return

    try:
        commits = response.json()
    except json.JSONDecodeError as e:
        logger.error(f"Erreur lors du décodage JSON : {e}")
        return

    for commit in commits:
        try:
            producer.send('github-commits', commit)
            sha = commit.get('sha', 'unknown')
            logger.info(f"Commit envoyé: {sha}")
            # Enregistrer dans un fichier texte
            with open('commits_envoyes.txt', 'w', encoding='utf-8') as f:
                f.write(f"{commit}\n")
        except Exception as e:
            logger.error(f"Erreur lors de l'envoi du commit à Kafka : {e}")

    try:
        producer.flush()
        logger.info("Tous les commits ont été envoyés dans Kafka")
    except Exception as e:
        logger.error(f"Erreur lors du flush Kafka : {e}")

if __name__ == "__main__":
    fetch_and_send_commits()
