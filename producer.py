import requests
from kafka import KafkaProducer
import json
import logging

# Configurer le logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration Kafka
try:
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
except Exception as e:
    logger.error(f"Erreur Kafka : {e}")
    raise

# Paramètres GitHub
owner = "Git-Know"
repo = "Base-de-connaissance"
base_url = f"https://api.github.com/repos/{owner}/{repo}"
commits_url = f"{base_url}/commits"
readme_url = f"{base_url}/readme"
contributors_url = f"{base_url}/contributors"
contents_url = f"{base_url}/contents"


def fetch_and_send_commits():
    logger.info("📥 Récupération des commits GitHub")
    try:
        commits = requests.get(commits_url, timeout=10).json()
    except Exception as e:
        logger.error(f"Erreur récupération commits : {e}")
        return

    for commit in commits:
        try:
            detail_url = commit.get("url")
            if not detail_url:
                continue
            detailed_commit = requests.get(detail_url, timeout=10).json()

            producer.send('github-commits', detailed_commit)
            sha = detailed_commit.get('sha', 'unknown')
            logger.info(f"✔ Commit détaillé envoyé : {sha}")

            with open('commits_envoyes.txt', 'w', encoding='utf-8') as f:
                json.dump(detailed_commit, f, indent=2)
                f.write("\n\n")

        except Exception as e:
            logger.error(f"Erreur commit {commit.get('sha', 'unknown')} : {e}")

    # Flush Kafka
    try:
        producer.flush()
        logger.info("✅ Tous les commits ont été envoyés à Kafka")
    except Exception as e:
        logger.error(f"Erreur flush Kafka : {e}")

def fetch_readme():
    logger.info("📄 Récupération du README")
    url = f"https://api.github.com/repos/{owner}/{repo}/readme"
    headers = {"Accept": "application/vnd.github.v3+json"}
    
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            download_url = data.get("download_url")
            if download_url:
                readme_content = requests.get(download_url).text
                with open("README.md", "w", encoding="utf-8") as f:
                    f.write(readme_content)
                logger.info("✔ README téléchargé et enregistré.")
                
                # 🔁 Envoyer dans Kafka (facultatif)
                producer.send('github-readme', {"content": readme_content})
            else:
                logger.warning("README trouvé mais sans download_url.")
        else:
            logger.warning("README introuvable.")
    except Exception as e:
        logger.error(f"Erreur lors de la récupération du README : {e}")


def fetch_contributors():
    logger.info("👥 Récupération des contributeurs")
    try:
        contributors = requests.get(contributors_url, timeout=10).json()
        with open("contributors.txt", "w", encoding="utf-8") as f:
            for c in contributors:
                f.write(f"{c['login']} - {c['contributions']} contributions\n")
        logger.info("✔ Contributeurs enregistrés.")
    except Exception as e:
        logger.error(f"Erreur contributeurs : {e}")



if __name__ == "__main__":
    fetch_and_send_commits()
    fetch_readme()
    fetch_contributors()

