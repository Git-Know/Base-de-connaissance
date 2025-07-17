import requests
from kafka import KafkaProducer
import json
import logging
import os
import time
from dotenv import load_dotenv


load_dotenv()
# Logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka
try:
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
except Exception as e:
    logger.error(f"Erreur Kafka : {e}")
    raise

KAFKA_TOPIC_README = "github-readme"
KAFKA_TOPIC_COMMITS = "github-commits"
KAFKA_TOPIC_CONTRIBUTORS = "github-contributors"

# GitHub
owner = "Git-Know"
base_api_url = f"https://api.github.com/orgs/{owner}/repos"

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")


headers = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

def fetch_and_send_readme(repo_name):
    logger.info(f":page_imprimée: README de {repo_name}")
    url = f"https://api.github.com/repos/{owner}/{repo_name}/readme"
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            readme_data = response.json()
            download_url = readme_data.get("download_url")
            if download_url:
                readme_content = requests.get(download_url).text
                # Envoi dans Kafka
                producer.send(KAFKA_TOPIC_README, {"content": readme_content, "repository": repo_name})

                # Sauvegarde locale
                os.makedirs(f"output/{repo_name}", exist_ok=True)
                with open(f"output/{repo_name}/README.md", "w", encoding="utf-8") as f:
                    f.write(readme_content)

                logger.info(f":coche_blanche: README envoyé et enregistré pour {repo_name}")
            else:
                logger.warning(f":danger: Pas de download_url pour {repo_name}")
        else:
            logger.warning(f":danger: README introuvable pour {repo_name}")
    except Exception as e:
        logger.error(f":x: Erreur README {repo_name} : {e}")
def fetch_and_send_commits(repo_name):
    logger.info(f":inbox: Commits de {repo_name}")
    url = f"https://api.github.com/repos/{owner}/{repo_name}/commits"
    try:
        response = requests.get(url, headers=headers, timeout=10)
        commits = response.json()

        if not isinstance(commits, list):
            logger.warning(f":danger: Pas de liste de commits reçue pour {repo_name}")
            return
        os.makedirs(f"output/{repo_name}", exist_ok=True)
        with open(f"output/{repo_name}/commits.txt", "w", encoding="utf-8") as f:
            for commit in commits:
                detail_url = commit.get("url")
                if not detail_url:
                    continue
                detailed_commit = requests.get(detail_url, headers=headers).json()

                # Envoi Kafka
                producer.send(KAFKA_TOPIC_COMMITS, detailed_commit)
                # Sauvegarde fichier
                f.write(f"{detailed_commit['sha']} - {detailed_commit['commit']['message']}\n")
                f.write(f"{detailed_commit['commit']['author']['name']} - {detailed_commit['commit']['author']['date']}\n\n")
        logger.info(f":coche_blanche: Commits envoyés et enregistrés pour {repo_name}")
    except Exception as e:
        logger.error(f":x: Erreur commits {repo_name} : {e}")
def fetch_and_send_contributors(repo_name):
    logger.info(f":bustes_silhouettes: Contributeurs de {repo_name}")
    url = f"https://api.github.com/repos/{owner}/{repo_name}/contributors"
    try:
        response = requests.get(url, headers=headers, timeout=10)
        contributors = response.json()

        if not isinstance(contributors, list):
            logger.warning(f":danger: Pas de contributeurs trouvés pour {repo_name}")
            return
        os.makedirs(f"output/{repo_name}", exist_ok=True)
        with open(f"output/{repo_name}/contributors.txt", "w", encoding="utf-8") as f:
            for c in contributors:
                contrib_info = {
                    "repository": repo_name,
                    "login": c["login"],
                    "contributions": c["contributions"]
                }
                f.write(f"{c['login']} - {c['contributions']} contributions\n")

                # Envoi Kafka
                producer.send(KAFKA_TOPIC_CONTRIBUTORS, contrib_info)
        logger.info(f":coche_blanche: Contributeurs envoyés et enregistrés pour {repo_name}")
    except Exception as e:
        logger.error(f":x: Erreur contributeurs {repo_name} : {e}")
def process_all_repos():
    logger.info(f":loupe_gauche: Parcours des dépôts de l'organisation : {owner}")
    try:
        response = requests.get(base_api_url, headers=headers, timeout=10)
        repos = response.json()
        if not isinstance(repos, list):
            logger.error(":x: Réponse inattendue de l'API GitHub.")
            return
        for repo in repos:
            repo_name = repo.get("name")
            if not repo_name:
                continue
            logger.info(f"\n{'='*40}\n:fléchette: Traitement du dépôt : {repo_name}")
            fetch_and_send_readme(repo_name)
            fetch_and_send_commits(repo_name)
            fetch_and_send_contributors(repo_name)
            time.sleep(1)  

        producer.flush()
        logger.info(":fusée: Traitement de tous les dépôts terminé")
    except Exception as e:
        logger.error(f":x: Erreur récupération des dépôts : {e}")
if __name__ == "__main__":
    process_all_repos()
