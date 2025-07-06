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

KAFKA_TOPIC = "github-readme"
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
        response = requests.get(commits_url, timeout=10)
        commits = response.json()

        if not isinstance(commits, list):
            logger.error(f"❌ Réponse inattendue de l'API GitHub : {commits}")
            return

    except Exception as e:
        logger.error(f"❌ Erreur récupération commits : {e}")
        return

    for commit in commits:
        try:
            if not isinstance(commit, dict):
                logger.warning(f"⚠️ Commit ignoré (non dict): {commit}")
                continue

            detail_url = commit.get("url")
            if not detail_url:
                continue

            detailed_commit = requests.get(detail_url, timeout=10).json()

            # Envoi dans Kafka
            producer.send('github-commits', detailed_commit)
            sha = detailed_commit.get('sha', 'unknown')
            logger.info(f"✔ Commit détaillé envoyé : {sha}")

            # Ajout dans le fichier (en mode append)
            with open('commits_envoyes.txt', 'a', encoding='utf-8') as f:
                json.dump(detailed_commit, f, indent=2)
                f.write("\n\n")

        except Exception as e:
            sha_fallback = commit.get('sha', 'unknown') if isinstance(commit, dict) else 'inconnu'
            logger.error(f"❌ Erreur commit {sha_fallback} : {e}")

    # Flush Kafka à la fin
    try:
        producer.flush()
        logger.info("✅ Tous les commits ont été envoyés à Kafka")
    except Exception as e:
        logger.error(f"❌ Erreur lors du flush Kafka : {e}")


def fetch_readme():
    logger.info(f"🔍 Récupération des dépôts de l'organisation : {owner}")
    repos_url = f"https://api.github.com/orgs/{owner}/repos"
    headers = {"Accept": "application/vnd.github.v3+json"}
    
    try:
        repos = requests.get(repos_url, headers=headers, timeout=10).json()

        if not isinstance(repos, list):
            logger.error("❌ Erreur : réponse inattendue de l'API GitHub.")
            return

        for repo in repos:
            repo_name = repo.get("name")
            if not repo_name:
                continue

            logger.info(f"📄 Récupération du README pour le repo : {repo_name}")
            readme_url = f"https://api.github.com/repos/{owner}/{repo_name}/readme"

            try:
                resp = requests.get(readme_url, headers=headers, timeout=10)
                if resp.status_code == 200:
                    readme_data = resp.json()
                    download_url = readme_data.get("download_url")
                    if download_url:
                        readme_content = requests.get(download_url).text

                        # Envoi dans Kafka
                        producer.send(KAFKA_TOPIC, {
                            "content": readme_content,
                            "repository": repo_name
                        })
                        logger.info(f"✅ README envoyé dans Kafka (repo: {repo_name})")
                    else:
                        logger.warning(f"⚠️ Pas de download_url pour {repo_name}")
                else:
                    logger.warning(f"⚠️ README non trouvé pour {repo_name} (code {resp.status_code})")

            except Exception as e:
                logger.error(f"❌ Erreur récupération README pour {repo_name} : {e}")

        producer.flush()

    except Exception as e:
        logger.error(f"❌ Erreur lors de la récupération des dépôts de l'organisation : {e}")


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

