import requests
from kafka import KafkaProducer
import json
import logging
import os
import time
from dotenv import load_dotenv
from datetime import datetime
import base64

load_dotenv()
# Logger
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

KAFKA_TOPIC_README = "github-readme"
KAFKA_TOPIC_COMMITS = "github-commits"
KAFKA_TOPIC_CONTRIBUTORS = "github-contributors"
KAFKA_TOPIC_LOGS = "github-import-logs"
KAFKA_TOPIC_FRAMEWORKS = "frameworks-topic"


# Configuration GitHub
owner = "Git-Know"
base_api_url = f"https://api.github.com/orgs/{owner}/repos"
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
headers = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

# Liste des frameworks connus
known_frameworks = [
    "React", "Angular", "Vue", "Next.js", "Nuxt", "Svelte",
    "Spring", "Spring Boot", "Django", "Flask", "Express", "NestJS",
    "Laravel", "Symfony", "Rails", "FastAPI"
]

frameworks_by_contributor = {}

# --- Extraction des README ---
def fetch_and_send_readme(repo_name):
    logger.info(f"📄 README de {repo_name}")
    url = f"https://api.github.com/repos/{owner}/{repo_name}/readme"
    try:
        response = requests.get(url, headers=headers, timeout=30)
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
                logger.info(f"✅ README envoyé et enregistré pour {repo_name}")
            else:
                logger.warning(f"⚠️ Pas de download_url pour {repo_name}")
            if not download_url:
                return "Pas de download_url pour README"
            readme_content = requests.get(download_url).text
            producer.send(KAFKA_TOPIC_README, {"content": readme_content, "repository": repo_name})
            os.makedirs(f"output/{repo_name}", exist_ok=True)
            with open(f"output/{repo_name}/README.md", "w", encoding="utf-8") as f:
                f.write(readme_content)
            return "sent"
        elif response.status_code == 404:
            return "README introuvable (404)"
        else:
            logger.warning(f"⚠️ README introuvable pour {repo_name}")
            return f"Erreur HTTP {response.status_code} sur README"
    except Exception as e:
        logger.error(f"❌ Erreur README {repo_name} : {e}")

        return f"Exception: {str(e)}"

# --- Extraction des commits et frameworks ---
def fetch_repo_file(repo_name, filepath):
    url = f"https://api.github.com/repos/{owner}/{repo_name}/contents/{filepath}"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        file_data = response.json()
        if file_data.get("encoding") == "base64":
            return base64.b64decode(file_data["content"]).decode("utf-8", errors="ignore")
    return None

def detect_frameworks_from_content(content):
    found = []
    for fw in known_frameworks:
        if fw.lower() in content.lower():
            found.append(fw)
    return list(set(found))

def fetch_and_send_commits(repo_name):
    logger.info(f"📥 Commits de {repo_name}")
    url = f"https://api.github.com/repos/{owner}/{repo_name}/commits"
    try:
        response = requests.get(url, headers=headers, timeout=10)
        commits = response.json()
        if not isinstance(commits, list):
            logger.warning(f"⚠️ Pas de liste de commits reçue pour {repo_name}")
            return
            return "La réponse des commits n’est pas une liste"

        os.makedirs(f"output/{repo_name}", exist_ok=True)
        with open(f"output/{repo_name}/commits.txt", "w", encoding="utf-8") as f:
            for commit in commits:
                detail_url = commit.get("url")
                if not detail_url:
                    continue
                detailed_commit = requests.get(detail_url, headers=headers).json()

                # Get contributor
                author = detailed_commit.get("commit", {}).get("author", {}).get("name", "unknown")

                # Send commit to Kafka
                producer.send(KAFKA_TOPIC_COMMITS, detailed_commit)

                # Write summary
                f.write(f"{detailed_commit['sha']} - {detailed_commit['commit']['message']}\n")
                f.write(f"{detailed_commit['commit']['author']['name']} - {detailed_commit['commit']['author']['date']}\n\n")
                logger.info(f"✅ Commits envoyés et enregistrés pour {repo_name}")
                f.write(f"{author} - {detailed_commit['commit']['author']['date']}\n\n")

                # Only check files once per repo, not every commit
            for file in ["Gemfile.lock","requirements-dev.txt","requirements-doc.txt","requirements-tests.txt", "package.json", "requirements.txt", "pom.xml"]:
                content = fetch_repo_file(repo_name, file)
                if content:
                    found_frameworks = detect_frameworks_from_content(content)
                    if found_frameworks:
                        for commit in commits:
                            author = commit.get("commit", {}).get("author", {}).get("name", "unknown")
                            if author not in frameworks_by_contributor:
                                frameworks_by_contributor[author] = set()
                            frameworks_by_contributor[author].update(found_frameworks)

        # Convert sets to lists before saving
        result = {author: list(frameworks) for author, frameworks in frameworks_by_contributor.items()}

        # Save locally
        with open("frameworks_by_contributor.json", "w", encoding="utf-8") as f:
            json.dump(result, f, indent=4)

        # Send to Kafka
        producer.send(KAFKA_TOPIC_FRAMEWORKS, result)

        return "sent"

    except Exception as e:
        logger.error(f"❌ Erreur commits {repo_name} : {e}")

        return f"Exception: {str(e)}"

# --- Extraction des contributeurs ---
def fetch_and_send_contributors(repo_name):
    logger.info(f"👥 Contributeurs de {repo_name}")
    url = f"https://api.github.com/repos/{owner}/{repo_name}/contributors"
    try:
        response = requests.get(url, headers=headers, timeout=10)
        contributors = response.json()
        if not isinstance(contributors, list):
            logger.warning(f"⚠️ Pas de contributeurs trouvés pour {repo_name}")
            return "La réponse des contributeurs n’est pas une liste"

        os.makedirs(f"output/{repo_name}", exist_ok=True)
        with open(f"output/{repo_name}/contributors.txt", "w", encoding="utf-8") as f:
            for c in contributors:
                contrib_info = {
                    "repository": repo_name,
                    "login": c["login"],
                    "contributions": c["contributions"]
                }
                f.write(f"{c['login']} - {c['contributions']} contributions\n")
                producer.send(KAFKA_TOPIC_CONTRIBUTORS, contrib_info)
        logger.info(f"✅ Contributeurs envoyés et enregistrés pour {repo_name}")
    except Exception as e:
        logger.error(f"❌ Erreur contributeurs {repo_name} : {e}")

# --- Extraction de l'arborescence des fichiers ---
def fetch_and_save_file_tree(repo_name):
    logger.info(f"📂 Hiérarchie des fichiers pour {repo_name}")
    tree_url = f"https://api.github.com/repos/{owner}/{repo_name}/git/trees/main?recursive=1"
    try:
        response = requests.get(tree_url, headers=headers, timeout=10)
        if response.status_code != 200:
            logger.warning(f"⚠️ Arborescence non trouvée pour {repo_name} (code {response.status_code})")
            return
        tree_data = response.json()
        if "tree" not in tree_data:
            logger.warning(f"⚠️ Réponse inattendue lors de la récupération de l'arborescence pour {repo_name}")
            return
        paths = [item['path'] for item in tree_data['tree']]
        os.makedirs(f"output/{repo_name}", exist_ok=True)
        output_path = f"output/{repo_name}/structure.txt"
        with open(output_path, "w", encoding="utf-8") as f:
            for path in paths:
                f.write(f"{path}\n")
        logger.info(f"✅ Arborescence enregistrée dans {output_path} ({len(paths)} fichiers)")
        return "sent"
    except Exception as e:
        return f"Exception: {str(e)}"


def save_sync_status(state, message):
    status = {
        "date": datetime.utcnow().isoformat() + "Z",
        "state": state,
        "message": message
    }
    with open("sync_status.json", "w") as f:
        json.dump(status, f, indent=4)

def log_import_cycle(status, repos_info, error_count, duration_seconds):
    log_entry = {
        "timestamp": datetime.utcnow().replace(microsecond=0).isoformat(),
        "status": status,
        "imported_repositories": repos_info,
        "error_count": error_count,
        "duration_seconds": round(duration_seconds, 2)
    }

    os.makedirs("logs", exist_ok=True)
    logs_path = "logs/import_logs.json"

    # Lire les anciens logs s'ils existent, sinon créer une nouvelle liste
    if os.path.exists(logs_path):
        with open(logs_path, "r", encoding="utf-8") as f:
            try:
                existing_logs = json.load(f)
                if not isinstance(existing_logs, list):
                    existing_logs = [existing_logs]
            except json.JSONDecodeError:
                existing_logs = []
    else:
        existing_logs = []

    existing_logs.append(log_entry)

    # Écrire tous les logs
    with open(logs_path, "w", encoding="utf-8") as f:
        json.dump(existing_logs, f, indent=4, ensure_ascii=False)

    try:
        producer.send(KAFKA_TOPIC_LOGS, log_entry)
        producer.flush()
        logger.info("📤 Log d'import envoyé à Kafka avec succès.")
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'envoi du log à Kafka : {e}")

# --- Traitement global de tous les repos ---
def process_all_repos():
    logger.info(f"🔍 Parcours des dépôts de l'organisation : {owner}")
    logger.info(f"🔍 Parcours des dépôts de l'organisation : {owner}")
    processed_repos = []
    error_count = 0
    start_time = time.time()

    try:
        response = requests.get(base_api_url, headers=headers, timeout=10)
        repos = response.json()
        if not isinstance(repos, list):
            logger.error("❌ Réponse inattendue de l'API GitHub.")
            return
            raise Exception("Réponse inattendue de l'API GitHub.")

        for repo in repos:
            repo_name = repo.get("name")
            if not repo_name:
                continue
            logger.info(f"\n{'='*40}\n🎯 Traitement du dépôt : {repo_name}")
            fetch_and_send_readme(repo_name)
            fetch_and_send_commits(repo_name)
            fetch_and_send_contributors(repo_name)
            time.sleep(1)  

            logger.info(f"\n{'='*40}\n🎯 Traitement du dépôt : {repo_name}")
            repo_log = {"name": repo_name}

            # README
            readme_status = fetch_and_send_readme(repo_name)
            repo_log["readme"] = readme_status
            if readme_status != "sent":
                error_count += 1

            # COMMITS
            commits_status = fetch_and_send_commits(repo_name)
            repo_log["commits"] = commits_status
            if commits_status != "sent":
                error_count += 1

            # CONTRIBUTORS
            contrib_status = fetch_and_send_contributors(repo_name)
            repo_log["contributors"] = contrib_status
            if contrib_status != "sent":
                error_count += 1

            processed_repos.append(repo_log)
            time.sleep(1)

        producer.flush()
        logger.info("🚀 Traitement de tous les dépôts terminé")
        duration = time.time() - start_time
        logger.info("✅ Traitement terminé.")
        return processed_repos, error_count, duration

    except Exception as e:
        logger.error(f"❌ Erreur récupération des dépôts : {e}")

# --- Point d'entrée ---
if __name__ == "__main__":
    try:
        repos_info, error_count, duration = process_all_repos()
        save_sync_status("success", "Import GitHub termine sans erreur.")
        log_import_cycle("success", repos_info, error_count, duration)
    except Exception as e:
        save_sync_status("error", f"Erreur lors de l'import : {str(e)}")
        log_import_cycle("error", [], 1, 0.0)


