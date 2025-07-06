import requests
from kafka import KafkaProducer
import json
import logging
import time
from datetime import datetime
import base64
import os

# Configuration du logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('github_processing.log')
    ]
)
logger = logging.getLogger(__name__)

# Configuration GitHub
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')  # À récupérer depuis les variables d'environnement
HEADERS = {
    'Authorization': f'token {GITHUB_TOKEN}',
    'Accept': 'application/vnd.github.v3+json'
}

# Configuration de l'organisation et des dépôts
ORGANIZATION = "Git-Know"
REPOSITORIES = [
    "Base-de-connaissance",
    "metasfresh", 
    "evershop",
    "maybe",
    "shop-e-commerce"
]

def setup_kafka():
    try:
        return KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5
        )
    except Exception as e:
        logger.error(f"Erreur Kafka: {e}")
        return None

def get_repo_data(repo_name):
    """Récupère toutes les données d'un dépôt"""
    base_url = f"https://api.github.com/repos/{ORGANIZATION}/{repo_name}"
    
    # 1. Infos de base du dépôt
    repo_info = requests.get(base_url, headers=HEADERS).json()
    if 'message' in repo_info and repo_info['message'] == 'Not Found':
        return None

    # 2. Récupération du README
    readme = None
    readme_res = requests.get(f"{base_url}/readme", headers=HEADERS)
    if readme_res.status_code == 200:
        readme_data = readme_res.json()
        try:
            readme = {
                'content': base64.b64decode(readme_data['content']).decode('utf-8'),
                'encoding': readme_data.get('encoding', 'base64')
            }
        except Exception as e:
            logger.error(f"Erreur décodage README: {e}")

    # 3. Récupération des contributeurs
    contributors = []
    contrib_res = requests.get(f"{base_url}/contributors", headers=HEADERS)
    if contrib_res.status_code == 200:
        for contributor in contrib_res.json():
            user_data = requests.get(contributor['url'], headers=HEADERS).json()
            contributors.append({
                'login': contributor['login'],
                'contributions': contributor['contributions'],
                'name': user_data.get('name'),
                'email': user_data.get('email'),
                'company': user_data.get('company')
            })
            time.sleep(0.5)  # Respect du rate limit

    # 4. Récupération des commits (30 derniers)
    commits = []
    commits_res = requests.get(f"{base_url}/commits", headers=HEADERS)
    if commits_res.status_code == 200:
        for commit in commits_res.json():
            commit_detail = requests.get(commit['url'], headers=HEADERS).json()
            commits.append({
                'sha': commit['sha'],
                'author': commit_detail['commit']['author']['name'],
                'date': commit_detail['commit']['author']['date'],
                'message': commit_detail['commit']['message']
            })
            time.sleep(0.3)

    return {
        'metadata': {
            'name': repo_name,
            'owner': ORGANIZATION,
            'created_at': repo_info.get('created_at'),
            'updated_at': repo_info.get('updated_at'),
            'language': repo_info.get('language'),
            'stars': repo_info.get('stargazers_count', 0)
        },
        'readme': readme,
        'contributors': contributors,
        'commits': commits,
        'collected_at': datetime.now().isoformat()
    }

def save_repo_files(repo_name, data):
    """Sauvegarde les données dans des fichiers organisés"""
    try:
        os.makedirs(f"output/{repo_name}", exist_ok=True)
        
        # 1. Fichier JSON complet
        with open(f'output/{repo_name}/full_data.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        # 2. Fichier texte des contributeurs
        with open(f'output/{repo_name}/contributors.txt', 'w', encoding='utf-8') as f:
            f.write(f"Contributeurs de {repo_name}\n")
            f.write("="*50 + "\n")
            for contrib in data['contributors']:
                f.write(f"Login: {contrib['login']}\n")
                f.write(f"Nom: {contrib.get('name', 'N/A')}\n")
                f.write(f"Contributions: {contrib['contributions']}\n")
                f.write(f"Email: {contrib.get('email', 'N/A')}\n")
                f.write("-"*50 + "\n")
        
        # 3. Fichier README
        if data['readme']:
            with open(f'output/{repo_name}/README.md', 'w', encoding='utf-8') as f:
                f.write(data['readme']['content'])
        
        # 4. Fichier des commits
        with open(f'output/{repo_name}/commits.txt', 'w', encoding='utf-8') as f:
            f.write(f"Commits de {repo_name}\n")
            f.write("="*50 + "\n")
            for commit in data['commits']:
                f.write(f"ID: {commit['sha'][:7]}\n")
                f.write(f"Auteur: {commit['author']}\n")
                f.write(f"Date: {commit['date']}\n")
                f.write(f"Message: {commit['message']}\n")
                f.write("-"*50 + "\n")
        
        logger.info(f"Fichiers sauvegardés pour {repo_name}")
    except Exception as e:
        logger.error(f"Erreur sauvegarde fichiers: {e}")

def main():
    # Initialisation Kafka
    producer = setup_kafka()
    
    # Traitement des dépôts
    for repo_name in REPOSITORIES:
        try:
            logger.info(f"\n{'='*60}")
            logger.info(f"Traitement de {ORGANIZATION}/{repo_name}")
            
            # Récupération des données
            repo_data = get_repo_data(repo_name)
            if not repo_data:
                logger.warning("Dépôt non trouvé - Skipping")
                continue
            
            # Sauvegarde locale
            save_repo_files(repo_name, repo_data)
            
            # Envoi à Kafka (optionnel)
            if producer:
                producer.send('github-repos', value=repo_data)
                producer.flush()
            
            time.sleep(2)  # Pause entre dépôts
            
        except Exception as e:
            logger.error(f"Erreur traitement {repo_name}: {e}")
    
    if producer:
        producer.close()
    logger.info("Traitement terminé")

if __name__ == "__main__":
    # Nettoyage du dossier output
    if os.path.exists('output'):
        for filename in os.listdir('output'):
            file_path = os.path.join('output', filename)
            try:
                if os.path.isdir(file_path):
                    import shutil
                    shutil.rmtree(file_path)
                elif os.path.isfile(file_path):
                    os.unlink(file_path)
            except Exception as e:
                logger.error(f"Erreur nettoyage {file_path}: {e}")
    
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Interruption manuelle")
    except Exception as e:
        logger.error(f"Erreur inattendue: {e}")