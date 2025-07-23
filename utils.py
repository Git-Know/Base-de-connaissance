import re
import spacy
from transformers import pipeline

# Charger modèle spaCy et pipeline Transformers une seule fois
nlp = spacy.load("en_core_web_md")
summarizer = pipeline("summarization", model="facebook/bart-large-cnn")

# Listes de référence
LANGUAGES = {"python", "java", "javascript", "typescript", "c++", "c#", "php", "ruby"}
FRAMEWORKS = {"django", "flask", "fastapi", "spring", "express", "react", "angular", "vue", "nestjs", "rails", "ionic"}
TOOLS = {"docker", "kafka", "git", "kubernetes", "spark", "hadoop", "erp", "graphql"}
FEATURES = {"authentication", "scheduling", "chat", "dashboard", "search", "cart", "payment", "upload", "download", "rest api", "notification"}
DOMAINS = {"e commerce", "nlp", "healthcare", "education", "finance", "iot", "machine learning", "ecommerce"}
SECURITY = {"jwt", "oauth2", "https", "rbac"}
DATABASES = {"mysql", "postgresql", "mongodb", "sqlite", "firebase", "redis"}

def clean_text(text):
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def extract_entities(text, project_name=None):
    doc = nlp(text)
    text_lower = text.lower()

    matched_langs = [lang for lang in LANGUAGES if lang in text_lower]
    matched_frameworks = [fw for fw in FRAMEWORKS if fw in text_lower]
    matched_tools = [tool for tool in TOOLS if tool in text_lower]
    matched_features = [feat for feat in FEATURES if feat in text_lower]
    matched_domains = [domain for domain in DOMAINS if domain in text_lower]
    matched_security = [sec.upper() for sec in SECURITY if sec in text_lower]
    matched_databases = [db for db in DATABASES if db in text_lower]

    result = {
        "languages": sorted(set(matched_langs)),
        "frameworks": sorted(set(matched_frameworks)),
        "tools": sorted(set(matched_tools)),
        "features": sorted(set(matched_features)),
        "domain": sorted(set(matched_domains)),
        "security": sorted(set(matched_security)),
        "database": sorted(set(matched_databases)),
    }

    return result

def generate_summary_nlp(text, project_name="This project"):
    max_chunk = 1024
    chunks = [text[i:i + max_chunk] for i in range(0, len(text), max_chunk)]

    full_summary = ""
    for chunk in chunks:
        summary = summarizer(chunk, max_length=130, min_length=30, do_sample=False)
        full_summary += summary[0]['summary_text'] + " "

    return full_summary.strip()


def match_developer_to_project(developer, project):
    # Supporte "author" ou "author_name"
    dev_name = developer.get("author") or developer.get("author_name") or "unknown"

    match_result = {
        "developer": dev_name,
        "repository": project.get("repository", "unknown"),
        "score": 0,
        "matched_technologies": {
            "languages": [],
            "frameworks": []
        }
    }

    dev_tech = {
        "languages": [tech.lower() for tech in developer.get("languages", [])],
        "frameworks": [tech.lower() for tech in developer.get("frameworks", [])]
    }

    weights = {
        "languages": 3,
        "frameworks": 2
    }

    max_score = sum(
        weights[cat] * len(project.get(cat, []))
        for cat in ["languages", "frameworks"]
    )

    score = 0

    for cat in ["languages", "frameworks"]:
        for tech in project.get(cat, []):
            if tech.lower() in dev_tech[cat]:
                match_result["matched_technologies"][cat].append(tech)
                score += weights[cat]

    percentage_score = (score / max_score) * 100 if max_score > 0 else 0
    contributions = developer.get("contributions") or developer.get("total_contributions", 0)
    bonus = min(contributions / 1000, 1.0) * 10
    final_score = min(percentage_score + bonus, 100)

    match_result["score"] = round(final_score, 2)
    return match_result
