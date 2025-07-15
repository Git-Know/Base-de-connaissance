import re
# import json
# import os
import spacy
from transformers import pipeline


nlp = spacy.load("en_core_web_md")

LANGUAGES = {"python", "java", "javascript", "typescript", "c++", "c#", "php", "ruby"}
FRAMEWORKS = {"django", "flask", "fastapi", "spring", "express", "react", "angular", "vue", "nestjs", "rails", "ionic"}
TOOLS = {"docker", "kafka", "git", "kubernetes", "spark", "hadoop","erp","graphql"}
FEATURES = {"authentication","scheduling", "chat", "dashboard", "search", "cart", "payment", "upload", "download", "rest api", "notification"}
DOMAINS = {"e commerce", "nlp", "healthcare", "education", "finance", "iot", "machine learning","ecommerce"}
SECURITY = {"jwt", "oauth2", "https", "rbac"}
DATABASES = {"mysql", "postgresql", "mongodb", "sqlite", "firebase", "redis"}

def clean_text(text):
    text = re.sub(r"[^\w\s]", " ", text) #supprime les ponctuations 
    text = re.sub(r"\s+", " ", text) #remplace les suites des espaces en un seul
    return text.strip() #supprime les espaces au debut et au fin du chaine

# def save_json(data, path):
#     dir_path = os.path.dirname(path)
#     if dir_path != "":
#         os.makedirs(dir_path, exist_ok=True)
#     with open(path, "w", encoding="utf-8") as f:
#         json.dump(data, f, indent=2, ensure_ascii=False)

def extract_entities(text, project_name=None):
    doc = nlp(text)
    text_lower = text.lower()

    spacy_entities = {(ent.text.strip().lower(), ent.label_) for ent in doc.ents}
    already_found = {e[0] for e in spacy_entities}

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

summarizer = pipeline("summarization", model="facebook/bart-large-cnn")

def generate_summary_nlp(text, project_name="This project"):
    max_chunk = 1024
    chunks = [text[i:i+max_chunk] for i in range(0, len(text), max_chunk)]

    full_summary = ""
    for chunk in chunks:
        summary = summarizer(chunk, max_length=130, min_length=30, do_sample=False)
        full_summary += summary[0]['summary_text'] + " "

    return full_summary.strip()

