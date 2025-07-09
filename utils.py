import re
import json
import os
import spacy

nlp = spacy.load("en_core_web_md")

LANGUAGES = {"python", "java", "javascript", "typescript", "c++", "c#", "php", "ruby"}
FRAMEWORKS = {"django", "flask", "fastapi", "spring", "express", "react", "angular", "vue", "nestjs", "rails", "ionic"}
TOOLS = {"docker", "kafka", "git", "kubernetes", "spark", "hadoop","erp","graphql"}
FEATURES = {"authentication","scheduling", "dashboard", "search", "cart", "payment", "upload", "download", "rest api", "notification"}
DOMAINS = {"e commerce", "nlp", "healthcare", "education", "finance", "chat", "iot", "machine learning"}
SECURITY = {"jwt", "oauth2", "https", "rbac"}
DATABASES = {"mysql", "postgresql", "mongodb", "sqlite", "firebase", "redis"}

def clean_text(text):
    text = re.sub(r"[^\w\s]", " ", text) #supprime les ponctuations 
    text = re.sub(r"\s+", " ", text) #remplace les suites des espaces en un seul
    return text.strip() #supprime les espaces au debut et au fin du chaine

def save_json(data, path):
    dir_path = os.path.dirname(path)
    if dir_path != "":
        os.makedirs(dir_path, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

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

def generate_summary(entities, project_name="This project"):
    summary = f"{project_name} is a project"

    # Domain
    if entities.get("domain"):
        domains = ", ".join(entities["domain"])
        summary += f" in the field of {domains}"

    # Features
    if entities.get("features"):
        feats = ", ".join(entities["features"][:3])
        summary += f", offering features such as {feats}"

    # Technologies
    tech_parts = []
    if entities.get("languages"):
        tech_parts.append("language(s): " + ", ".join(entities["languages"]))
    if entities.get("frameworks"):
        tech_parts.append("framework(s): " + ", ".join(entities["frameworks"]))
    if entities.get("tools"):
        tech_parts.append("tool(s): " + ", ".join(entities["tools"]))
    if tech_parts:
        summary += ". It uses " + "; ".join(tech_parts)

    # Database
    if entities.get("database"):
        summary += f". It relies on databases such as {', '.join(entities['database'])}"

    # Security
    if entities.get("security"):
        summary += f". It integrates security mechanisms such as {', '.join(entities['security'])}"

    summary += "."

    return summary

