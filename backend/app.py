from flask import Flask, request, jsonify
from flask_cors import CORS
from database import get_collections
from matching_utils import match_developer_to_project,clean_text, extract_entities, generate_summary_nlp
app = Flask(__name__)
CORS(app, supports_credentials=True)

collections = get_collections()

#retourner tous les projets
@app.route("/projects", methods=["GET"])
def get_projects():
    projects = list(collections["projects"].find({}, {"_id": 0}))
    return jsonify(projects)

#ajouter un projet
@app.route("/projects", methods=["POST"])
def add_project():
    # Récupérer les champs du formulaire (form-data)
    repo_name = request.form.get("repository")
    languages = request.form.getlist("languages")  # plusieurs valeurs possibles
    frameworks = request.form.getlist("frameworks")

    if not repo_name:
        return jsonify({"error": "repository is required"}), 400

    # Vérifier si fichier README est uploadé
    if "readme" not in request.files:
        return jsonify({"error": "README file is required"}), 400

    readme_file = request.files["readme"]
    readme_text = readme_file.read().decode("utf-8")  # lire le contenu en texte

    # Nettoyer et extraire entités
    cleaned_readme = clean_text(readme_text)
    entities = extract_entities(cleaned_readme, repo_name)
    summary = generate_summary_nlp(cleaned_readme, repo_name)

    exists = collections["projects"].count_documents({"repository": repo_name})
    if exists:
        return jsonify({"message": "Project already exists"}), 200

    project_doc = {
        "repository": repo_name,
        "languages": languages,
        "frameworks": frameworks,
        "tools": entities.get("tools", []),
        "features": entities.get("features", []),
        "domain": entities.get("domain", []),
        "security": entities.get("security", []),
        "database": entities.get("database", []),
        "summary": summary
    }

    # insertion en DB
    inserted = collections["projects"].insert_one(project_doc)

    # ajouter le champ _id en string pour la réponse
    project_doc["_id"] = str(inserted.inserted_id)
    
    return jsonify({"message": "Project added successfully", "project": project_doc}), 201

#developpeurs recommendes
@app.route("/projects/<repository>/recommend", methods=["GET"])
def recommend_developers(repository):
    project = collections["projects"].find_one({"repository": repository})
    if not project:
        return jsonify({"error": "Project not found"}), 404

    developers = list(collections["contributors"].find({}, {"_id": 0}))

    results = [
        match_developer_to_project(dev, project)
        for dev in developers
    ]
    results.sort(key=lambda x: (-x["score"]))
    return jsonify(results[:3])

#details d'un projet
@app.route("/projects/<repository>", methods=["GET"])
def get_project_by_repository(repository):
    project = collections["projects"].find_one({"repository": repository}, {"_id": 0})
    if not project:
        return jsonify({"error": "Project not found"}), 404
    return jsonify(project), 200

#suppression d'un projet
@app.route("/projects/<repository>", methods=["DELETE"])
def delete_project(repository):
    result = collections["projects"].delete_one({"repository": repository})
    if result.deleted_count == 0:
        return jsonify({"error": "Project not found"}), 404
    return jsonify({"message": f"Project '{repository}' deleted successfully."}), 200

if __name__ == "__main__":
    app.run(debug=True)
