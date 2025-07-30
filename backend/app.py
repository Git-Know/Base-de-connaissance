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

# Ajouter un projet 
@app.route("/projects", methods=["POST"])
def add_project():
    data = request.get_json()
    if not data:
        return jsonify({"error": "Invalid JSON"}), 400

    repo_name = data.get("repository")
    languages = data.get("languages", [])
    frameworks = data.get("frameworks", [])
    features = data.get("features", [])
    summary = data.get("summary")

    if not repo_name or not summary:
        return jsonify({"error": "repository and summary are required"}), 400

    exists = collections["projects"].count_documents({"repository": repo_name})
    if exists:
        return jsonify({"message": "Project already exists"}), 200

    project_doc = {
        "repository": repo_name,
        "languages": languages,
        "frameworks": frameworks,
        "features": features,
        "summary": summary
    }

    inserted = collections["projects"].insert_one(project_doc)
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

@app.route("/projects/<repository>", methods=["PUT"])
def update_project(repository):
    data = request.get_json()
    if not data:
        return jsonify({"error": "Invalid JSON"}), 400

    updated_doc = {
        "languages": data.get("languages", []),
        "frameworks": data.get("frameworks", []),
        "features": data.get("features", []),
        "summary": data.get("summary", "")
    }

    result = collections["projects"].update_one({"repository": repository}, {"$set": updated_doc})
    if result.matched_count == 0:
        return jsonify({"error": "Project not found"}), 404

    return jsonify({"message": "Project updated successfully"}), 200

if __name__ == "__main__":
    app.run(debug=True)
