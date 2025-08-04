from flask import Flask, request, jsonify
from flask_cors import CORS
from database import get_collections
from matching_utils import match_developer_to_project
from datetime import datetime
from flask import Flask, request, jsonify
from neo4j_service import initialize_neo4j_driver

neo4j_driver = initialize_neo4j_driver()

app = Flask(__name__)
CORS(app, supports_credentials=True)

collections = get_collections()

if collections["contributors"].count_documents({"available": {"$exists": False}}) > 0:
    collections["contributors"].update_many(
        {"available": {"$exists": False}},
        {"$set": {"available": True}}
    )

#######################################API MongoDB#########################################

# @app.route("/developers/set_all_available", methods=["PUT"])
# def set_all_available_true():
#     result = collections["contributors"].update_many({}, {"$set": {"available": True}})
#     return jsonify({"message": f"{result.modified_count} développeurs mis à jour avec available=True."}), 200

#retourner tous les projets
@app.route("/projects", methods=["GET"])
def get_projects():
    projects = list(collections["projects"].find({}, {"_id": 0}))
    return jsonify(projects)

# Ajouter un projet 
@app.route("/projects/add", methods=["POST"])
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

#details d'un projet
@app.route("/projects/<repository>", methods=["GET"])
def get_project_by_repository(repository):
    project = collections["projects"].find_one({"repository": repository}, {"_id": 0})
    if not project:
        return jsonify({"error": "Project not found"}), 404

    # Récupérer les noms des développeurs assignés à ce projet
    assigned_devs = list(collections["assignments"].find(
        {"repository": repository}, {"_id": 0, "developer": 1}
    ))
    assigned_dev_names = [dev["developer"] for dev in assigned_devs]

    # Récupérer les détails complets des développeurs assignés
    full_devs = list(collections["contributors"].find(
        {"author": {"$in": assigned_dev_names}},
        {"_id": 0}
    ))

    # Ajouter à la réponse
    project["assigned_developers"] = full_devs

    return jsonify(project), 200

#suppression d'un projet
@app.route("/projects/<repository>", methods=["DELETE"])
def delete_project(repository):
    result = collections["projects"].delete_one({"repository": repository})
    if result.deleted_count == 0:
        return jsonify({"error": "Project not found"}), 404
    return jsonify({"message": f"Project '{repository}' deleted successfully."}), 200

#met a jour un projet
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

#retourner le nombre de projet par langage
@app.route("/projects/language-stats", methods=["GET"])
def get_language_stats():
    projects = list(collections["projects"].find())
    language_count = {}

    for project in projects:
        for lang in project.get("languages", []):
            lang = lang.strip().lower() 
            language_count[lang] = language_count.get(lang, 0) + 1

    return jsonify(language_count)

#recommender un developpeur à un projet
@app.route("/projects/<repository>/recommend/combined", methods=["GET"])
def recommend_combined(repository):
    project = collections["projects"].find_one({"repository": repository})
    if not project:
        return jsonify({"error": "Project not found"}), 404

    devs = list(collections["contributors"].find({"available": True}, {"_id": 0}))

    # Match part: techno & framework match from MongoDB
    initial_scores = {}
    for dev in devs:
        match = match_developer_to_project(dev, project)
        initial_scores[match["developer"]] = {
            "match_score": match["score"],
            "matched_technologies": match["matched_technologies"],
            "repository": match["repository"]
        }

    # Enrich with Neo4j contributions
    techs = project.get("languages", []) + project.get("frameworks", [])
    with neo4j_driver.session() as session:
        contributions = session.execute_read(find_top_developers_by_tech_stack, techs)

    for tech, dev_list in contributions.items():
        max_contrib = max((dev["total_contributions"] for dev in dev_list), default=1)
        for dev in dev_list:
            name = dev["name"]
            normalized = dev["total_contributions"] / max_contrib
            if name in initial_scores:
                initial_scores[name]["contrib_score"] = initial_scores[name].get("contrib_score", 0) + normalized

    # Fusionner les scores avec pondération
    results = []
    for dev_name, info in initial_scores.items():
        match_score = info["match_score"]
        contrib_score = info.get("contrib_score", 0)
        final_score = round((0.7 * match_score) + (0.3 * contrib_score * 100), 2)  # poids: 70% match, 30% contributions
        results.append({
            "developer": dev_name,
            "repository": info["repository"],
            "score": final_score,
            "matched_technologies": info["matched_technologies"]
        })

    # Trier par score décroissant
    results.sort(key=lambda x: -x["score"])
    return jsonify(results[:3])

#assigner un developpeur a un projet
@app.route("/projects/assign", methods=["POST"])
def assign_developer():
    data = request.get_json()
    developer_name = data.get("developer")
    repository = data.get("repository")

    if not developer_name or not repository:
        return jsonify({"error": "Both 'developer' and 'repository' are required"}), 400

    # Check if developer exists
    developer = collections["contributors"].find_one({"author": developer_name})
    if not developer:
        return jsonify({"error": "Developer not found"}), 404

    # Check if already assigned (disponibilité true)
    if not developer.get("available", True):
        return jsonify({"message": "Developer is already assigned to another project"}), 400

    # Assign developer (disponible = true)
    collections["contributors"].update_one(
        {"author": developer_name},
        {"$set": {"available": False}}
    )

    # Optionally: track assignment (e.g., add to a collection)
    collections["assignments"].insert_one({
        "developer": developer_name,
        "repository": repository,
        "assigned_at": datetime.utcnow()
    })

    return jsonify({"message": f"Developer '{developer_name}' assigned to project '{repository}'"}), 200

#supprimer un developepeur d'un projet
@app.route("/projects/unassign", methods=["POST"])
def unassign_developer():
    data = request.get_json()
    developer_name = data.get("developer")
    repository = data.get("repository")

    if not developer_name or not repository:
        return jsonify({"error": "Both 'developer' and 'repository' are required"}), 400

    # Vérifier que l'assignation existe
    assignment = collections["assignments"].find_one({
        "developer": developer_name,
        "repository": repository
    })

    if not assignment:
        return jsonify({"error": "This developer is not assigned to the specified project"}), 404

    # Supprimer l'assignation
    collections["assignments"].delete_one({
        "developer": developer_name,
        "repository": repository
    })

    # Mettre à jour la disponibilité du développeur
    collections["contributors"].update_one(
        {"author": developer_name},
        {"$set": {"available": True}}
    )

    return jsonify({"message": f"Developer '{developer_name}' removed from project '{repository}' and set available."}), 200


#####################################################API Neo4j#####################################

from flask_restx import Api, Resource, fields
from neo4j_service import initialize_neo4j_driver, find_top_developers_by_tech_stack, add_project_with_devs_and_tech, find_top_modules_by_tech_stack
from neo4j.exceptions import Neo4jError

# Flask app setup
api = Api(app, version='1.0', title='Developer Recommendation API',
          description='Recommend developers by technology stack with contribution ranking')

# ==== Models ====

project_input_model = api.model('ProjectInput', {
    'project_name': fields.String(required=True, description='Name of the project'),
    'developers': fields.List(fields.Nested(api.model('DeveloperInput', {
        'name': fields.String(required=True, description='Developer name'),
        'technologies': fields.List(fields.String, required=True, description='Technologies used by the developer')
    })), required=True)
})

tech_list_model = api.model('TechList', {
    'technologies': fields.List(fields.String, required=True, description='List of technologies')
})

developer_model = api.model('Developer', {
    'name': fields.String(required=True, description='Developer name'),
    'total_contributions': fields.Integer(required=True, description='Total contributions in this tech')
})

recommendation_model = api.model('Recommendations', {
    'technology': fields.List(fields.Nested(developer_model), description='List of recommended developers per tech')
})

# ==== Routes ====

@api.route('/recommend/developers')
class DeveloperRecommendation(Resource):
    @api.expect(tech_list_model, validate=True)
    @api.doc(description="Recommend developers by a list of technologies, ranked by contributions.")
    def post(self):
        data = request.json
        tech_stack = data.get("technologies", [])

        if not tech_stack:
            api.abort(400, "Technologies list cannot be empty.")

        try:
            with neo4j_driver.session() as session:
                recommendations = session.execute_read(find_top_developers_by_tech_stack, tech_stack)
            return recommendations
        except Neo4jError as e:
            api.abort(500, f"Neo4j query failed: {str(e)}")
        except Exception as e:
            api.abort(500, f"Unexpected error: {str(e)}")


@api.route('/projects')
class ProjectRegistration(Resource):
    @api.expect(project_input_model, validate=True)
    @api.doc(description="Add a project and associate developers with technologies.")
    def post(self):
        data = request.json
        project_name = data["project_name"]
        developers = data["developers"]

        if not developers:
            api.abort(400, "Developers list cannot be empty.")

        try:
            with neo4j_driver.session() as session:
                session.execute_write(add_project_with_devs_and_tech, project_name, developers)
            return {"message": "Project and developers added successfully."}, 201
        except Neo4jError as e:
            api.abort(500, f"Neo4j write failed: {str(e)}")
        except Exception as e:
            api.abort(500, f"Unexpected error: {str(e)}")

from flask_restx import Resource, fields

# Define input model for tech list (reuse existing tech_list_model if applicable)
module_model = api.model("Module", {
    "module_name": fields.String,
    "num_contributors": fields.Integer,
    "num_commits": fields.Integer,
    "total_churn": fields.Integer,
})

module_recommendation_model = api.model("ModuleRecommendation", {
    "technology": fields.String,
    "modules": fields.List(fields.Nested(module_model))
})

@api.route('/recommend/modules')
class ModuleRecommendation(Resource):
    @api.expect(tech_list_model, validate=True)
    @api.marshal_with(module_recommendation_model)
    @api.doc(description="Recommend modules by a list of technologies, ranked by contributors and commits.")
    def post(self):
        data = request.json
        tech_stack = data.get("technologies", [])

        if not tech_stack:
            api.abort(400, "Technologies list cannot be empty.")

        try:
            with neo4j_driver.session() as session:
                recommendations = session.execute_read(find_top_modules_by_tech_stack, tech_stack)
                # Transform dict into list
                response_data = [
                    {"technology": tech, "modules": modules}
                    for tech, modules in recommendations.items()
                ]

            return response_data
        except Exception as e:
            api.abort(500, f"Failed to retrieve module recommendations: {str(e)}")

if __name__ == "__main__":
    app.run(debug=True)
