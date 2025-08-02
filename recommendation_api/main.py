from flask import Flask, request, jsonify
from flask_restx import Api, Resource, fields
from neo4j_service import initialize_neo4j_driver, find_top_developers_by_tech_stack, add_project_with_devs_and_tech, find_top_modules_by_tech_stack
from neo4j.exceptions import Neo4jError

# Flask app setup
app = Flask(__name__)
api = Api(app, version='1.0', title='Developer Recommendation API',
          description='Recommend developers by technology stack with contribution ranking')

# Neo4j driver setup
neo4j_driver = initialize_neo4j_driver()

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


if __name__ == '__main__':
    app.run(debug=True)
