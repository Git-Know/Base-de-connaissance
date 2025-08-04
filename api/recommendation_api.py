from flask import Flask, request, jsonify
from neo4j import GraphDatabase
from flask_restx import Api, Resource, fields

app = Flask(__name__)

# Initialize this properly in your app setup
# --- Neo4j setup ---
neo4j_uri = "bolt://localhost:7687"
neo4j_user = "neo4j"
neo4j_password = "test12345678"
neo4j_driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))


api = Api(app, version='1.0', title='Developer Recommendation API',
          description='Recommend developers by technology stack with contribution ranking')

# Define the expected input model for request validation
tech_list_model = api.model('TechList', {
    'technologies': fields.List(fields.String, required=True, description='List of technologies')
})

# Define the output model (simplified)
developer_model = api.model('Developer', {
    'name': fields.String(required=True, description='Developer name'),
    'total_contributions': fields.Integer(required=True, description='Total contributions in this tech')
})

recommendation_model = api.model('Recommendations', {
    # This maps tech to list of developers
    'technology': fields.List(fields.Nested(developer_model), description='List of recommended developers per tech')
})


@api.route('/recommend/developers')
class DeveloperRecommendation(Resource):
    @api.expect(tech_list_model, validate=True)
    @api.doc(description="Recommend developers by a list of technologies, ranked by contributions.")
    def post(self):
        data = request.json
        tech_stack = data.get("technologies", [])

        with neo4j_driver.session() as session:
            recommendations = session.execute_read(find_top_developers_by_tech_stack, tech_stack)

        return recommendations


def find_top_developers_by_tech_stack(tx, tech_stack):
    query = """
    UNWIND $stack AS tech
    MATCH (dev:Developer)-[r:HAS_SKILL_IN]->(lang:Language)
    WHERE lang.name = tech
    RETURN tech AS technology,
           dev.name AS developer_name,
           r.total_contributions AS contributions
    ORDER BY tech, r.total_contributions DESC
    """

    result = tx.run(query, stack=tech_stack)

    recommendations = {tech: [] for tech in tech_stack}
    for row in result:
        tech = row["technology"]
        dev_name = row["developer_name"]
        contributions = row["contributions"]
        recommendations[tech].append({
            "name": dev_name,
            "total_contributions": contributions
        })

    return recommendations


if __name__ == '__main__':
    app.run(debug=True, port=5001)