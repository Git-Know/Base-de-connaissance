from neo4j import GraphDatabase, Driver, Transaction
from typing import List, Dict, Any


def initialize_neo4j_driver() -> Driver:
    # localhost -> neo4j
    return GraphDatabase.driver("bolt://neo4j:7687", auth=("neo4j", "test12345678"))


def add_project_with_devs_and_tech(
    tx: Transaction,
    project_name: str,
    developers: List[Dict[str, Any]]
) -> None:
    for dev in developers:
        dev_name: str = dev["name"]
        techs: List[str] = dev["technologies"]

        # Create Developer and Project nodes and their relationship
        tx.run(
            """
            MERGE (d:Developer {name: $dev_name})
            MERGE (p:Project {name: $project_name})
            MERGE (d)-[:WORKED_ON]->(p)
            """,
            project_name=project_name,
            dev_name=dev_name,
        )

        for tech in techs:
            # Create Tech nodes, link Developer to Tech, and Tech to Project
            tx.run(
                """
                MERGE (d:Developer {name: $dev_name})
                MERGE (t:Language {name: toLower($tech)})
                MERGE (d)-[:USED_TECH]->(t)
                MERGE (t)-[:IN_PROJECT]->(p)
                """,
                dev_name=dev_name,
                tech=tech,
                project_name=project_name,
            )



def find_top_developers_by_tech_stack(
    tx: Transaction,
    tech_stack: List[str]
) -> Dict[str, List[Dict[str, Any]]]:
    query = """
    UNWIND $stack AS tech
    MATCH (dev:Developer)-[:HAS_SKILL_IN]->(t:Language)
    WHERE t.name = toLower(tech)
    RETURN tech AS technology,
           dev.name AS developer_name,
           COUNT(*) AS contributions
    ORDER BY tech, contributions DESC
    """

    result = tx.run(query, stack=tech_stack)

    recommendations: Dict[str, List[Dict[str, Any]]] = {tech: [] for tech in tech_stack}
    for row in result:
        tech = row["technology"]
        recommendations[tech].append({
            "name": row["developer_name"],
            "total_contributions": row["contributions"],
        })

    return recommendations


def find_top_modules_by_tech_stack(
    tx: Transaction,
    tech_stack: List[str]
) -> Dict[str, List[Dict[str, Any]]]:
    query = """
    UNWIND $stack AS tech
    MATCH (m:Module)-[:USES]->(t:Language)
    WHERE t.name = toLower(tech)
    RETURN tech AS technology,
           m.name AS module_name,
           m.num_contributors AS num_contributors,
           m.num_commits AS num_commits,
           m.total_churn AS total_churn
    ORDER BY tech, m.num_contributors DESC, m.num_commits DESC, m.total_churn ASC
            """

    result = tx.run(query, stack=tech_stack)

    recommendations: Dict[str, List[Dict[str, Any]]] = {tech: [] for tech in tech_stack}
    for row in result:
        tech = row["technology"]
        recommendations[tech].append({
            "module_name": row["module_name"],
            "num_contributors": row["num_contributors"],
            "num_commits": row["num_commits"],
            "total_churn": row["total_churn"]
        })
    return recommendations
