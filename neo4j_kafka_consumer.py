import json
import logging
from confluent_kafka import Consumer
from neo4j import GraphDatabase

# --- Logging setup ---
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    handlers=[logging.StreamHandler()]
)

# --- Kafka setup ---
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'neo4j-processing',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf)
topics = ['contributor_skill_summary', 'contributor_module_rank', 'modules_summary']
consumer.subscribe(topics)
logging.info(f"Subscribed to topics: {topics}")

# --- Neo4j setup ---
neo4j_uri = "bolt://localhost:7687"
neo4j_user = "neo4j"
neo4j_password = "test12345678"
driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
logging.info("Connected to Neo4j")

# --- Neo4j write functions ---
def insert_module(tx, record):
    logging.debug(f"Inserting module node and linking to technology: {record}")
    tx.run("""
        MERGE (m:Module {name: $module})
        SET m.num_contributors = $num_contributors,
            m.num_commits = $num_commits,
            m.total_churn = $total_churn,
            m.age_days = $age_days,
            m.popularity_score = $popularity_score,
            m.first_commit_time = $first_commit_time,
            m.last_commit_time = $last_commit_time,
            m.commit_cnt = $commit_cnt,
            m.single_author_module = $single_author_module,
            m.stability_score = $stability_score,
            m.maturity_score = $maturity_score,
            m.cohesion_score = $cohesion_score

        MERGE (t:Language  {name: $tech})
        MERGE (m)-[:USES]->(t)
    """,
    module=record["module_candidate"],
    num_contributors=record["num_contributors"],
    num_commits=record["num_commits"],
    total_churn=record["total_churn"],
    age_days=record["age_days"],
    popularity_score=record["popularity_score"],
    first_commit_time=record.get("first_commit_time"),
    last_commit_time=record.get("last_commit_time"),
    commit_cnt=record.get("commit_cnt"),
    single_author_module=record.get("single_author_module"),
    stability_score=record.get("stability_score"),
    maturity_score=record.get("maturity_score"),
    cohesion_score=record.get("cohesion_score"),
    tech=record["dominant_language"])


def insert_developer_module_edge(tx, record):
    logging.debug(f"Linking developer to module: {record}")
    tx.run("""
        MERGE (a:Developer {name: $author})
        MERGE (m:Module {name: $module})
        MERGE (a)-[r:CONTRIBUTED_TO]->(m)
        SET r.contributions = $contributions,
            r.primary_author = $primary,
            r.recent_contributor = $recent,
            r.rank = $rank,
            r.last_commit = datetime($last_commit),
            r.language = $language
    """, 
        author=record["author_name"],
        module=record["module_path"],
        contributions=record["contributions"],
        primary=record["primary_author"],
        recent=bool(record["recent_contributor"]),
        rank=record["rank"],
        last_commit=record["last_commit"],
        language=record["dominant_language"]
    )

def insert_developer_tech_edge(tx, record):
    logging.debug(f"Linking developer to tech: {record}")
    tx.run("""
        MERGE (a:Developer {name: $author})
        MERGE (t:Language {name: $tech})
        MERGE (a)-[r:HAS_SKILL_IN]->(t)
        SET r.total_contributions = $total_contributions,
            r.last_commit = datetime($last_commit),
            r.recent = $recent
    """, 
        author=record["author_name"],
        tech=record["dominant_language"],
        total_contributions=record["total_contributions"],
        last_commit=record["last_commit"],
        recent=bool(record["recent_contributor"])
    )

# --- Main loop ---
try:
    with driver.session() as session:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                logging.debug("No message received.")
                continue

            if msg.error():
                logging.error(f"Kafka error: {msg.error()}")
                continue

            topic = msg.topic()
            value = msg.value().decode('utf-8')
            logging.info(f"Received message from {topic}: {value}")

            try:
                record = json.loads(value)

                if topic == "modules_summary":
                    session.execute_write(insert_module, record)
                    logging.info("Module node written to Neo4j.")
                elif topic == "contributor_module_rank":
                    session.execute_write(insert_developer_module_edge, record)
                    logging.info("Developer–Module edge written to Neo4j.")
                elif topic == "contributor_skill_summary":
                    session.execute_write(insert_developer_tech_edge, record)
                    logging.info("Developer–Language edge written to Neo4j.")
                else:
                    logging.warning(f"Unhandled topic: {topic}")

            except json.JSONDecodeError as e:
                logging.error(f"JSON decode error: {e}")
            except Exception as e:
                logging.exception(f"Unexpected error during processing: {e}")

except KeyboardInterrupt:
    logging.info("Kafka consumer stopped by user.")
finally:
    consumer.close()
    driver.close()
    logging.info("Kafka consumer and Neo4j driver closed.")
