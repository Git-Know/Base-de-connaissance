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
    logging.debug(f"Inserting module node and linking to technologies and repo: {record}")
    query = """
        MERGE (r:Repository {name: $repo})
        
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

        MERGE (r)-[:HAS_MODULE]->(m)

        FOREACH (lang IN $languages | 
            MERGE (t:Language {name: lang})
            MERGE (m)-[:USES]->(t)
        )
    """
    tx.run(query,
        repo=record["repo"],
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
        languages=record.get("languages", [])
    )


def insert_developer_module_edge(tx, record, module_path):
    module_path = record.get("module_path") or record.get("module_candidate")
    if not module_path:
        raise ValueError("Missing 'module_path' in developer record")
    logging.debug(f"Linking developer to module: {record}")
    query = """
        MERGE (a:Developer {name: $author})
        MERGE (m:Module {name: $module})
        MERGE (a)-[rel:CONTRIBUTED_TO]->(m)
        SET rel.contributions = $contributions,
            rel.primary_author = $primary,
            rel.recent_contributor = $recent,
            rel.rank = $rank,
            rel.last_commit = datetime($last_commit),
            rel.language = $language
    """
    tx.run(query,
        author=record["author_name"],
        module=module_path,
        contributions=record.get("total_contributions", 0),
        primary=record.get("primary_author", False),
        recent=bool(record.get("recent_contributor", False)),
        rank=record.get("rank", 0.0),
        last_commit=record["last_commit"],
        language=record.get("language", "unknown")
    )

def insert_developer_tech_edge(tx, record):
    logging.debug(f"Linking developer to tech: {record}")
    query = """
        MERGE (a:Developer {name: $author})
        MERGE (t:Language {name: $tech})
        MERGE (a)-[r:HAS_SKILL_IN]->(t)
        SET r.total_contributions = $total_contributions,
            r.last_commit = datetime($last_commit),
            r.recent = $recent
    """
    tx.run(query,
        author=record["author_name"],
        tech=record.get("language", "unknown"),
        total_contributions=record.get("total_contributions", 0),
        last_commit=record["last_commit"],
        recent=bool(record.get("recent_contributor", False))
    )
# --- Main “run once” logic ---
try:
    with driver.session() as session:
        # give yourself up to 5 seconds of inactivity before quitting
        NO_MSG_TIMEOUT = 5.0  
        while True:
            msg = consumer.poll(NO_MSG_TIMEOUT)

            # if no message arrives in NO_MSG_TIMEOUT seconds, we assume queue is drained
            if msg is None:
                logging.info("No more messages; exiting consumer loop.")
                break

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
                    session.execute_write(insert_developer_module_edge, record, record["module_path"])
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
    logging.info("Interrupted by user.")
finally:
    consumer.close()
    driver.close()
    logging.info("Consumer and Neo4j driver closed.")
