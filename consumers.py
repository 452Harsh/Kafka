import json
from kafka import KafkaConsumer
from neo4j import GraphDatabase

# Kafka configuration
KAFKA_TOPIC = 'project1'  # Replace with your Kafka topic
KAFKA_BROKER = 'localhost:9092'  # Assuming Kafka is running on your local machine

# Neo4j configuration
NEO4J_URI = "neo4j+ssc://3f3cafed.databases.neo4j.io:7687"
NEO4J_USERNAME = "neo4j"
NEO4J_PASSWORD = "GicxwhcKtwFR19Liv4rJtUdB3mI7Qfur51AsVbIiatc"

# Create a Kafka consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',  # Start from the earliest messages
    enable_auto_commit=True,
    group_id='your_group_id'  # Replace with your consumer group ID
)

# Create a Neo4j driver
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))

def save_to_neo4j(data):
    """Function to save data to Neo4j."""
    request = """
    CREATE (m:Movie {id: $show_id, title: $title})
    SET 
        m.director = $director,
        m.country = $country,
        m.date_str = $date_added, 
        m.release_year = $release_year,
        m.rating = $rating,
        m.duration = $duration,
        m.listed_in = $listed_in,
        m.description = $description,
        m.cast = $cast,
        m.year = $year,
        m.month = $month,
        m.day = $day,
        m.type = $type_movie
    """

    try:
        with driver.session() as session:
            session.run(request,
                         show_id=data.get('show_id'),
                         title=data.get('title'),
                         director=data.get('director'),
                         country=data.get('country'),
                         date_added=data.get('date_added'),
                         release_year=data.get('release_year'),
                         rating=data.get('rating'),
                         duration=data.get('duration'),
                         listed_in=data.get('listed_in'),
                         description=data.get('description'),
                         cast=data.get('cast'),
                         year=data.get('year'),
                         month=data.get('month'),
                         day=data.get('day'),
                         type_movie=data.get('type'))
        print(f"Saved to Neo4j: {data.get('title')}")  # Confirmation message
    except Exception as e:
        print(f"Failed to save {data.get('title')} to Neo4j: {e}")

def create_relationships(data):
    """Function to create relationships for the movie."""
    try:
        # Create Persons for the cast
        cast_request = """
        MATCH (m:Movie {id: $show_id})
        UNWIND split($cast, ',') AS actor
        MERGE (p:Person {name: trim(actor)})
        MERGE (p)-[:ACTED_IN]->(m)
        """
        
        # Create Categories
        category_request = """
        MATCH (m:Movie {id: $show_id})
        UNWIND split($listed_in, ',') AS category
        MERGE (c:Category {name: trim(category)})
        MERGE (m)-[:IN_CATEGORY]->(c)
        """

        # Create Type
        type_request = """
        MATCH (m:Movie {id: $show_id})
        MERGE (t:Type {type: $type_movie})
        MERGE (m)-[:TYPED_AS]->(t)
        """
        
        # Create Director
        director_request = """
        MATCH (m:Movie {id: $show_id})
        MERGE (d:Person {name: $director})
        MERGE (d)-[:DIRECTED]->(m)
        """

        # Create Countries
        country_request = """
        MATCH (m:Movie {id: $show_id})
        UNWIND split($country, ',') AS country
        MERGE (c:Country {name: trim(country)})
        MERGE (m)-[:WHERE]->(c)
        """

        with driver.session() as session:
            session.run(cast_request, show_id=data.get('show_id'), cast=data.get('cast'))
            session.run(category_request, show_id=data.get('show_id'), listed_in=data.get('listed_in'))
            session.run(type_request, show_id=data.get('show_id'), type_movie=data.get('type'))
            session.run(director_request, show_id=data.get('show_id'), director=data.get('director'))
            session.run(country_request, show_id=data.get('show_id'), country=data.get('country'))

        print(f"Created relationships for {data.get('title')}")  # Confirmation message
    except Exception as e:
        print(f"Failed to create relationships for {data.get('title')}: {e}")

try:
    print("Starting Kafka consumer...")
    for message in consumer:
        # Process each message
        print(f"Consumed message: {message.value}")
        save_to_neo4j(message.value)  # Save message to Neo4j
        create_relationships(message.value)  # Create relationships

except Exception as e:
    print(f"Error: {e}")

finally:
    consumer.close()
    driver.close()
