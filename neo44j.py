from neo4j import GraphDatabase

# Update the URI to use self-signed SSL
uri = "neo4j+ssc://3f3cafed.databases.neo4j.io:7687"
username = "neo4j"
password = "GicxwhcKtwFR19Liv4rJtUdB3mI7Qfur51AsVbIiatc"

# Create a driver instance
driver = GraphDatabase.driver(uri, auth=(username, password))

try:
    with driver.session() as session:
        # Run a basic query
        result = session.run("RETURN 1 AS number")
        print(result.single())
except Exception as e:
    print(f"Error: {e}")



with driver.session() as session:
    session.run("CREATE (a:Person {name: 'Alice'})")
    session.run("CREATE (b:Person {name: 'Bob'})")
    session.run("CREATE (a)-[:KNOWS]->(b)")
