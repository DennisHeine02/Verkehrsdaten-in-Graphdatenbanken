from neo4j import GraphDatabase

# Neo4j-Datenbankverbindung herstellen
uri = "bolt://localhost:7687"  # URI deiner Neo4j-Datenbank
username = "neo4j"  # Benutzername für die Authentifizierung
password = "password"  # Passwort für die Authentifizierung
driver = GraphDatabase.driver(uri, auth=(username, password))

# Cypher-Abfrage, die alle Agency-Knoten mit dem Namen "omnibus verkehr Wangen" abfragt
query = "MATCH (a:Agency) RETURN a"

# Die Abfrage in der Datenbank ausführen und die Ergebnisse ausgeben
with driver.session() as session:
    result = session.run(query)
    for record in result:
        agency = record["a"]
        print(agency["name"])

# Schließe die Verbindung zur Datenbank
driver.close()
