from neo4j import GraphDatabase
import time

# Neo4j-Datenbankverbindung herstellen
uri = "bolt://localhost:7687"
username = "neo4j"
password = "password"
driver = GraphDatabase.driver(uri, auth=(username, password))

# Zeit vor der Ausführung der Abfrage erfassen
start_time = time.time()

# Neo4j-Datenbank komplett löschen
delete_query = """
MATCH (n)
DETACH DELETE n
"""

with driver.session() as session:
    session.run(delete_query)

print("Deleted Database")

# Cypher-Abfrage zum Laden der agency
load_agency = """
CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///agency.txt" AS row RETURN row',
  'CREATE (:Agency {
     agency_id: row.agency_id, 
     agency_name: row.agency_name, 
     agency_url: row.agency_url, 
     agency_timezone: row.agency_timezone
  })',
  {batchSize: 5000, parallel: true}
)
"""

with driver.session() as session:
    session.run(load_agency)

print("Finished agency")

# Cypher-Abfrage zum Laden der routes
load_routes = """
CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///routes.txt" AS row RETURN row',
  'MATCH (a:Agency {agency_id: row.agency_id})
   CREATE (a)-[:OPERATES]->(:Route {
     route_id: row.route_id,
     agency_id: row.agency_id, 
     route_short_name: row.route_short_name,
     route_long_name: row.route_long_name
  })',
  {batchSize: 5000, parallel: true}
)
"""

with driver.session() as session:
    session.run(load_routes)

print("Finished routes")

# Cypher-Abfrage zum Laden der trips
load_trips = """
CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///trips.txt" AS row RETURN row',
  'MATCH (r:Route {route_id: row.route_id})
   CREATE (r)<-[:USES]-(:Trip {
     route_id: row.route_id,
     service_id: row.service_id,
     trip_id: row.trip_id, 
     trip_headsign: row.trip_headsign, 
     direction_id: toInteger(row.direction_id),
     trip_short_name: row.trip_short_name
  })',
  {batchSize: 5000, parallel: true}
)
"""

with driver.session() as session:
    session.run(load_trips)

print("Finished trips")

# Cypher-Abfrage zum Laden der stops
load_stops = """
CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///stops.txt" AS row RETURN row',
  'CREATE (:Stop {
     stop_id: row.stop_id, 
     stop_name: row.stop_name, 
     location: point({
        latitude: toFloat(row.stop_lat), 
        longitude: toFloat(row.stop_lon)
     })
  })',
  {batchSize: 5000, parallel: true}
)
"""

with driver.session() as session:
    session.run(load_stops)

print("Finished stops")

# Cypher-Abfrage zum Laden der stop_times
load_stop_times = """
CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///stop_times.txt" AS row RETURN row',
  'MATCH (t:Trip {trip_id: row.trip_id}), (s:Stop {stop_id: row.stop_id})
   CREATE (t)<-[:BELONGS_TO]-(:StopTime {
     trip_id: row.trip_id,
     arrival_time: row.arrival_time, 
     departure_time: row.departure_time,
     stop_id: row.stop_id,
     stop_sequence: toInteger(row.stop_sequence)
   })-[:STOPS_AT]->(s)',
  {batchSize: 5000, parallel: true}
)
"""

with driver.session() as session:
    session.run(load_stop_times)

print("Finished stop_times")

# Cypher-Abfrage zum Erstellen der Roadmap
load_roadmap = """
CALL apoc.periodic.iterate(
  'MATCH (t:Trip) RETURN t',
  'MATCH (t)<-[:BELONGS_TO]-(st) WITH st ORDER BY st.stop_sequence ASC
   WITH collect(st) AS stops
   UNWIND range(0, size(stops)-2) AS i
   WITH stops[i] AS curr, stops[i+1] AS next
   MERGE (curr)-[:NEXT_STOP]->(next)',
  {batchSize: 5000, parallel: true}
)
"""

with driver.session() as session:
    session.run(load_roadmap)

print("Finished roadmap")

# Zeit nach der Ausführung der Abfrage erfassen
end_time = time.time()

# Die Dauer der Abfrage berechnen
execution_time_ms = (end_time - start_time) * 1000  # Umrechnung in Millisekunden

# Ausgabe der Ausführungszeit
print(f"Die Abfrage wurde in {execution_time_ms:.2f} ms ausgeführt.")

print("Finished Upload")

# Schließe die Verbindung zur Datenbank
driver.close()
