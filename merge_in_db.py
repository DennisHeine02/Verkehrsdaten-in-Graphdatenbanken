import pandas as pd
import time
import os
from neo4j import GraphDatabase

# Variablen um zu sehen wie viele Daten hinzugefügt wurden
added_agencies = 0
added_routes = 0
added_trips = 0
added_stop_times = 0
added_stop = 0

def new_id(old_id):
    return f'n:{old_id}'

def create_agency(agency_id, agency_name, agency_url, agency_timezone):
    with driver.session() as session:
        result = session.run("""
            CREATE (a:Agency {
                agency_id: $agency_id,
                agency_name: $agency_name,
                agency_url: $agency_url,
                agency_timezone: $agency_timezone
            })
        """, agency_id=agency_id, agency_name=agency_name, agency_url=agency_url, agency_timezone=agency_timezone)
    global added_agencies
    added_agencies += 1
    driver.close()
    
def create_route(route_id, agency_id, route_short_name, route_long_name):
    with driver.session() as session:
        result = session.run("""
            MATCH (a:Agency {agency_id: $agency_id})
            CREATE (a)-[:OPERATES]->(r:Route {
                route_id: $route_id,
                agency_id: $agency_id,
                route_short_name: $route_short_name,
                route_long_name: $route_long_name
            })
        """, route_id=route_id, agency_id=agency_id, route_short_name=route_short_name, route_long_name=route_long_name)
    global added_routes 
    added_routes += 1
    driver.close()

def create_route_without_agency(route_id, agency_id, route_short_name, route_long_name):
    route_id = new_id(route_id)
    with driver.session() as session:
        result = session.run("""
            MATCH (t:Trip {trip_id: $trip_id}, (a:Agency {agency_id: $agency_id})
            CREATE (t)-[:USES]->(r:Route {
                route_id: $route_id,
                agency_id: $agency_id,
                route_short_name: $route_short_name,
                route_long_name: $route_long_name
            })<-[:OPERATES]-(a)
        """, route_id=route_id, agency_id=agency_id, route_short_name=route_short_name, route_long_name=route_long_name)
    global added_routes 
    added_routes += 1
    driver.close()
    
def create_trip(route_id, service_id, trip_id, trip_headsign, direction_id, trip_short_name):
    with driver.session() as session:
        result = session.run("""
            MATCH (r:Route {route_id: $route_id})
            CREATE (r)<-[:USES]-(t:Trip {
                route_id: $route_id,
                serviceId: $service_id,
                trip_id: $trip_id,
                trip_headsign: $trip_headsign,
                direction_id: $direction_id
                trip_short_name: §trip_short_name
            })
        """, route_id=route_id, service_id=service_id, trip_id=trip_id, trip_headsign=trip_headsign, direction_id=direction_id, trip_short_name=trip_short_name)
    global added_trips 
    added_trips += 1
    driver.close()
    
def create_trip_without_route(route_id, service_id, trip_id, trip_headsign, direction_id, trip_short_name):
    route_id = new_id(route_id)
    trip_id = new_id(trip_id)
    with driver.session() as session:
        result = session.run("""
            MATCH (st:Stop_time {route_id: $route_id})                 
            CREATE (st)-[:BELONGS_TO]->(t:Trip {
                route_id: $route_id,
                serviceId: $service_id,
                trip_id: $trip_id,
                trip_headsign: $trip_headsign,
                direction_id: $direction_id,
                trip_short_name: §trip_short_name
            })
        """, trip_id=trip_id, service_id=service_id, trip_id=trip_id, trip_headsign=trip_headsign, direction_id=direction_id, trip_short_name=trip_short_name)
    global added_trips 
    added_trips += 1
    driver.close()

def create_stop_time(trip_id, arrival_time, departure_time, stop_id, stop_sequence):
    with driver.session() as session:
        result = session.run("""
            MATCH (t:Trip {trip_id: $trip_id}), (s:Stop {stop_id: $stop_id})
            CREATE (t)<-[:BELONGS_TO]-(st:StopTime {
                trip_id: $trip_id,
                arrival_time: $arrival_time,
                departure_time: $departure_time,
                stop_id: $stop_id,
                stop_sequence: toInteger($stop_sequence)
            })-[:STOPS_AT]->(s)
        """, trip_id=trip_id, arrival_time=arrival_time, departure_time=departure_time, stop_id=stop_id, stop_sequence=stop_sequence)
    global added_stop_times
    added_stop_times += 1
    driver.close()
    
def create_stop_time_without_trip(trip_id, arrival_time, departure_time, stop_id, stop_sequence):
    trip_id = new_id(trip_id)
    stop_id = new_id(stop_id)
    with driver.session() as session:
        result = session.run("""
            MATCH (s:Stop {stop_id: $stop_id})
            CREATE (st:StopTime {
                trip_id: $trip_id,
                arrival_time: $arrival_time,
                departure_time: $departure_time,
                stop_id: $stop_id,
                stop_sequence: toInteger($stop_sequence)
            })-[:STOPS_AT]->(s)
        """, trip_id=trip_id, arrival_time=arrival_time, departure_time=departure_time, stop_id=stop_id, stop_sequence=stop_sequence)
    global added_stop_times
    added_stop_times += 1
    driver.close()
    
def create_stop(stop_id, stop_name, latitude, longitude):
    with driver.session() as session:
        result = session.run("""
            CREATE (s:Stop {
            stop_id: $stop_id, 
            stop_name: $stop_name, 
            location: point({
                latitude: $latitude, 
                longitude: $longitude
            })
        });
            """, stop_id=stop_id, stop_name=stop_name, latitude=latitude, longitude=longitude)
    global added_stop
    added_stop += 1
    driver.close()
    
def search_all_agencys():
    agency_list = []
    with driver.session() as session:
        result = session.run("""
            MATCH (a:Agency)
            RETURN a
        """)
        for record in result:
            agency = record["a"]
            agency_list.append(agency)
    driver.close()
    return agency_list

def search_routes(agency_id):
    routes_list = []
    with driver.session() as session:
        result = session.run("""
            MATCH (a:Agency {agency_id: $agency_id})-[:OPERATES]->(r:Route)
            RETURN r
        """, agency_id=agency_id)
        for record in result:
            agency = record["r"]
            routes_list.append(agency)
    driver.close()
    return routes_list

# Funktion zum Suchen aller Trips für eine bestimmte Route
def search_trips(route_id):
    trips_list = []
    with driver.session() as session:
        result = session.run("""
            MATCH (r:Route {route_id: $route_id})<-[:USES]-(t:Trip)
            RETURN t
        """, route_id=route_id)
        for record in result:
            trip = record["t"]
            trips_list.append(trip)
    driver.close()
    return trips_list

# Funktion zum Suchen aller Stop Times für einen bestimmten Trip
def search_stop_times(trip_id):
    stop_times_list = []
    with driver.session() as session:
        result = session.run("""
            MATCH (t:Trip {trip_id: $trip_id})<-[:BELONGS_TO]-(st:StopTime)
            RETURN st
        """, trip_id=trip_id)
        for record in result:
            stop_time = record["st"]
            stop_times_list.append(stop_time)
    driver.close()
    return stop_times_list

# Funktion zum Suchen aller Stops für eine bestimmte Stop Time
def search_stops(stop_id):
    stops_list = []
    with driver.session() as session:
        result = session.run("""
            MATCH (s:Stop {stop_id: $stop_id})
            RETURN s
        """, stop_id=stop_id)
        temp = result.single()
        stop = temp["s"]
    driver.close()
    return stop

def search_stop_by_coordinates(latitude, longitude, uri="bolt://localhost:7687", username="neo4j", password="password"):

    with driver.session() as session:
        result = session.run("""
            MATCH (s:Stop)
            WHERE s.location.latitude = $latitude AND s.location.longitude = $longitude
            RETURN s
        """, latitude=latitude, longitude=longitude)
        temp = result.single()
        stop = temp["s"]
    driver.close()
    return stop


# Verbindung zur Neo4j-Datenbank herstellen
uri = "bolt://localhost:7687"  # URI der Neo4j-Datenbank
username = "neo4j"  # Benutzername für die Authentifizierung
password = "password"  # Passwort für die Authentifizierung
driver = GraphDatabase.driver(uri, auth=(username, password))

main_Folder = "Main-Data"

file_agency = os.path.join(main_Folder, 'agency.txt')
file_routes = os.path.join(main_Folder, 'routes.txt')
file_trips = os.path.join(main_Folder, 'trips.txt')
file_stop_times = os.path.join(main_Folder, 'stop_times.txt')
file_stops = os.path.join(main_Folder, 'stops.txt')

df1 = pd.read_csv(file_agency)
df1_routes = pd.read_csv(file_routes)
df1_trips = pd.read_csv(file_trips)
df1_stop_times = pd.read_csv(file_stop_times)
df1_stops = pd.read_csv(file_stops)
    
agency_found = False

found_doubles = 0

# Durch jede Zeile des DataFrames iterieren
for index, df_row in df1.iterrows():
    
    # Überprüfen, ob der Wert in der zweiten Spalte gleich einem bestimmten Wert ist
    agency_list = search_all_agencys()
    
    for agency in agency_list:
        
        if df_row.values[1] == agency['agency_name']:
            
            found_doubles += 1
            agency_found = True
            double_agency = agency['agency_id']
            double_agency_id = df_row.values[0]
            # print(f"Agency ID: {double_agency}")
            
            for index, df_row_routes in df1_routes.iterrows():
                
                if df_row_routes.value[1] == double_agency_id:
                    
                    neu = False
                    neu_trip = False
                    neu_stop_time = False
                    neu_stop = False
                    
                    double_routes_id = df_row_routes.values[0]
                    
                    for index, df_row_trips in df1_trips.iterrows():
                        
                        if df_row_trips.value[0] == double_routes_id:
                            
                            double_trip_id = df_row_trips.values[2]
                            
                            for index, df_row_stop_times in df1_stop_times.iterrows():
                                
                                if df_row_stop_times.values[0] == double_trip_id:
                                    
                                    double_stop_id = df_row_stop_times.values[3]
                                    
                                    for index, df_row_stops in df1_stops.iterrows():
                                        
                                        if df_row_stops.values[0] == double_stop_id:
                                            
                                            stop_db = search_stop_by_coordinates(df_row_stops.values[2], df_row_stops.values[3])
                                            temp = stop_db["location"]
                                            long = temp[0]
                                            lat = temp[1]
                                            
                                            if lat == df_row_stops.values[2] and long == df_row_stops.values[3]:
                                                pass
                                            else:
                                                new_stop_id = new_id(df_row_stops.values[0])
                                                create_stop(new_stop_id, df_row_stops.values[1], df_row_stops.values[2], df_row_stops.values[3])
                                                neu_stop = True
                                                break
                                    if neu_stop == True:
                                        create_stop_time_without_trip(df_row_stop_times.values[0], df_row_stop_times.values[1], df_row_stop_times.values[2], new_stop_id, df_row_stop_times.values[4])
                                        neu_stop_time = True
                                        neu = False
                        
                            if neu_stop_time == True:
                                create_trip_without_route(df_row_trips.values[0], df_row_trips.values[1], df_row_trips.values[2], df_row_trips.values[3], df_row_trips.values[4], df_row_trips.values[5])
                                neu_trip = True
                                neu_stop_time = False
                    if neu_trip == True:
                        create_route_without_agency(df_row_routes.values[0], df_row_routes.values[1], df_row_routes.values[2], df_row_routes.values[3])
            
            # routes_list = search_routes(double_agency)
            
            # for routes in routes_list:
            #     double_routes = routes['route_id']
            #     # print(f"Routes ID: {double_routes}")
                
            #     trips_list = search_trips(double_routes)
                
            #     for trip in trips_list:
            #         double_trip = trip['trip_id']
            #         # print(f"Trip ID: {double_trip}")
                    
            #         stop_times_list = search_stop_times(double_trip)
                    
            #         for stop_time in stop_times_list:
            #             double_trip_id = stop_time['trip_id']
            #             double_stop_id = stop_time['stop_id']
            #             # print(f"Processing stop times for Stop Time ID: {double_stop_id} and Trip ID: {double_trip_id}")
                        
            #             stop = search_stops(double_stop_id)
                        
            #             stopee = stop["stop_name"]
            #             temp = stop["location"]
            #             long = temp[0]
            #             lat = temp[1]
                        
            #             # print(f"Stop ID with {stopee} found and location is {lat}")
        
        else:
            pass
    
    if agency_found == False:
        print(f"{df_row.values[1]} not found")
        create_agency(df_row.values[0], df_row.values[1], df_row.values[2], df_row.values[3])
        double_agency = df_row.values[0]
        
        for index, df_row_routes in df1_routes.iterrows():
            if df_row_routes.values[1] == double_agency:
                # print(f"Routes ID: {df_row_routes.values[0]}")
                
                create_route(df_row_routes.values[0], df_row_routes.values[1], df_row_routes.values[2], df_row_routes.values[3])
                double_routes = df_row_routes.values[0]
                
                for index, df_row_trips in df1_trips.iterrows():
                    if df_row_trips.values[0] == double_routes:
                        # print(f"Trip ID: {df_row_trips.values[2]}")
                        
                        create_trip(df_row_trips.values[0], df_row_trips.values[1], df_row_trips.values[2], df_row_trips.values[3], df_row_trips.values[4], df_row_trips.values[5])
                        double_trip_id = df_row_trips.values[2]
                        
                        for index, df_row_stop_times in df1_stop_times.iterrows():
                            if df_row_stop_times.values[0] == double_trip_id:
                                # print(f"Processing stop times for Stop Time ID: {df_row_stop_times.values[3]} and Trip ID: {df_row_trips.values[2]}")
                                
                                # create_stop_time hier noch nicht da erst stops erstellen
                                
                                double_stop_id = df_row_stop_times.values[3]
                                
                                for index, df_row_stops in df1_stops.iterrows():
                                    if df_row_stops.values[0] == double_stop_id:
                                        
                                        # print(f"Stop ID with {df_row_stops.values[1]} found and location is {df_row_stops.values[3]}")
                                        
                                        stop_db = search_stop_by_coordinates(df_row_stops.values[2], df_row_stops.values[3])
                                        
                                        temp = stop_db["location"]
                                        long = temp[0]
                                        lat = temp[1]   
                                                                            
                                        if lat == df_row_stops.values[2] and long == df_row_stops.values[3]:
                                            pass
                                        else:
                                            create_stop(new_id(df_row_stops.values[0]), df_row_stops.values[1], df_row_stops.values[2], df_row_stops.values[3])
                                            create_stop_time(new_id(df_row_stop_times.values[0]), df_row_stop_times.values[1], df_row_stop_times.values[2], new_id(df_row_stop_times.values[3]), df_row_stop_times.values[4])
                                            break
                                        create_stop_time(new_id(df_row_stop_times.values[0]), df_row_stop_times.values[1], df_row_stop_times.values[2], stop_db["stop_id"], df_row_stop_times.values[4])
        print(f"Added {df_row.values[1]}")
    if agency_found == True:
        agency_found = False

print(f"Added {added_agencies} agencies and {added_routes} routes and {added_trips} trips and {added_stop_times} stop times and {added_stop} stops")
print(f"Found {found_doubles} double Agency")
        

                    
                    
                
            