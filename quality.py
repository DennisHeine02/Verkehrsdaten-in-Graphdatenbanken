import os
import pandas as pd
import time

main_Folder = 'Output'

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

start_time = time.time()
# Überprüfung der Agency ID in Routes
for index_routes, row_routes in df1_routes.iterrows():
    
    if row_routes.values[1] != None:
    
        agency_id = row_routes.values[1]
        
        found = False
        
        for index, row in df1.iterrows():
            
            if agency_id == row.values[0]:
                
                found = True
                break
            else:
                continue
        
        if found == False:
            print("Agency ID not found: " + agency_id)
    else:
        print(f"Agency ID in Line {index_routes} is None")

end_time = time.time()

print("Finished Agency ID Time elapsed: " + str(end_time - start_time))

start_time = time.time()
        
# Überprüfung der Route ID in Trips
for index_trips, row_trips in df1_trips.iterrows():
    
    if row_trips.values[0] != None:
    
        route_id = row_trips.values[0]
        
        found = False
        
        for index, row in df1_routes.iterrows():
            
            if route_id == row.values[0]:
                
                found = True
                break
            else:
                continue
        
        if found == False:
            print("Route ID not found: " + route_id)
    else:
        print(f"Route ID in Line {index_trips} is None")

end_time = time.time()

print("Finished Route ID Time elapsed: " + str(end_time - start_time))

start_time = time.time()

# Überprüfung der Trip ID in Stop Times
for index_stop_times, row_stop_times in df1_stop_times.iterrows():
    
    if row_stop_times.values[0] != None:
    
        trip_id = row_stop_times.values[0]
        
        found = False
        
        for index, row in df1_trips.iterrows():
            
            if trip_id == row.values[2]:
                
                found = True
                break
            else:
                continue
        
        if found == False:
            print("Trip ID not found: " + trip_id)
    else:
        print(f"Trip ID in Line {index_stop_times} is None")

end_time = time.time()

print("Finished Trip ID Time elapsed: " + str(end_time - start_time))

start_time = time.time()

# Überprüfung der Stop ID in Stop Times
for index_stop_times, row_stop_times in df1_stop_times.iterrows():
    
    if row_stop_times.values[3] != None:
    
        stop_id = row_stop_times.values[3]
        
        found = False
        
        for index, row in df1_stops.iterrows():
            
            if stop_id == row.values[0]:
                
                found = True
                break
            else:
                continue
        
        if found == False:
            print("Stop ID not found: " + stop_id)
    else:
        print(f"Stop ID in Line {index_stop_times} is None")

end_time = time.time()

print("Finished Stop ID Time elapsed: " + str(end_time - start_time))

print("Überprüfung abgeschlossen.")
