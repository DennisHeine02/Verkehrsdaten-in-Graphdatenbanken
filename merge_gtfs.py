import pandas as pd
import time
import os

excluded_folder = '20231030_fahrplaene_gesamtdeutschland_gtfs'

double_file = 'double_agency.txt'

double_route_file = 'double_routes.txt'

double_trip_file = 'double_trips.txt'

double_stop_times_file = 'double_stop_times.txt'

double_stops_file = 'double_stops.txt'

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

sub_Folder = [f.path for f in os.scandir('daten') if f.is_dir() and f.name != excluded_folder]

count_all_doubles = 0

count_all_added = 0

all_start_time = time.time()

for folder in sub_Folder:
    file2 = os.path.join(folder, 'agency.txt')
    file2_routes = os.path.join(folder, 'routes.txt')
    file2_trips = os.path.join(folder, 'trips.txt')
    file2_stop_times = os.path.join(folder, 'stop_times.txt')
    file2_stops = os.path.join(folder, 'stops.txt')

    # Load the second file into a DataFrame
    df2 = pd.read_csv(file2)
    df2_routes = pd.read_csv(file2_routes)
    df2_trips = pd.read_csv(file2_trips)
    df2_stop_times = pd.read_csv(file2_stop_times)
    df2_stops = pd.read_csv(file2_stops)

    temp = []
    temp_routes = []
    temp_trips = []
    temp_stop_times = []
    temp_stops = []
    vorhanden = False
    vorhanden_routes = False
    vorhanden_trips = False
    vorhanden_stop_times = False
    vorhanden_stops = False
    count_doubels = 0
    count_doubel_routes = 0
    count_double_trips = 0
    count_double_stop_times = 0
    count_double_stops = 0

    start_time = time.time()

    # agency überprüfung
    
    with open(double_file, 'a') as dd:
        
        for df2_index, df2_row in df2.iterrows():
            new_agency = df2_row.values[1]
            
            for df1_index, df1_row in df1.iterrows():
                vorhanden_agency = df1_row.values[1]
                
                # wenn die beiden namen der agency gleich sind
                if vorhanden_agency == new_agency:
                    
                    old_id = df2_row.values[0]
                    
                    # routes überprüfung
                    
                    with open (double_route_file, "a") as dr:
                        
                        print("routes überprüfung startet")
                        
                        for df2_routes_index, df2_routes_row in df2_routes.iterrows():
                            
                            if old_id == df2_routes_row.values[1]:
                                
                                temp_route_id = df2_routes_row.values[0]
                                temp_agency_id = df2_routes_row.values[1]
                                temp_route_short_name = df2_routes_row.values[2]
                                
                                for df1_routes_index, df1_routes_row in df1_routes.iterrows():
                                    
                                    if temp_route_id == df1_routes_row.values[0] and temp_agency_id == df1_routes_row.values[1] and temp_route_short_name == df1_routes_row.values[2]:
                                        
                                        old_route_id = df2_routes_row.values[0]
                                        
                                        # trips überprüfung
                                        
                                        with open (double_trip_file, "a") as dt:
                                            
                                            print("trips überprüfung startet")
                                            
                                            for df2_trips_index, df2_trips_row in df2_trips.iterrows():
                                                
                                                if old_route_id == df2_trips_row.values[0]:
                                                    
                                                    temp_route_id = df2_trips_row.values[0]
                                                    temp_service_id = df2_trips_row.values[1]
                                                    temp_trip_id = df2_trips_row.values[2]
                                                    temp_trip_headsign = df2_trips_row.values[3]
                                                    
                                                    for df1_trips_index, df1_trips_row in df1_trips.iterrows():
                                                        
                                                        if temp_trip_id == df1_trips_row.values[2] and temp_route_id == df1_trips_row.values[0] and temp_service_id == df1_trips_row.values[1] and temp_trip_headsign == df1_trips_row.values[3]:
                                                            
                                                            old_trip_id = df2_trips_row.values[2]
                                                            
                                                            # stop_times überprüfung
                                                            
                                                            with open (double_stop_times_file, "a") as dst:
                                                                    
                                                                    print ("stop_times überprüfung startet")
                                                                    
                                                                    for df2_stop_times_index, df2_stop_times_row in df2_stop_times.iterrows():
                                                                        
                                                                        if old_trip_id == df2_stop_times_row.values[0]:
                                                                            
                                                                            temp_trip_id = df2_stop_times_row.values[0]
                                                                            temp_arrival_time = df2_stop_times_row.values[1]
                                                                            temp_departure_time = df2_stop_times_row.values[2]
                                                                            temp_stop_id = df2_stop_times_row.values[3]
                                                                            
                                                                            for df1_stop_times_index, df1_stop_times_row in df1_stop_times.iterrows():
                                                                                
                                                                                if temp_trip_id == df1_stop_times_row.values[0] and temp_arrival_time == df1_stop_times_row.values[1] and temp_departure_time == df1_stop_times_row.values[2] and temp_stop_id == df1_stop_times_row.values[3] and temp_stop_sequence == df1_stop_times_row.values[4]:
                                                                                    
                                                                                    old_stop_id = df2_stop_times_row.values[3]
                                                                                    
                                                                                    # stops überprüfung
                                                                                    
                                                                                    with open (double_stops_file, "a") as ds:
                                                                                        
                                                                                        print ("stops überprüfung startet")
                                                                                        
                                                                                        for df2_stops_index, df2_stops_row in df2_stops.iterrows():
                                                                                            
                                                                                            if old_stop_id == df2_stops_row.values[0]:
                                                                                                
                                                                                                temp_stop_id = df2_stops_row.values[0]
                                                                                                temp_stop_name = df2_stops_row.values[2]
                                                                                                temp_stop_lat = df2_stops_row.values[4]
                                                                                                temp_stop_lon = df2_stops_row.values[5]
                                                                                                
                                                                                                for df1_stops_index, df1_stops_row in df1_stops.iterrows():
                                                                                                    
                                                                                                    if temp_stop_id == df1_stops_row.values[0] and temp_stop_name == df1_stops_row.values[2] and temp_stop_lat == df1_stops_row.values[4] and temp_stop_lon == df1_stops_row.values[5]:
                                                                                    
                                                                                                        ds.write(f"\"{temp_stop_id}\",\"\",\"{temp_stop_name}\",,\"{temp_stop_lat}\",\"{temp_stop_lon}\"\n")
                                                                                                        count_double_stops += 1
                                                                                                        vorhanden_stops = True
                                                                                                        break
                                                                                                    
                                                                                                    else:
                                                                                                        pass
                                                                                                    
                                                                                            if vorhanden_stops != True:
                                                                                                            
                                                                                                temp_stops.append(f"\"{temp_stop_id}\",\"\",\"{temp_stop_name}\",,\"{temp_stop_lat}\",\"{temp_stop_lon}\"\n")
                                                                                                            
                                                                                            if vorhanden_stops == True:
                                                                                                vorhanden_stops = False
                                                                                                                                                                                            
                                                                                    ds.close()
                                                                                    
                                                                                    print("stop überprüfung abgeschlossen")
                                                                                    
                                                                                    # stop_times überprüfung
                                                                                
                                                                                    dst.write(f"\"{temp_trip_id}\",\"{temp_arrival_time}\",\"{temp_departure_time}\",\"{temp_stop_id}\"\n")
                                                                                    count_double_stop_times += 1
                                                                                    vorhanden_stop_times = True
                                                                                    break
                                                                                
                                                                            else:
                                                                                pass
                                                                                
                                                                    if vorhanden_stop_times != True:
                                                                                        
                                                                        temp_stop_times.append(f"\"{temp_trip_id}\",\"{temp_arrival_time}\",\"{temp_departure_time}\",\"{temp_stop_id}\"\n")
                                                                                    
                                                                    if vorhanden_stop_times == True:
                                                                        vorhanden_stop_times = False
                                                                                    
                                                            dst.close()
                                                            
                                                            print("stop_times überprüfung abgeschlossen")
                                                            
                                                            # trips überprüfung                
                                                            
                                                            dt.write(f"\"{temp_route_id}\",\"{temp_service_id}\",\"{temp_trip_id}\",\"{temp_trip_headsign}\"\n")
                                                            count_double_trips += 1
                                                            vorhanden_trips = True
                                                            break
                                                        
                                                        else:
                                                            pass
                                                    
                                                    if vorhanden_trips != True:
                                                            
                                                        temp_trips.append(f"\"{temp_route_id}\",\"{temp_service_id}\",\"{temp_trip_id}\",\"{temp_trip_headsign}\"\n")
                                                            
                                                    if vorhanden_trips == True:
                                                        vorhanden_trips = False
                                                      
                                        dt.close()
                                        print("trips überprüfung abgeschlossen")
                                        
                                        # routes überprüfung
                                        
                                        dr.write(f"\"{temp_route_id}\",\"{temp_agency_id}\",\"{temp_route_short_name}\"\n")
                                        count_doubel_routes = 0
                                        vorhanden_routes = True
                                        break
                                    
                                    else: 
                                        pass
                                    
                                if vorhanden_routes != True:
                                        
                                    temp_routes.append(f"\"{temp_route_id}\",\"{temp_agency_id}\",\"{temp_route_short_name}\"\n")
                                    
                                if vorhanden_routes == True:
                                    vorhanden_routes = False
   
                    dr.close()
                    print("route überprüfung abgeschlossen")
                    
                    # agency überprüfung
                    
                    
                    
                    # Hinzufügem der doppelten Agenturen in eine Datei
                    temp_agency_id = df2_row.values[0]
                    temp_agency_name = df2_row.values[1]
                    temp_agency_url = df2_row.values[2]
                    temp_agency_timezone = df2_row.values[3]
                    
                    dd.write(f"\"{temp_agency_id}\",\"{temp_agency_name}\",\"{temp_agency_url}\",\"{temp_agency_timezone}\"\n")
                    count_doubels += 1
                    vorhanden = True
                    break
                
                else:
                    pass
            
            if vorhanden != True:
                
                temp.append(f"\"{temp_agency_id}\",\"{temp_agency_name}\",\"{temp_agency_url}\",\"{temp_agency_timezone}\"\n")
            
            if vorhanden == True:
                vorhanden = False
                    
    dd.close()

    count_added = 0
    with open(file_agency, 'r+') as f:
        f.seek(0, 2)
        for i in temp:
            f.write(i)
            count_added += 1
        f.close()
    
    with open(file_routes, 'r+') as fr:
        fr.seek(0, 2)
        for i in temp_routes:
            fr.write(i)
        fr.close()
    
    with open(file_trips, 'r+') as ft:
        ft.seek(0, 2)
        for i in temp_trips:
            ft.write(i)
        ft.close()
        
    with open(file_stop_times, 'r+') as fst:
        fst.seek(0, 2)
        for i in temp_stop_times:
            fst.write(i)
        fst.close()
        
    with open(file_stops, 'r+') as fs:
        fs.seek(0, 2)
        for i in temp_stops:
            fs.write(i)
        fs.close()

    end_time = time.time()

    execution_time = (end_time - start_time) * 1000

    print("Finished folder: " + folder + ".")

    print(f"Execution time: {execution_time} ms")
    
    
    print(f"Added {count_added} new agencies to {file_agency}.")
    
    count_all_added += count_added

    print(f"Found {count_doubels} double agencies in {file_agency} and {file2}.")
    
    count_all_doubles += count_doubels
    
    print("--------------------------------------------------")

all_end_time = time.time()

print("Finished all folders.")

print(f"Execution time: {(all_end_time - all_start_time) * 1000} ms")

print(f"Added {count_all_added} new agencies to {file_agency}.")

print(f"Found {count_all_doubles} double agencies in all folders.")
