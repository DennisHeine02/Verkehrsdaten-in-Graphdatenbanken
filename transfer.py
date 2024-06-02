import csv
import os

def reorder_columns(input_file, output_file, desired_order):
    with open(input_file, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        
        # Get all unique fieldnames from the input file
        fieldnames = reader.fieldnames
        
        # Add missing columns to the desired order
        for field in desired_order:
            if field not in fieldnames:
                fieldnames.append(field)
        
        # Write the output file with the new column order
        with open(output_file, 'w', newline='', encoding='utf-8') as fw:
            writer = csv.DictWriter(fw, fieldnames=desired_order)
            writer.writeheader()
            
            for row in reader:
                # Ensure missing keys have an empty string value
                ordered_row = {key: row.get(key, '') for key in desired_order}
                writer.writerow(ordered_row)

def process_directory(input_directory, output_directory, file_order_mapping):
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    
    for filename, desired_order in file_order_mapping.items():
        input_file = os.path.join(input_directory, filename)
        output_file = os.path.join(output_directory, filename)
        reorder_columns(input_file, output_file, desired_order)

if __name__ == "__main__":
    input_directory = 'Transfer'
    output_directory = 'Output'
    
    file_order_mapping = {
        'stops.txt': ['stop_id', 'stop_name', 'stop_lat', 'stop_lon'],
        'agency.txt': ['agency_id', 'agency_name', 'agency_url', 'agency_timezone'],
        'routes.txt': ['route_id', 'agency_id', 'route_short_name', 'route_long_name'],
        'trips.txt': ['route_id', 'service_id', 'trip_id', 'trip_headsign', 'direction_id', 'trip_short_name'], 
        'stop_times.txt': ['trip_id', 'arrival_time', 'departure_time', 'stop_id', 'stop_sequence']
    }
    
    process_directory(input_directory, output_directory, file_order_mapping)
