import os
from collections import defaultdict

def count_lines_in_directory(directory):
    # Definiere die zu berücksichtigenden Dateien
    target_files = {"agency.txt", "routes.txt", "trips.txt", "stop_times.txt", "stops.txt"}
    # Verwende ein defaultdict, um die Zeilenzahlen zu speichern
    individual_counts = defaultdict(list)
    total_counts = defaultdict(int)
    
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file in target_files:
                filepath = os.path.join(root, file)
                try:
                    with open(filepath, 'r') as f:
                        # Zeilen zählen und die Kopfzeile abziehen
                        line_count = sum(1 for line in f) - 1
                        individual_counts[file].append((os.path.basename(root), line_count))
                        total_counts[file] += line_count
                except Exception as e:
                    print(f"Fehler beim Lesen von {file}: {str(e)}")
    
    return individual_counts, total_counts

if __name__ == "__main__":
    directory = "Daten"
    individual_counts, total_counts = count_lines_in_directory(directory)
    
    with open("results_count.txt", 'w') as f:
        # Ergebnisse der einzelnen Dateien in den Unterordnern ausgeben
        f.write("Einzelne Zählungen:\n")
        for file, counts in individual_counts.items():
            f.write(f"------------------\n{file}\n------------------\n")
            for folder, count in counts:
                f.write(f"{folder}_{file}: {count} Zeilen\n")
        
        # Gesamtergebnisse Ordner übergreifend ausgeben
        f.write("\nGesamte Zählungen:\n")
        for file, count in total_counts.items():
            f.write(f"{file}: {count} Zeilen\n")
