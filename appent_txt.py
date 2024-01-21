
import os
from datetime import datetime

def append_txt_files(directory_path, output_file):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(output_file, 'a') as output_file:
        output_file.write(f"\n\n--- Timestamp: {timestamp} ---\n\n")
        
        # Iterate through each file in the specified directory
        for filename in os.listdir(directory_path):
            if filename.endswith(".txt"):
                file_path = os.path.join(directory_path, filename)
                with open(file_path, 'r') as input_file:
                    output_file.write(f"=== File: {filename} ===\n")
                    output_file.write(input_file.read())
                    output_file.write("\n\n")

if __name__ == "__main__":
    # Specify the directory containing the .txt files
    input_directory = "/Users/yquazi/Desktop"

    # Specify the output file
    output_file = "output_combined.txt"

    append_txt_files(input_directory, output_file)
    print(f"Text files appended into {output_file} with timestamp entries.")
