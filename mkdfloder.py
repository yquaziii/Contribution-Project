import os

def create_folders():
    base_folder = "day"
    total_folders = 99
    log_file = "folder_creation_log.txt"

    with open(log_file, 'w') as log:
        for day_number in range(1, total_folders + 1):
            folder_name = f"{base_folder} {day_number}"
            
            # Check if the folder already exists
            if not os.path.exists(folder_name):
                os.makedirs(folder_name)
                log.write(f"Folder '{folder_name}' created.\n")
                print(f"Folder '{folder_name}' created.")
            else:
                log.write(f"Folder '{folder_name}' already exists, skipping.\n")
                print(f"Folder '{folder_name}' already exists, skipping.")

    print(f"Log file '{log_file}' created.")

if __name__ == "__main__":
    create_folders()

