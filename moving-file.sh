#!/bin/bash

# Define source and destination directories
source_dir="/Users/yquazi/Desktop/repro/temp/estone"
destination_dir="/Users/yquazi/Desktop/repro/temp/estone2"

# Create the destination directory if it doesn't exist
mkdir -p "$destination_dir"

# Use a counter to keep track of how many files have been moved
count=0

# Loop through files in the source directory
for file in "$source_dir"/*; do
    # Check if the file count has reached 50
    if [ "$count" -eq 50 ]; then
        break
    fi

    # Move the file to the destination directory
    mv "$file" "$destination_dir/"

    # Increment the counter
    ((count++))
done

echo "Moved $count files from $source_dir to $destination_dir"
