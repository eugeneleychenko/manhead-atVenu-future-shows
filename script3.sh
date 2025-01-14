#!/bin/bash

# Set the directory where the CSV files are located
directory="."

# Set the output file name
output_file="combined_sales_with_headings.csv"

# Initialize an empty array to store the CSV files
csv_files=()

# Loop through the files in the directory and add files from (1) to (9) to the array
for i in {1..9}; do
    file="$directory/sales_2024-02-01_2024-02-29 ($i).csv"
    if [[ -f "$file" ]]; then
        csv_files+=("$file")
    fi
done

# Combine the CSV files into a single file
echo "Combining CSV files..."
for file in "${csv_files[@]}"; do
    echo "File: $(basename "$file")" >> "$output_file"
    cat "$file" >> "$output_file"
    echo "" >> "$output_file"  # Add an empty line between files
done

echo "Combined CSV file: $output_file"