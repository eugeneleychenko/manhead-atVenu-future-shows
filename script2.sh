#!/bin/bash

# Set the directory where the CSV files are located
directory="."

# Set the output file name
output_file="combined_sales_with_headings.csv"

# Initialize an empty array to store the CSV files
csv_files=()

# Loop through the files in the directory
for file in "$directory"/sales_2024-02-01_2024-02-29*.csv; do
    # Add each file to the array
    csv_files+=("$file")
done

# Sort the array to ensure the files are processed in the correct order
IFS=$'\n' sorted_csv_files=($(sort <<<"${csv_files[*]}"))

# Combine the CSV files into a single file
echo "Combining CSV files..."
for file in "${sorted_csv_files[@]}"; do
    echo "File: $(basename "$file")" >> "$output_file"
    cat "$file" >> "$output_file"
    echo "" >> "$output_file"  # Add an empty line between files
done

echo "Combined CSV file: $output_file"