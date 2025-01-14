#!/bin/bash

# Set the directory where the CSV files are located
directory="."

# Set the output file name
output_file="combined_sales.csv"

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
head -n 1 "${sorted_csv_files[0]}" > "$output_file"  # Copy header from the first file
tail -q -n +2 "${sorted_csv_files[@]}" >> "$output_file"  # Append data from all files

echo "Combined CSV file: $output_file"