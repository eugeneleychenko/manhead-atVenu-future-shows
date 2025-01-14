from bs4 import BeautifulSoup
import csv

with open('table.html', 'r', encoding='utf-8') as file:
    html_table = file.read()

soup = BeautifulSoup(html_table, 'html.parser')

main_table = soup.find('table', class_='table table-condensed')

# Extract the table headers
headers = ['ST', 'Venue', 'Nights', 'Type', 'Capacity', 'Attn.', '$/Head', 'GROSS', 'Show Costs', 'Net Receipts']

# Extract the table data
data = []
for row in main_table.find('tbody').find_all('tr'):
    row_data = []
    for td in row.find_all('td'):
        # Check if the cell contains an input field
        input_field = td.find('input')
        if input_field:
            row_data.append(input_field.get('value', ''))
        else:
            row_data.append(td.text.strip())
    data.append(row_data)

# Create a formatted table string
table_str = ""
# Add headers
table_str += "| " + " | ".join(headers) + " |\n"
table_str += "|" + "-" * (len(table_str) - 2) + "|\n"

# Add data rows
for row in data:
    table_str += "| " + " | ".join(row[:5] + row[5:7] + [row[8], row[10], row[12]]) + " |\n"

# Add footer row
footer_row = main_table.find('tfoot').find('tr')
footer_data = [td.text.strip() for td in footer_row.find_all('td')]
table_str += "| " + " | ".join(footer_data[:2] + footer_data[2:5] + footer_data[7:9] + footer_data[11:12]) + " |\n"

print(table_str)


# Define the CSV file name
csv_file_name = 'extracted_table_data.csv'

# Open the CSV file in write mode
with open(csv_file_name, 'w', newline='', encoding='utf-8') as csvfile:
    # Create a CSV writer object
    csvwriter = csv.writer(csvfile)
    
    # Write the headers to the CSV file
    csvwriter.writerow(headers)
    
    # Write the data rows to the CSV file
    for row in data:
        csvwriter.writerow(row)

print(f"Table data has been successfully written to {csv_file_name}")
