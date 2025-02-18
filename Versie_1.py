import sqlite3
import pandas as pd
import re
import os
from IPython.display import display

def count_brackets(query):
    return query.count('(') + query.count(')')

def count_tables(query, table_list):
    return len(set(table for table in table_list if table in query.lower()))

def extract_queries_and_timestamps(file_path):
    with open(file_path, 'r') as file:
        log_content = file.read()
    log_content = re.sub(r'/\*.*?\*/', '', log_content, flags=re.DOTALL)
    pattern = r'# (\w+, \d+ \w+ \d+ \d+:\d+:\d+).*?\n.*?query(\d+_\d+) = \'\'\'(.*?)\'\'\'' 
    matches = re.findall(pattern, log_content, re.DOTALL)
    data = []
    table_names = ['store', 'shoppinglist', 'purchase', 'product', 'inventory', 'customer']
    for match in matches:
        timestamp = ' '.join(match[0].split(', ')[1:])
        query_name = match[1]
        query = match[2].strip()
        if query and query != 'PUT YOUR QUERY HERE' and not query.isspace() and '\\' not in query:
            bracket_count = count_brackets(query)
            table_count = count_tables(query, table_names)
            data.append((timestamp, query, query_name, bracket_count, table_count))
    return data

def count_keywords(query):
    keywords = ['where', 'group by', 'join','having', 'distinct']
    return {keyword: query.lower().count(keyword) for keyword in keywords}

# Function to categorize the error type
def categorize_error(error_message):
    if not error_message:
        return "No Error"

    error_message = error_message.lower()  # Normalize the error message for consistency

    # Syntax Errors
    syntax_keywords = [
        "syntax", "unrecognized token", "incomplete input", "no such", 
        "not found", "wrong number of arguments", "misuse of aggregate", 
        "circular reference", "you can only execute one statement at a time",
        "misuse of aliased aggregate", "row value misused"
    ]
    if any(keyword in error_message for keyword in syntax_keywords):
        return "Syntax Error"

    # Logic Errors
    logic_keywords = [
        "ambiguous column name", "sub-select returns", "selects to the left and right of", 
        "a join clause is required before on", "having clause on a non-aggregate query", 
        "in(...) element has", "table missing", "no tables specified", "foreign key constraint", 
        "constraint fails", "already exists", "has 2 values for 1 columns", "has 3 values for 1 columns", 
        "has 1 values for 2 columns"
    ]
    if any(keyword in error_message for keyword in logic_keywords):
        return "Logic Error"

    # Conceptual Errors
    conceptual_keywords = ["misuse of aggregate function"]
    if any(keyword in conceptual_keywords for keyword in error_message):
        return "Conceptual Error"

    return "Unknown Error"

# Connect to an in-memory SQLite database
db_path = 'C:/Users/NihalBoukhoubza/OneDrive/Scriptie/shoppingDB_sqlite.sqlite'  # Voeg het juiste pad toe aan je databasebestand
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Path to the main directory containing folders
base_directory_path = 'C:/Users/NihalBoukhoubza/OneDrive/Scriptie/VIS_data_Nihal'
folders = ['submissions_week3', 'submissions_week4']

# Extract and test queries from all students
all_queries = []
for folder in folders:
    folder_path = os.path.join(base_directory_path, folder)
    for file in os.listdir(folder_path):
        if file.endswith('.py'):
            file_path = os.path.join(folder_path, file)
            extracted_data = extract_queries_and_timestamps(file_path)
            for timestamp, query, query_name, bracket_count, table_count in extracted_data:
                error_message = ""
                try:
                    cursor.execute(query)
                    conn.commit()
                except Exception as e:
                    error_message = str(e)
                keyword_counts = count_keywords(query)
                all_queries.append({
                    'Folder': folder,
                    'File': file,
                    'Timestamp': timestamp,
                    'Query': query,
                    'Query Name': query_name,
                    'Bracket Count': bracket_count,
                    'Table Count': table_count,
                    'Error': error_message,
                    'Where Count': keyword_counts['where'],
                    'Group By Count': keyword_counts['group by'],
                    'Join Count': keyword_counts['join'],
                    'Having count': keyword_counts['having'],
                    'Distinct count': keyword_counts['distinct'],
#                    'Combi having and group by': keyword_counts['having', 'group by'],
                    'Contains error': 1 if error_message else 0,
                    'Error Type': categorize_error(error_message)
                })

# Convert to DataFrame and remove duplicates
queries_df = pd.DataFrame(all_queries).drop_duplicates()

# Group by relevant columns and get the minimum timestamp for each group
grouped_df = queries_df.groupby(['Folder', 'File','Query Name', 'Query', 'Error', 'Contains error', 'Error Type', 'Bracket Count', 'Table Count', 'Where Count',  'Group By Count', 'Join Count', 'Having count', 'Distinct count']).agg({'Timestamp': 'min'}).reset_index()

# Display the DataFrame
display(grouped_df)

# Save the DataFrame to CSV
csv_file_path = 'C:/Users/NihalBoukhoubza/OneDrive/Scriptie/SQL_queries_analysis_grouped.csv'
grouped_df.to_csv(csv_file_path, index=False)

# Close the database connection
conn.close()

print(f"Data saved to {csv_file_path}")