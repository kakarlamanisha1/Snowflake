import snowflake.connector
import os

conn = snowflake.connector.connect(
    user=os.environ['SNOWFLAKE_USER'],
    password=os.environ['SNOWFLAKE_PASSWORD'],
    account=os.environ['SNOWFLAKE_ACCOUNT'],
    warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
    database=os.environ['SNOWFLAKE_DATABASE'],
    schema=os.environ['SNOWFLAKE_SCHEMA']
)

cursor = conn.cursor()

def run_sql_file(file_path):
    with open(file_path, 'r') as f:
        sql_commands = f.read().split(';')
        for command in sql_commands:
            if command.strip():
                cursor.execute(command)

# Run all scripts
files = [
    "01_setup.sql",
    "02_load_data.sql",
    "03_transform.sql",
    "04_streams_tasks.sql",
    "05_procedure.sql"
]

for file in files:
    print(f"Running {file}")
    run_sql_file(file)

cursor.close()
conn.close()

print("Pipeline executed successfully 🚀")
