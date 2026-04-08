import snowflake.connector
import os

# Create connection
conn = snowflake.connector.connect(
    user=os.environ['SNOWFLAKE_USER'],
    password=os.environ['SNOWFLAKE_PASSWORD'],
    account=os.environ['SNOWFLAKE_ACCOUNT'],
    warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
    database=os.environ['SNOWFLAKE_DATABASE'],
    schema=os.environ['SNOWFLAKE_SCHEMA']
)

conn.autocommit = True
cursor = conn.cursor()


# ✅ Smart SQL splitter (handles $$ blocks)
def split_sql_statements(sql_script):
    statements = []
    current = ""
    in_dollar_block = False

    for line in sql_script.splitlines():
        if "$$" in line:
            in_dollar_block = not in_dollar_block

        current += line + "\n"

        if not in_dollar_block and ";" in line:
            statements.append(current.strip())
            current = ""

    if current.strip():
        statements.append(current.strip())

    return statements


def run_sql_file(file_path):
    print(f"\n📂 Running file: {file_path}")

    with open(file_path, 'r') as f:
        sql_script = f.read()

    statements = split_sql_statements(sql_script)

    for stmt in statements:
        stmt = stmt.strip()
        if stmt:
            try:
                print(f"➡️ Executing: {stmt[:80]}...")
                cursor.execute(stmt)
            except Exception as e:
                print(f"❌ Error in:\n{stmt}")
                print(e)
                raise e


# 🔁 Execution order
files = [
    "01_setup.sql",
    "02_load_data.sql",
    "03_transform.sql",
    "04_streams_tasks.sql",
    "05_procedure.sql"
]

for file in files:
    run_sql_file(file)

cursor.close()
conn.close()

print("\n✅ Pipeline executed successfully 🚀")
