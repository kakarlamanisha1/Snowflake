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

cursor = conn.cursor()

def run_sql_file(file_path):
    print(f"\n📂 Running file: {file_path}")

    with open(file_path, 'r') as f:
        sql_script = f.read()

    try:
        # 🔥 If stored procedure exists → run whole script
        if "$$" in sql_script:
            print("⚙️ Detected stored procedure. Executing full script...")
            cursor.execute(sql_script)
        else:
            # ✅ Split and execute safely
            statements = sql_script.split(';')

            for stmt in statements:
                stmt = stmt.strip()
                if stmt:
                    try:
                        print(f"➡️ Executing: {stmt[:50]}...")
                        cursor.execute(stmt)
                    except Exception as e:
                        print(f"❌ Error in statement:\n{stmt}")
                        print(f"Error: {e}")
                        raise e

    except Exception as e:
        print(f"🔥 Failed executing file: {file_path}")
        print(e)
        raise e


# 🔁 Run all SQL files in order
files = [
    "01_setup.sql",
    "02_load_data.sql",
    "03_transform.sql",
    "04_streams_tasks.sql",
    "05_procedure.sql"
]

for file in files:
    run_sql_file(file)

# Close connection
cursor.close()
conn.close()

print("\n✅ Pipeline executed successfully 🚀")
