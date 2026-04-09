import snowflake.connector
import os
from cryptography.hazmat.primitives import serialization

# 🔐 Function to load private key (if provided)
def get_private_key():
    private_key_path = os.getenv("PRIVATE_KEY_PATH")

    if private_key_path and os.path.exists(private_key_path):
        with open(private_key_path, "rb") as key_file:
            p_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None
            )

        private_key = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )

        return private_key

    return None


# 🔗 Create connection (auto-switch: key or password)
private_key = get_private_key()

if private_key:
    print("🔐 Using Key Pair Authentication")
    conn = snowflake.connector.connect(
        user=os.environ['SNOWFLAKE_USERNAME'],
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        private_key=private_key,
        warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
        database=os.environ['SNOWFLAKE_DATABASE'],
        schema=os.environ['SNOWFLAKE_SCHEMA']
    )
else:
    print("🔑 Using Password Authentication")
    conn = snowflake.connector.connect(
        user=os.environ['SNOWFLAKE_USERNAME'],
        password=os.environ.get('SNOWFLAKE_PASSWORD'),
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
