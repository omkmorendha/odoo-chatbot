import json
from dotenv import load_dotenv
import os
import psycopg2
from llama_index.core import VectorStoreIndex, SimpleDirectoryReader
from llama_index.core.node_parser import JSONNodeParser
from llama_index.core.schema import Node

load_dotenv()

# Load database configuration from environment variables
DATABASE = {
    'NAME': os.environ.get("DB_NAME"),
    'USER': os.environ.get("DB_USER"),
    'PASSWORD': os.environ.get("DB_PASSWORD"),
    'HOST': os.environ.get("DB_HOST"),
    'PORT': int(os.environ.get("DB_PORT")),
}

def build_database_structure():
    conn = None
    database_structure = {}

    try:
        # Establish database connection
        conn = psycopg2.connect(
            dbname=DATABASE['NAME'],
            user=DATABASE['USER'],
            password=DATABASE['PASSWORD'],
            host=DATABASE['HOST'],
            port=DATABASE['PORT']
        )

        # Retrieve table names
        cur = conn.cursor()
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
        tables = cur.fetchall()

        # Retrieve columns with data types for each table
        for table in tables:
            table_name = table[0]
            cur.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name='{table_name}'")
            columns = cur.fetchall()
            database_structure[table_name] = {col[0]: col[1] for col in columns}

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
    finally:
        if conn is not None:
            conn.close()

    return database_structure

# Build database structure
database_structure = build_database_structure()

# Create nodes for LlamaIndex
nodes = []
for table_name, columns in database_structure.items():
    content = f"Table: {table_name}\nColumns: {', '.join(f'{col}: {data_type}' for col, data_type in columns.items())}"
    nodes.append(Node(text=content))

# Initialize and persist index
index = VectorStoreIndex(nodes)
index.storage_context.persist()

print("Database structure saved to LlamaIndex.")
