import json
from dotenv import load_dotenv
import os
import psycopg2
from llama_index.core import VectorStoreIndex, SimpleDirectoryReader
from llama_index.core.node_parser import JSONNodeParser
from llama_index.core.schema import Node

load_dotenv()

DATABASE = {
    'NAME': os.environ.get("DB_NAME"),
    'USER': os.environ.get("DB_USER"),
    'PASSWORD': os.environ.get("DB_PASSWORD"),
    'HOST': os.environ.get("DB_HOST"),
    'PORT': int(os.environ.get("DB_PORT")),
}

def build_database_structure():
    conn = psycopg2.connect(
        dbname=DATABASE['NAME'],
        user=DATABASE['USER'],
        password=DATABASE['PASSWORD'],
        host=DATABASE['HOST'],
        port=DATABASE['PORT']
    )

    cur = conn.cursor()

    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
    tables = cur.fetchall()

    database_structure = {}

    for table in tables:
        table_name = table[0]
        
        cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name='{table_name}'")
        columns = cur.fetchall()
        database_structure[table_name] = [col[0] for col in columns]

    cur.close()
    conn.close()

    return database_structure


database_structure = build_database_structure()
nodes = []
for table_name, columns in database_structure.items():
    content = f"Table: {table_name}\nColumns: {', '.join(columns)}"
    nodes.append(Node(text=content))


index = VectorStoreIndex(nodes)
index.storage_context.persist()

print("Database structure saved to LlamaIndex.")