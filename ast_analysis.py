import json
import os
import psycopg2
from dotenv import load_dotenv
from llama_index.core import VectorStoreIndex
from llama_index.core.schema import Node


def load_database_config():
    """
    Load database configuration from environment variables.

    Returns:
        dict: A dictionary containing database connection details.
    """
    load_dotenv()
    return {
        "NAME": os.environ.get("DB_NAME"),
        "USER": os.environ.get("DB_USER"),
        "PASSWORD": os.environ.get("DB_PASSWORD"),
        "HOST": os.environ.get("DB_HOST"),
        "PORT": int(os.environ.get("DB_PORT")),
    }


def build_database_structure(database_config):
    """
    Build the structure of the database including tables and their columns with data types.

    Args:
        database_config (dict): Database connection details.

    Returns:
        dict: A dictionary representing the structure of the database.
    """
    conn = None
    database_structure = {}

    try:
        conn = psycopg2.connect(
            dbname=database_config["NAME"],
            user=database_config["USER"],
            password=database_config["PASSWORD"],
            host=database_config["HOST"],
            port=database_config["PORT"],
        )

        cur = conn.cursor()
        cur.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
        )
        tables = cur.fetchall()

        for table in tables:
            table_name = table[0]
            cur.execute(
                f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name='{table_name}'"
            )
            columns = cur.fetchall()
            database_structure[table_name] = {col[0]: col[1] for col in columns}

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
    finally:
        if conn is not None:
            conn.close()

    return database_structure


def create_nodes_from_structure(database_structure):
    """
    Create nodes from the database structure for indexing.

    Args:
        database_structure (dict): The structure of the database.

    Returns:
        list: A list of nodes for indexing.
    """
    nodes = []
    for table_name, columns in database_structure.items():
        content = f"Table: {table_name}\nColumns: {', '.join(f'{col}: {data_type}' for col, data_type in columns.items())}"
        nodes.append(Node(text=content))
    return nodes


def main():
    """
    Main function to load the database structure and save it to LlamaIndex.
    """
    # Load database configuration
    database_config = load_database_config()

    # Build database structure
    database_structure = build_database_structure(database_config)

    # Create nodes from the database structure
    nodes = create_nodes_from_structure(database_structure)

    # Initialize and persist index
    index = VectorStoreIndex(nodes)
    index.storage_context.persist()

    print("Database structure saved to LlamaIndex.")


if __name__ == "__main__":
    main()
