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
    Build the structure of the database including tables, their columns with data types,
    and foreign key relationships.

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
            """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema='public'
            """
        )
        tables = cur.fetchall()

        for table in tables:
            table_name = table[0]

            # Check if table has at least one row
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cur.fetchone()[0]
            if row_count == 0:
                continue

            cur.execute(
                f"""
                SELECT
                    column_name,
                    data_type,
                    is_nullable,
                    column_default,
                    character_maximum_length
                FROM
                    information_schema.columns
                WHERE
                    table_name='{table_name}'
                """
            )
            columns = cur.fetchall()

            valid_columns = []
            for col in columns:
                try:
                    column_name = col[0]
                    cur.execute(f'SELECT COUNT(*) FROM "{table_name}" WHERE "{column_name}" IS NOT NULL')
                    non_null_count = cur.fetchone()[0]
                    if non_null_count > 0:
                        valid_columns.append(col)
                except:
                    print(f"Error processing column {column_name} in table {table_name}")

            if not valid_columns:
                continue

            cur.execute(
                f"""
                SELECT
                    kcu.column_name,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name
                FROM
                    information_schema.table_constraints AS tc
                    JOIN information_schema.key_column_usage AS kcu
                      ON tc.constraint_name = kcu.constraint_name
                      AND tc.table_schema = kcu.table_schema
                    JOIN information_schema.constraint_column_usage AS ccu
                      ON ccu.constraint_name = tc.constraint_name
                WHERE
                    tc.constraint_type = 'FOREIGN KEY'
                    AND tc.table_name='{table_name}'
                """
            )
            foreign_keys = cur.fetchall()

            column_info = [
                {
                    "name": col[0],
                    "data_type": col[1],
                    # "is_nullable": col[2],
                    # "default": col[3],
                    # "max_length": col[4],
                }
                for col in valid_columns
            ]
            fk_info = [
                {
                    "column": fk[0],
                    "references_table": fk[1],
                    "references_column": fk[2],
                }
                for fk in foreign_keys
            ]
            database_structure[table_name] = {
                "columns": column_info,
                "foreign_keys": fk_info,
            }

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
    finally:
        if conn is not None:
            conn.close()

    return database_structure


def save_structure_to_json(database_structure, filename="database_structure.json"):
    """
    Save the database structure to a JSON file.

    Args:
        database_structure (dict): The structure of the database.
        filename (str): The name of the file to save the structure to.
    """
    with open(filename, 'w') as json_file:
        json.dump(database_structure, json_file, indent=4)
    print(f"Database structure saved to {filename}")


def create_nodes_from_structure(database_structure):
    """
    Create nodes from the database structure for indexing.

    Args:
        database_structure (dict): The structure of the database.

    Returns:
        list: A list of nodes for indexing.
    """
    nodes = []
    for table_name, table_info in database_structure.items():
        columns = table_info["columns"]
        foreign_keys = table_info["foreign_keys"]

        column_descriptions = [
            f"{col['name']} ({col['data_type']}"
            for col in columns
        ]
        content = f"Table: {table_name}\nColumns:\n" + "\n".join(column_descriptions)
        
        if foreign_keys:
            fk_content = "\nForeign Keys:\n" + "\n".join(
                f"{fk['column']} -> {fk['references_table']}({fk['references_column']})"
                for fk in foreign_keys
            )
            content += fk_content
        nodes.append(Node(text=content))
    return nodes


def main():
    """
    Main function to load the database structure and save it to LlamaIndex and JSON.
    """
    # Load database configuration
    database_config = load_database_config()

    # Build database structure
    database_structure = build_database_structure(database_config)

    # Save database structure to JSON
    save_structure_to_json(database_structure)

    # Create nodes from the database structure
    nodes = create_nodes_from_structure(database_structure)

    # Initialize and persist index
    index = VectorStoreIndex(nodes)
    index.storage_context.persist()

    print("Database structure saved to LlamaIndex.")


if __name__ == "__main__":
    main()
