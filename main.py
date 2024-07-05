from flask import Flask, request, jsonify
import psycopg2
import os
from dotenv import load_dotenv
import openai
from llama_index.core import (
    VectorStoreIndex,
    get_response_synthesizer,
    load_index_from_storage,
    StorageContext,
)
from llama_index.core.retrievers import VectorIndexRetriever
from llama_index.core.query_engine import RetrieverQueryEngine
import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML
from llama_index.llms.openai import OpenAI

load_dotenv()

DATABASE = {
    "NAME": os.environ.get("DB_NAME"),
    "USER": os.environ.get("DB_USER"),
    "PASSWORD": os.environ.get("DB_PASSWORD"),
    "HOST": os.environ.get("DB_HOST"),
    "PORT": int(os.environ.get("DB_PORT")),
}

app = Flask(__name__)


def is_valid_sql_query(sql_query):
    """
    Check if the SQL query is valid.
    
    Args:
        sql_query (str): The SQL query to be validated.
    
    Returns:
        bool: True if the query is valid, False otherwise.
    """
    parsed = sqlparse.parse(sql_query)
    if len(parsed) != 1:
        return False
    
    stmt = parsed[0]
    if not stmt.get_type() == 'UNKNOWN':
        return True
    return False


def perform_sql_query(sql_query):
    """
    Execute a SQL query on the configured database.
    
    Args:
        sql_query (str): The SQL query to be executed.
    
    Returns:
        tuple: A tuple containing the result of the query and a boolean indicating success.
    """
    if not is_valid_sql_query(sql_query):
        return None, False

    try:
        conn = psycopg2.connect(
            dbname=DATABASE["NAME"],
            user=DATABASE["USER"],
            password=DATABASE["PASSWORD"],
            host=DATABASE["HOST"],
            port=DATABASE["PORT"],
        )

        cursor = conn.cursor()
        cursor.execute(sql_query)

        result = cursor.fetchall()

        conn.commit()

        cursor.close()
        conn.close()

        return result, True

    except Exception as e:
        print(f"Error executing SQL query: {e}")
        return None, False


def create_sql_query(query):
    """
    Create a SQL query using LlamaIndex and OpenAI based on a given query.
    
    Args:
        query (str): The input query to generate a SQL query for.
    
    Returns:
        str: The generated SQL query.
    """
    llm = OpenAI(model="gpt-3.5-turbo", temperature=0)
    storage_context = StorageContext.from_defaults(persist_dir="./storage")
    index = load_index_from_storage(storage_context)

    retriever = VectorIndexRetriever(
        index=index,
        similarity_top_k=2,
    )

    response_synthesizer = get_response_synthesizer(
        llm=llm,
        response_mode="tree_summarize",
    )

    query_engine = RetrieverQueryEngine(
        retriever=retriever,
        response_synthesizer=response_synthesizer,
    )

    response = query_engine.query(
        f"Generate only POSTGRESQL query for the following, take care of conflicting data-types and do not hallucinate columns: {query}"
    )

    return str(response)


def evaluate(question, sql_query, result):
    """
    Generate a natural language response to a SQL query result using OpenAI.
    
    Args:
        question (str): The input question.
        sql_query (str): The generated SQL query.
        result (list): The result of the executed SQL query.
    
    Returns:
        str: The generated response.
    """
        
    try:
        openai_client = openai.OpenAI(
            api_key=os.environ.get("OPENAI_API_KEY"),
        )
        prompt = f"""
            Given the following question:\n {question} \n\n

            The generated SQL Query:\n {sql_query} \n\n

            and the following result from the query:\n {result} \n\n

            Respond with a proper sentence answering the question
        """

        response = openai_client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": prompt},
                {
                    "role": "user",
                    "content": "",
                },
            ],
            temperature=0.5,
        )

        gpt_output = response.choices[0].message.content

        return str(gpt_output)
    except Exception as e:
        print(f"Error generating caption: {e}")
        return None

def straight_answer(question):
    try:
        openai_client = openai.OpenAI(
            api_key=os.environ.get("OPENAI_API_KEY"),
        )
        response = openai_client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": question},
                {"role": "user", "content": ""},
            ],
            temperature=0.5,
        )

        gpt_output = response.choices[0].message.content

        return str(gpt_output)
    except Exception as e:
        print(f"Error generating caption: {e}")
        return None


@app.route("/query", methods=["POST"])
def answer():
    """
    Endpoint to handle query requests.
    
    Returns:
        Response: A JSON response containing the generated answer, SQL query, and query result.
    """

    try:
        data = request.get_json()
        question = data["query"]

        sql_query = create_sql_query(question)

        if sql_query:
            result, stat = perform_sql_query(sql_query)

            if stat:
                final_response = evaluate(question, sql_query, result)

                if final_response:
                    return jsonify({"response": final_response, "sql_query": sql_query, "query_result": result}), 200
                else:
                    return jsonify({"response": "Failed to generate response", "sql_query": sql_query, "query_result": result}), 500
            else:
                return jsonify({"response": "SQLQuery Failed", "sql_query": sql_query}), 500
        else:
            ans = straight_answer(question)
            return jsonify({"response": ans}), 200
        
    except Exception as e:
        print("Error:", e)
        return jsonify({"response": "Internal server error"}), 500


if __name__ == "__main__":
    app.run(debug=True)
