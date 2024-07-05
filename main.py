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
from langchain_community.utilities import SQLDatabase

load_dotenv()

DATABASE = {
    "NAME": os.environ.get("DB_NAME"),
    "USER": os.environ.get("DB_USER"),
    "PASSWORD": os.environ.get("DB_PASSWORD"),
    "HOST": os.environ.get("DB_HOST"),
    "PORT": int(os.environ.get("DB_PORT")),
}

app = Flask(__name__)


def perform_sql_query(sql_query):
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
    storage_context = StorageContext.from_defaults(persist_dir="./storage")
    index = load_index_from_storage(storage_context)

    retriever = VectorIndexRetriever(
        index=index,
        similarity_top_k=2,
    )

    response_synthesizer = get_response_synthesizer(
        response_mode="tree_summarize",
    )

    query_engine = RetrieverQueryEngine(
        retriever=retriever,
        response_synthesizer=response_synthesizer,
    )

    response = query_engine.query(
        f"Generate a strict POSTGRESQL query for the following: {query}"
    )
    print(response)

    return str(response)


def evaluate(question, sql_query, result):
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
                    "content": """
                    Given the following question:
                    What is the total value of all the sales?
                    

                    The generated SQL Query:
                    SELECT SUM(amount) AS total_sales_value FROM sale_advance_payment_inv
                    

                    and the following result from the query:
                    [(32,)]
                    

                    Response:
                    There were $320 dollars of sales
                 """,
                },
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
    try:
        data = request.get_json()
        question = data["query"]

        sql_query = create_sql_query(question)

        if sql_query != "None":
            result, stat = perform_sql_query(sql_query)

            if stat:
                final_response = evaluate(question, sql_query, result)

                if final_response:
                    return jsonify({"response": final_response}), 200
                else:
                    return jsonify({"error": "Failed to generate response"}), 500
            else:
                return jsonify({"error": "SQL Query failed"}), 500
        else:
            return jsonify({"error": "No need for query"}), 400

    except Exception as e:
        print("Error:", e)
        return jsonify({"error": "Internal server error"}), 500


if __name__ == "__main__":
    app.run(debug=True)
