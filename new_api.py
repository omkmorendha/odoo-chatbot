from flask import Flask, request, jsonify
from langchain_openai import ChatOpenAI
from langchain.agents import create_openai_tools_agent, AgentExecutor
from langchain_core.prompts.chat import ChatPromptTemplate, HumanMessagePromptTemplate, MessagesPlaceholder
from langchain_core.messages import AIMessage
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits import SQLDatabaseToolkit
import os

app = Flask(__name__)
DATABASE = {
    "NAME": os.environ.get("DB_NAME"),
    "USER": os.environ.get("DB_USER"),
    "PASSWORD": os.environ.get("DB_PASSWORD"),
    "HOST": os.environ.get("DB_HOST"),
    "PORT": int(os.environ.get("DB_PORT")),
}

print(DATABASE)
db = SQLDatabase.from_uri(f"postgresql://{DATABASE['USER']}:{DATABASE['PASSWORD']}@{DATABASE['HOST']}:{DATABASE['PORT']}/{DATABASE['NAME']}")
toolkit = SQLDatabaseToolkit(db=db, llm=ChatOpenAI(temperature=0))
context = toolkit.get_context()
tools = toolkit.get_tools()

custom_messages = [
    HumanMessagePromptTemplate.from_template("Answer the following query based on the database with information provided: {input}"),
    AIMessage(content="{SQL_FUNCTIONS_SUFFIX}"),
    MessagesPlaceholder(variable_name="agent_scratchpad"),
]
custom_prompt = ChatPromptTemplate.from_messages(custom_messages)
custom_prompt = custom_prompt.partial(**context)

llm = ChatOpenAI(model="gpt-3.5-turbo", temperature=0)
agent = create_openai_tools_agent(llm, tools, custom_prompt)
agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True)


@app.route('/query', methods=['POST'])
def query():
    data = request.json
    user_query = data.get('query')

    if not user_query:
        return jsonify({'error': 'Query is required'}), 400

    response = agent_executor.invoke({"input": user_query})
    return jsonify({'response': response})

if __name__ == "__main__":
    app.run(port=5000)
