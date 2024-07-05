import streamlit as st
import requests

# Streamlit app
st.title("Query Handler")

st.write("Enter your query and get the response from the API.")

# User input
query = st.text_input("Query")

if st.button("Submit"):
    if query:
        url = "http://localhost:5000/query"
        payload = {"query": query}
        try:
            response = requests.post(url, json=payload)
            if response.status_code == 200:
                data = response.json()
                st.write("Response:", data.get("response"))
                st.write("SQL Query:", data.get("sql_query"))
                st.write("Query Result:", data.get("query_result"))
            else:
                st.write("Error:", response.json().get("response"))
        except Exception as e:
            st.write("Error:", e)
    else:
        st.write("Please enter a query.")
