from openai import OpenAI
import streamlit as st
import os

st.sidebar.title("KnowledgeBaseAI")  # Display title in the sidebar
with st.sidebar:
    st.write("")  # Add some space at the top
    st.markdown("**History**")


client = OpenAI(api_key=api_key)

if "openai_model" not in st.session_state:
    st.session_state["openai_model"] = "gpt-3.5-turbo"

if "messages" not in st.session_state:
    st.session_state.messages = []

if "user_history" not in st.session_state:
    st.session_state.user_history = []

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])


for question in st.session_state.user_history:
    st.sidebar.write(f"- {question}")  # Display user question history in the sidebar

if prompt := st.chat_input("What is up?"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    st.session_state.user_history.append(prompt)  # Add user question to history

    with st.chat_message("user"):
        st.markdown(prompt)
    #
    with st.chat_message("assistant"):
        from KBServer import generate_response

        output = generate_response(prompt)
        stream = client.chat.completions.create(
            model=st.session_state["openai_model"],
            messages=[
                {"role": m["role"], "content": output['answer'] }
                for m in st.session_state.messages
            ],
            stream=True,
        )
        response = st.write_stream(stream)
    st.session_state.messages.append({"role": "assistant", "content": response})
