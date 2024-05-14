import os
from langchain.text_splitter import CharacterTextSplitter
from langchain.vectorstores import Pinecone , Chroma
from langchain.embeddings.openai import OpenAIEmbeddings
from langchain.chains import ConversationalRetrievalChain
from langchain.chat_models import ChatOpenAI
from langchain.document_loaders import DirectoryLoader
import pinecone
from langchain_pinecone import PineconeVectorStore
from langchain_openai import ChatOpenAI
from langchain.llms import OpenAI
from langchain_community.vectorstores import Pinecone
from langchain_community.vectorstores import Chroma
from langchain_community.chat_models import ChatOpenAI
from langchain_community.llms import OpenAI
from langchain_community.document_loaders import DirectoryLoader
from langchain_openai import OpenAIEmbeddings
from langchain.chains import create_history_aware_retriever
from langchain_core.prompts import MessagesPlaceholder
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.messages import HumanMessage, AIMessage


# Create a function to store loaded filenames in a text file
# Store the loaded filenames in the text file



import os

def get_all_filenames(folder_path):
    filenames = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            filenames.append(os.path.join(root, file))
    return filenames

def save_to_txt(filenames, output_file):
    with open(output_file, 'w') as f:
        for filename in filenames:
            f.write(filename + '\n')

def get_filenames_from_file(filename):
    filenames = []
    with open(filename, 'r') as f:
        for line in f:
            filenames.append(line.strip())
    return filenames

def should_load_file(filename, exclude_filenames):
    # Check if the filename is in the list of filenames to exclude
    return os.path.basename(filename) not in exclude_filenames

folder_path = 'C:\\Users\\UppwiseLap\\Documents\\GenAIProject\\KnowledgeBaseAI\\Uppwise_Application_Doc'
output_file = 'filenames.txt'
filenames = get_all_filenames(folder_path)
save_to_txt(filenames, output_file)

# Existing code to load filenames from filenames.txt
exclude_file = 'filenames.txt'
exclude_filenames = get_filenames_from_file(exclude_file)






llm = ChatOpenAI(model="gpt-3.5-turbo-0125", api_key=os.environ.get("OPEN_API_KEY"), temperature=0.3)



txt_loader=DirectoryLoader('./Uppwise_Application_Doc',glob="**/*.txt",silent_errors=True)
docx_loader=DirectoryLoader('./Uppwise_Application_Doc',glob="**/*.docx",silent_errors=True)
pptx_loader=DirectoryLoader('./Uppwise_Application_Doc',glob='**/*.pptx',silent_errors=True)
xlsx_loader=DirectoryLoader('./Uppwise_Application_Doc',glob='**/*.xlsx',silent_errors=True)




#take all loader
loaders=[txt_loader,docx_loader,xlsx_loader,pptx_loader]




#lets create document
documents = []
for loader in loaders:
    documents.extend(loader.load())





# Split the text from the documents

text_splitter=CharacterTextSplitter(chunk_size=3000,chunk_overlap=1500)
documents = text_splitter.split_documents(documents)
print(len(documents))

#Embeddings and storing it in vector store

embeddings = OpenAIEmbeddings()

# Using pinecone for storing vectors

os.environ['PINECONE_API_KEY']="469893d2-888e-4294-8b0d-e72b36f33724"

#initialize the pinecone
index_name="langchain-demo"
vectorstore=PineconeVectorStore.from_documents(documents,embeddings,index_name=index_name)






# Now langchain part (Chaining with chat history )
retriever = vectorstore.as_retriever(search_type='similarity',search_kwargs={"k":10})
qa=ConversationalRetrievalChain.from_llm(llm,retriever)
chat_history=[]

def generate_response(prompt):
    user_input = prompt
    result=qa({
        "question":user_input,
        "chat_history":chat_history
    })
    chat_history.append((user_input,result['answer']))
    # return str(result["answer"])
    return result


