# utils.py

import boto3
import os
import shutil
import tomli as tomllib
from langchain_community.vectorstores import Chroma
from langchain_community.document_loaders import PyPDFDirectoryLoader  # Updated import
from langchain_community.document_loaders import PyPDFLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.embeddings.bedrock import BedrockEmbeddings
from langchain.prompts import ChatPromptTemplate
from botocore.exceptions import ClientError
import time
from thefuzz import fuzz
from haystack import Document as DocumentH
from haystack import Pipeline
from haystack.document_stores.in_memory import InMemoryDocumentStore
from haystack_integrations.components.embedders.amazon_bedrock import (
    AmazonBedrockDocumentEmbedder,
    AmazonBedrockTextEmbedder,
)
import json
from haystack.components.retrievers.in_memory import InMemoryEmbeddingRetriever


# Path to the database and documents
DATABASE_PATH = os.environ.get("DATABASE_PATH", "data/database")
DATA_PATH = os.environ.get("DATA_PATH", "data/docs")
TEMPLATE_PATH = os.environ.get("TEMPLATE_PATH", "data/templates")

# Function to initialize Bedrock embeddings
def get_embedding_function():
    return BedrockEmbeddings()

# Function to split documents into chunks
def split_documents(documents):
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=10000,
        chunk_overlap=2500,
        length_function=len,
        separators=["\n\n", "\n", ". ", "? ", "! ", " "]
    )
    return splitter.split_documents(documents)

# Function to calculate unique chunk IDs
def calculate_chunk_ids(chunks):
    last_page_id = None
    current_chunk_index = 0
    for chunk in chunks:
        source = chunk.metadata.get("source")
        page = chunk.metadata.get("page")
        current_page_id = f"{source}:{page}"
        if current_page_id == last_page_id:
            current_chunk_index += 1
        else:
            current_chunk_index = 0
        chunk.metadata["id"] = f"{current_page_id}:{current_chunk_index}"
        last_page_id = current_page_id
    return chunks

# Function to generate the document database
def generate_database(path):
    loader = PyPDFDirectoryLoader(path)  # Load PDF documents
    documents = loader.load()            # Load documents into a list

    chunks = split_documents(documents)   # Split documents into smaller chunks
    database = Chroma(
        persist_directory=DATABASE_PATH,
        embedding_function=get_embedding_function()
    )
    chunks_with_ids = calculate_chunk_ids(chunks)  # Assign unique IDs to chunks

    existing_items = database.get(include=[])  # Fetch existing document IDs
    existing_ids = set(existing_items["ids"])
    new_chunks = [chunk for chunk in chunks_with_ids if chunk.metadata["id"] not in existing_ids]

    # Add new chunks to the database if any exist
    if new_chunks:
        new_chunk_ids = [chunk.metadata["id"] for chunk in new_chunks]
        database.add_documents(new_chunks, ids=new_chunk_ids)
        database.persist()
    return documents

# Function to retrieve relevant context for a query
def get_relevant_context(template_dir, k=5):
    embedding_function_instance = get_embedding_function()
    db = Chroma(persist_directory=DATABASE_PATH, embedding_function = embedding_function_instance)
    query = assemble_rag_query(template_dir)
    results = db.similarity_search_with_score(query, k=k)
    context_text = "\n\n---\n\n".join([doc.page_content for doc, _score in results])
    conversation = assemble_analysis_prompt(context_text, template_dir)

    # prompt_template = ChatPromptTemplate.from_template(prompt)
    # prompt_text = prompt_template.format(context=context_text, question=query)
    # conversation = [{"role": "user", "content": [{"text": prompt_text}]}]


    return conversation, results


def get_relevant_context_chat(query, prompt, k=5):
    embedding_function_instance = get_embedding_function()
    db = Chroma(persist_directory=DATABASE_PATH, embedding_function = embedding_function_instance)
    results = db.similarity_search_with_score(query, k=k)
    context_text = "\n\n---\n\n".join([doc.page_content for doc, _score in results])
    prompt_template = ChatPromptTemplate.from_template(prompt)
    prompt_text = prompt_template.format(context=context_text, question=query)
    conversation = [{"role": "user", "content": [{"text": prompt_text}]}]
    return conversation, results


# Function to query the language model
def query_llm(conversation, client, model_id):
    try:
        response = client.converse(
            modelId=model_id,
            messages=conversation,
            inferenceConfig={"maxTokens": 4096, "temperature": 0},
            additionalModelRequestFields={"top_k": 250, "top_p": 1},
        )
        response_text = response["output"]["message"]["content"][0]["text"]
        return response_text
    except (ClientError, Exception) as e:
        print(f"ERROR: Can't invoke '{model_id}'. Reason: {e}")
        exit(1)


def assemble_rag_query(template_dir):
    with open(template_dir, "rb") as f:
        settings = tomllib.load(f)

    #query = " \n ".join([settings['role_prompt'],settings['task_prompt'],settings['instruction_prompt']])
    query = settings['task_prompt']
    # message = [
    #     {"role" : "user", "content" : [{"text" : settings['role_prompt']}, 
    #                                    {"text" : settings['task_prompt']},
    #                                    {"text" : settings['instruction_prompt']}]}
    # ]
    return query


def assemble_analysis_prompt(content, template_dir):
    with open(template_dir, "rb") as f:
        settings = tomllib.load(f)

    message = [
        {"role" : "user", "content" : [{"text" : settings['role_prompt']}, 
                                       {"text" : settings['task_prompt']},
                                       {"text" : settings['example_prompt']},
                                       {"text" : settings['reasoning_prompt']},
                                       {"text" : settings['output_prompt']},
                                       {"text" : f">>>>>\n{content}\n<<<<<"},
                                       {"text" : settings['instruction_prompt']}]}
    ]
    return message
    


os.environ["AWS_ACCESS_KEY_ID"] = "AKIAZXNNZJEPQOQ6SCAT"
os.environ["AWS_SECRET_ACCESS_KEY"] = "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"
os.environ["AWS_DEFAULT_REGION"] = "us-west-2"

embedder_model_id = "amazon.titan-embed-text-v2:0"

model_id = "anthropic.claude-3-haiku-20240307-v1:0"

client = boto3.client("bedrock-runtime", region_name="us-west-2")

def append_prompt(template_dir):
    with open(template_dir, "rb") as f:
        settings = tomllib.load(f)

    prompts = [settings['instruction_prompt'], 
               settings['task_prompt'], 
               settings['example_prompt'], 
               settings['reasoning_prompt']]
    return "\n".join(prompts)
    

def assemble_analysis_prompt(content, template_dir):
    with open(template_dir, "rb") as f:
        settings = tomllib.load(f)

    message = [
        {"role" : "user", "content" : [{"text" : settings['role_prompt']}, 
                                       {"text" : settings['task_prompt']},
                                       {"text" : settings['example_prompt']},
                                       {"text" : settings['reasoning_prompt']},
                                       {"text" : settings['output_prompt']},
                                       {"text" : f">>>>>\n{content}\n<<<<<"},
                                       {"text" : settings['instruction_prompt']}]}
    ]
    return message

def document_embedder_pipline(file_path, embedder_model_id):

    document_store = InMemoryDocumentStore(embedding_similarity_function="cosine")

    loader = PyPDFLoader(file_path)
    documents = loader.load()

    documents = [DocumentH(content = d.page_content, meta = d.metadata) for d in documents]

    document_embedder = AmazonBedrockDocumentEmbedder(model=embedder_model_id, meta_fields_to_embed=["source"])
    documents_with_embeddings = document_embedder.run(documents)['documents']
    document_store.write_documents(documents_with_embeddings)

    query_pipeline = Pipeline()
    query_pipeline.add_component("text_embedder", AmazonBedrockTextEmbedder(model=embedder_model_id))
    query_pipeline.add_component("retriever", InMemoryEmbeddingRetriever(document_store=document_store))
    query_pipeline.connect("text_embedder.embedding", "retriever.query_embedding")

    return query_pipeline, document_store

def extract_relevent_and_prompt_llm(query_pipeline, template_dir, top_k = 10):
    query = append_prompt(template_dir)

    result = query_pipeline.run({"text_embedder":{"text": query}})

    relevant_results = result['retriever']['documents'][:top_k]

    relevant_results = sorted(relevant_results, key = lambda x : x.meta['page'])

    content = "\n\n -------------------- \n\n".join([d.content for d in relevant_results])

    analysis_prompt = assemble_analysis_prompt(content, template_dir)
    response = query_llm(analysis_prompt, client, model_id)
    try:
        response_dict = json.loads(response) 
        response_dict['pages'] = [r.meta['page'] for r in relevant_results]

        return response, response_dict
    except:
        print("Invalid format returned by LLM")
        return response
    
def llm_pipeline(query_pipeline, name, with_delays = 1):
    analysis = dict(name = name)

    response1 = extract_relevent_and_prompt_llm(query_pipeline, "../templates/analysis_basic_indicators.toml", top_k = 10)
    analysis['basic'] = {'text' : response1} if len(response1) == 1 else {'text' : response1[0], 'obj' : response1[1]}
    time.sleep(with_delays)

    # response2 = extract_relevent_and_prompt_llm(query_pipeline, "../templates/analysis_sector.toml", top_k = 10)
    # analysis['sectore'] = {'text' : response2} if len(response2) == 1 else {'text' : response2[0], 'obj' : response2[1]}
    # time.sleep(with_delays)

    # response3 = extract_relevent_and_prompt_llm(query_pipeline, "../templates/analysis_sentiment.toml", top_k = 10)
    # analysis['sentiment'] = {'text' : response3} if len(response3) == 1 else {'text' : response3[0], 'obj' : response3[1]}
    # time.sleep(with_delays)

    return analysis


def get_report_name(company_name, year, directory_path):
    all_files = [f for f in os.listdir(directory_path) if os.path.isfile(os.path.join(directory_path, f))]

    files = [f for f in all_files if year.lower() in f.lower()]

    files2 = [f for f in files if company_name.lower() in f.lower()]

    if len(files2) == 1:
        return f"{directory_path}{files2[0]}"

    report_query = f"{company_name}"
    fuzz_ratios = [(f, fuzz.ratio(f, report_query)) for f in files]
    report = sorted(fuzz_ratios, key = lambda x : -x[1])[0][0]
    return f"{directory_path}{report}"

def ai_financial_assistant(company_name, year):
    directory_path = '../data/doc_store/'

    report_name = get_report_name(company_name, year, directory_path)

    document_store = InMemoryDocumentStore.load_from_disk(report_name)

    query_pipeline = Pipeline()
    query_pipeline.add_component("text_embedder", AmazonBedrockTextEmbedder(model=embedder_model_id))
    query_pipeline.add_component("retriever", InMemoryEmbeddingRetriever(document_store=document_store))
    query_pipeline.connect("text_embedder.embedding", "retriever.query_embedding")

    
    analysis = llm_pipeline(query_pipeline, report_name)

    return analysis