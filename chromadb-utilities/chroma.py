import chromadb
from dotenv import load_dotenv
from pprint import pprint
from chromadb.utils import embedding_functions
from chromadb.config import Settings, DEFAULT_DATABASE
load_dotenv()


def create_collection(
    coll_name:str,
    embedding_func=embedding_functions.SentenceTransformerEmbeddingFunction,
    model_name="all-MiniLM-L6-v2"
):
    '''
        Lookup for Embedding functions and Model names

        - https://docs.trychroma.com/docs/embeddings/embedding-functions
    '''
    try:
        print(f"\n\nCreating a new collection - {coll_name}")
        collection = client.create_collection(
            name=coll_name,
            embedding_function=embedding_func(
                model_name=model_name
            )
        )
        print(f"\n\nCreated new Collection: {collection.name} successfully.")
    except Exception as e:
        print(f"\n\nERROR - {e}")


def get_collection(coll_name: str):
    try:
        coll = client.get_collection(name=coll_name)
        return coll
    except Exception as e:
        print(f"\n\nUnable to get collection. \nException - {e}")
        return None


def delete_collection(coll_name: str):
    coll = get_collection(coll_name)
    print(f"\n\nDeleting the collection - {coll_name}")
    
    if coll is not None:
        pass


def add_document(coll_name: str, ids, docs):
    coll = get_collection(coll_name)
    coll.add(       # Lengths of ID, Document and Embeddings must be equal.
        ids=ids,
        documents=docs,
        # embeddings=
    )


def query_collection(coll_name, query_text, n):     
    """
        Client will Embed (Semantic Embedding) the query_text and return "n" results
    """
    coll = get_collection(coll_name)
    print(f'\n\nResponse for Query: "{query_text}" in Collection: {coll.name}')

    results = coll.query(
        query_texts=[query_text],
        n_results=n,

        # DOCUMENT FILTER
        # where_document={ '$contains': 'MLFlow' },     # Single Condition
        where_document={
            "$or" : [
                { "$contains": 'awesome' },
                { "$contains": 'versioning' },
            ]
        },

        # METADATA FILTER
        # where={"page": { "$gt": 10 }},    # Single Condition
        where={
            "$and" : [
                { "category": 'fiction' },
                { "category": 'adventure' }
            ]
        }
    )
    return results


def print_collections():
    coll_list = client.list_collections()
    
    if coll_list == []:
        print("\n\nNo Collection created.")
    else:
        print(f"\n\nCollections for the client are,")
        for coll in client.list_collections():
            print(f"\t- {coll.name}")






client = chromadb.Client()
pers_client = chromadb.PersistentClient(path="./chroma_storage")
# print_collections()

# delete_collection("TestCollection")


# openai_ef = embedding_functions.OpenAIEmbeddingFunction(
#     api_key=env.OPENAI_API_KEY,
#     model_name="text-embedding-3-small"
# )

# create_collection("TestCollection")

# collection = get_collection("TestCollection")

# add_document("TestCollection", "id1", "MLFlow is an awesome tool for Machine Learning.")
# add_document("TestCollection", "id2", "DVC is an awesome versioning tool.")
# add_document("TestCollection", "id3", "Query document about MLFlow.")
# add_document("TestCollection", "id4", "Arnold Schwarzenegger is a big, jacked bodybuilder from Austria.")


# query_results = query_collection("TestCollection", "This is a query document about MLFlow.", 4)
# pprint(query_results)

# print_collections()
# print('\n')