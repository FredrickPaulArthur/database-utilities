import polars as pl
from chromadb.utils import embedding_functions
from dotenv import load_dotenv
from chroma import client, pers_client
import numpy as np
from pprint import pprint
from tqdm import tqdm
load_dotenv()


# Creating a Collection
collection = client.create_collection(name="articles")


# openai_ef = embedding_functions.OpenAIEmbeddingFunction(
#     api_key=env.OPENAI_API_KEY,
#     model_name="text-embedding-3-small"
# )
text2vec_ef = embedding_functions.Text2VecEmbeddingFunction(model_name="shibing624/text2vec-base-chinese")

articles = pl.read_csv('./chroma_storage/Articles.csv', encoding="ISO-8859-1").with_row_index(offset=1)
# print(articles.shape)
# print(articles.head())



N = 50
articles = articles[:N]

articles_list = articles["Article"].to_list()
vectors = text2vec_ef(articles_list)
ids = [ f"id{x}" for x in articles['index'].to_list() ]

print(f"\n\nArticles: {len(articles["Article"])}")
print(f"\nVectors shape: {len([vectors])}")





# # Adding Documents to a Collection.
# collection.add(
#     documents=[articles["Article"][0]],
#     ids=["id1"],
#     # embeddings=[np.array(vector).flatten()],
#     embeddings=vectors
# )
# Adding Documents to a Collection.
collection.add(
    documents=articles_list,
    ids=ids,
    # embeddings=[np.array(vector).flatten()],
    embeddings=vectors
)
print("Total articles count: ", collection.count())

# Retrieve documents by ID
retrieved_document = collection.get(ids=["id1"])
print(f"Retrieved document: ")
print(retrieved_document)


# Querying the Collection
query_text = 'Whats the deal with the price of petrol?'
query_embeddings = text2vec_ef([query_text])
query_result = collection.query(
    query_embeddings=query_embeddings,
    n_results=3
)
pprint(f"\nQuery Result:\n{query_result}")