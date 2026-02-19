import chromadb
from chromadb.utils import embedding_functions
import pandas as pd
from pathlib import Path
from sentence_transformers import SentenceTransformer


def sync_vector_store():
    # ChromaDB setup (Local Persistently)
    client = chromadb.PersistentClient(path="./chroma_db")

    # MiniLM-L6-v2 embedding model
    model = embedding_functions.SentenceTransformerEmbeddingFunction(model_name="all-MiniLM-L6-v2")
    
    # get the collection
    collection = client.get_or_create_collection(
        name="lab_products", 
        embedding_function=model
    )

    # Paths
    PROJECT_ROOT = Path(__file__).resolve().parents[1]
    csv_path = PROJECT_ROOT / "analytics" / "data" / "core.Product_with_Descriptions.csv"

    # CSV with descritption column
    df = pd.read_csv(csv_path)

    # Upsert into ChromaDB
    # store the Description as the 'document' and ProductID as 'metadata'
    collection.upsert(
        ids=[str(pid) for pid in df['ProductID']],
        documents=df['Description'].tolist(),
        metadatas=[{"name": name} for name in df['ProductName']]
    )
    print(f"✅ Successfully synced {len(df)} products to ChromaDB.")

if __name__ == "__main__":
    sync_vector_store()