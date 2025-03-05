import openai
import pandas as pd
import os
from sqlalchemy import create_engine
from tqdm import tqdm

tqdm.pandas()

# Set OpenAI API Key
openai.api_key = open(os.path.expanduser("~/.openai_key")).read().strip()

# Connect to PostgreSQL
engine = create_engine("postgresql://postgres:password@localhost:3000/shilldb")

# Fetch tweets that need embeddings
df = pd.read_sql("SELECT tweetid, content FROM tweets WHERE content_embedding IS NULL", engine)

# Batch size
BATCH_SIZE = 50


def get_embeddings(batch_texts):
    """Generate embeddings for a batch of texts."""
    response = openai.embeddings.create(
        input=batch_texts,  # Send the entire batch in one API call
        model="text-embedding-ada-002"
    )
    return [item.embedding for item in response.data]  # Return list of embeddings


# Process the DataFrame in batches
for i in tqdm(range(0, len(df), BATCH_SIZE)):
    batch = df.iloc[i:i + BATCH_SIZE]

    # Generate embeddings in bulk
    batch_embeddings = get_embeddings(batch["content"].tolist())

    # Insert embeddings into PostgreSQL
    conn = engine.raw_connection()
    cursor = conn.cursor()
    
    for tweetid, embedding in zip(batch["tweetid"], batch_embeddings):
        cursor.execute(
            "UPDATE tweets SET content_embedding = %s WHERE tweetid = %s",
            (embedding, tweetid)
        )

    conn.commit()
    cursor.close()
    conn.close()

print("Embeddings stored successfully!")
