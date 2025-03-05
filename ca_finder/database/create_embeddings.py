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


def get_embedding(text):
    response = openai.embeddings.create(
        input=text,
        model="text-embedding-ada-002"
    )
    return response.data[0].embedding  # Correct way to access the embedding


df["embedding"] = df["content"][:1000].progress_apply(get_embedding)

# Insert embeddings into PostgreSQL
conn = engine.raw_connection()
cursor = conn.cursor()
for index, row in list(df.iterrows())[:1000]:
    cursor.execute(
        "UPDATE tweets SET content_embedding = %s WHERE tweetid = %s",
        (row["embedding"], row["tweetid"])
    )
conn.commit()
cursor.close()
conn.close()

print("Embeddings stored successfully!")
