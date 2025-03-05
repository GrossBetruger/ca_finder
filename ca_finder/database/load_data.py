import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path
from sqlalchemy import create_engine, text


df = pd.read_csv(
    Path(__file__).parent.parent / "data" / "all_kol_tweets.csv"
)

# Create a connection to PostgreSQL
try:
    engine = create_engine('postgresql://postgres:password@localhost:3000/shilldb')
except Exception as e:
    print(f"Error creating engine: {e}")
    print("did you run docker_cmd.sh?")
    exit(1)


df.columns = df.columns.str.lower()
df.to_sql('tweets', engine, index=False, if_exists='replace')

# add embeddings column
alter_table_query = "ALTER TABLE tweets ADD COLUMN content_embedding vector(1536);"

# Execute the query using the engine
with engine.connect() as connection:
    connection.execute(text(alter_table_query))
    connection.commit()  # Ensure the change is committed


print(df)
