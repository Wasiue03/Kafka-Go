from kafka import KafkaConsumer
import json
from sentence_transformers import SentenceTransformer
import faiss  

# Connect to Kafka
consumer = KafkaConsumer(
    'api_data',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='vector-db-group'
)

# Load a model to convert text into vectors (embeddings)
model = SentenceTransformer('all-MiniLM-L6-v2')  # Or any other model

# Initialize FAISS index (for example)
import numpy as np
dimension = 384  # Depending on the model
index = faiss.IndexFlatL2(dimension)

for message in consumer:
    data = message.value
    print("Received:", data)

    # Example preprocessing (depends on your data)
    text = data.get('title')  # Adjust based on your actual data
    if text:
        embedding = model.encode([text])
        index.add(np.array(embedding))  # Add to vector DB

        print("Stored vector for:", text)
