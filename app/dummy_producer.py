import json
import random
import time

from google.cloud import pubsub_v1

# Define as informações do projeto e tópico do Pub/Sub
project_id = "dev-project"
topic_id = "dev-topic"

if __name__ == "__main__":
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    i = 0
    while True:
        n = random.randint(0, 100)
        msg = {
            "i": i,
            "n": n,
            "model_id": n % 2
        }
        print(f"Publicando msg: {msg}")
        publisher.publish(topic_path, json.dumps(msg).encode('utf-8'))
        time.sleep(0.05)
        i += 1
