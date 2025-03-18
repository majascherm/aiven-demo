from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime, timedelta

from kafka_config import KAFKA_BROKER, KAFKA_TOPICS, KAFKA_ACCESS_KEY_PATH, KAFKA_ACCESS_CERT_PATH, KAFKA_CA_CERT_PATH

# Initialize the Kafka producer with SSL configuration
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),  # Serialize JSON to Kafka
    security_protocol='SSL',
    ssl_certfile=KAFKA_ACCESS_CERT_PATH,  # Path to your client certificate
    ssl_keyfile=KAFKA_ACCESS_KEY_PATH,  # Path to your client key
    ssl_cafile=KAFKA_CA_CERT_PATH,  # Path to your CA certificate
)

# Sample data
users = [str(i) for i in range(1, 21)]
products = ["blue crab", "king crab", "snow crab", "dungeness crab", "stone crab", "hermit crab", "ghost crab", "horseshoe crab"]
pages = ["/home", "/checkout", "/cart", "/profile"] + [f"/product/{p.replace(' ', '_')}" for p in products]
elements_general = ["link_about", "image_banner", "menu_item"]
element_checkout = "checkout_button"
element_buy = "button_buy"
queries = ["blue crab", "king crab", "snow crab", "dungeness crab", "stone crab", "hermit crab", "fiddler crab", "ghost crab", "horseshoe crab"]

def generate_random_timestamp():
    # Get current time
    current_time = datetime.now()
    
    # Generate a random number of days within the past week
    random_days = random.randint(0, 7)
    random_hour = random.randint(0,24)
    random_min = random.randint(0,60)
    
    # Subtract the random number of days from the current time
    random_date = current_time - timedelta(days=random_days, hours=random_hour, minutes=random_min)
    
    # Format the timestamp to match the required format
    return random_date.isoformat()

def generate_search_event():
    return {
        "user_id": random.choice(users),
        "query": random.choice(queries),
        "timestamp": generate_random_timestamp()
    }

def generate_page_click_event():
    page = random.choice(pages)
    if page.startswith("/product/"):
        element = element_buy
    elif page == "/checkout":
        element = element_checkout
    else:
        element = random.choice(elements_general)
    return {
        "user_id": random.choice(users),
        "page": page,
        "element": element,
        "timestamp": generate_random_timestamp()
    }

def generate_transaction_event():
    return {
        "user_id": random.choice(users),
        "amount": round(random.uniform(10, 500), 2),
        "purchase_id": f"{random.randint(1000, 9999)}",
        "product_ids": [random.choice(products) for _ in range(random.randint(1, 5))],
        "timestamp": generate_random_timestamp()
    }

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

if __name__ == "__main__":
    print("Producing Clickstream events...")
    try:
        while True:
            event_type = random.choice(["search", "page_click", "transactions"])
            if event_type == "search":
                event = generate_search_event()
                topic = KAFKA_TOPICS["search"]
            elif event_type == "page_click":
                event = generate_page_click_event()
                topic = KAFKA_TOPICS["page_click"]
            else:
                event = generate_transaction_event()
                topic = KAFKA_TOPICS["transactions"]
            
            print(event)
            producer.send(
                topic, 
                value=event
            )
            producer.flush()
            time.sleep(1)  
    except KeyboardInterrupt:
        print("Stopping producer...")
        producer.close()
