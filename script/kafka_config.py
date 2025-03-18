# kafka_config.py
KAFKA_BROKER = "<host>:<port>"
KAFKA_TOPICS = {
    "search": "clickstream_search",
    "page_click": "clickstream_page_click",
    "transactions": "clickstream_transactions"
}
KAFKA_ACCESS_KEY_PATH = "./security/access_key.pem"
KAFKA_ACCESS_CERT_PATH = "./security/access_cert.pem"
KAFKA_CA_CERT_PATH = "./security/ca_cert.pem"
