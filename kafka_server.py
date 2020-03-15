import producer_server


def run_kafka_server():
    
    # TODO get the json file path
    input_file = "./police-department-calls-for-service.json"

    # TODO fill in blanks
    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic="data_topic",
        bootstrap_servers="localhost:9092",
        client_id="crimeData_clientID"
    )

    return producer


def feed():
    producer = run_kafka_server()
    producer.generate_data()
    
def create_topic():
    """Creates the topic with the given topic name"""
    client = AdminClient({"bootstrap.servers": "localhost:9092"})
    futures = client.create_topics(
        [NewTopic(topic=TOPIC_NAME, num_partitions=1, replication_factor=1)]
    )
    for _, future in futures.items():
        try:
            future.result()
        except Exception as e:
            pass



if __name__ == "__main__":
    feed()
