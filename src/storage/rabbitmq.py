class RabbitMQStorage:
    def __init__(self, host='localhost', port=5672, username='guest', password='guest'):
        import pika
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host, port, '/', pika.PlainCredentials(username, password)))
        self.channel = self.connection.channel()

    def publish(self, queue_name, message):
        self.channel.queue_declare(queue=queue_name, durable=True)
        self.channel.basic_publish(exchange='', routing_key=queue_name, body=message, properties=pika.BasicProperties(delivery_mode=2))
        print(f"Published message to {queue_name}: {message}")

    def close(self):
        self.connection.close()