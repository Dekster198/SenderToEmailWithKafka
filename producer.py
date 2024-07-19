from decouple import config
from confluent_kafka import Producer


conf = {
    'bootstrap.servers': 'localhost:9092'
}


producer = Producer(conf)


def delivery_report(err, msg):
    if err is not None:
        print(f'Ошибка при доставке: {err}')
    else:
        print(f'Доставлено сообщение {msg} в {msg.topic()}')


topic = 'email_topic'
emails = []


with open('email.txt', 'r') as f:
    for line in f:
        producer.produce(topic, value=line, callback=delivery_report)
        producer.poll(0)


producer.flush()

