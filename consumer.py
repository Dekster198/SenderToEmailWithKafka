from decouple import config
from confluent_kafka import Consumer, KafkaException, KafkaError
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'email_group',
    'auto.offset.reset': 'earliest'
}


consumer = Consumer(conf)


topic = 'email_topic'
consumer.subscribe([topic])


try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f'Конец раздела {msg.topic()}')
            elif msg.error():
                raise KafkaException(msg.error())
        else:
            email = msg.value().decode('utf-8')
            msg = MIMEMultipart()
            msg['From'] = config('email_from')
            msg['To'] = email
            msg['Subject'] = 'Отправка сообщения с помощью Kafka'
            message = 'Данное текстовое сообщение было отправлено с помощью Kafka'
            msg.attach(MIMEText(message))

            mailserver = smtplib.SMTP('smtp.yandex.ru',587)
            mailserver.set_debuglevel(True)


            mailserver.ehlo()
            mailserver.starttls()
            mailserver.ehlo()

            mailserver.login(config('email_from'), config('SMTP_key'))
            mailserver.sendmail(config('email_from'), email, msg.as_string())
            mailserver.quit()
finally:
    consumer.close()
