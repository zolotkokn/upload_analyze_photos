import os
import pika
import time
import signal
import sys
import logging
from threading import Thread
from minio import Minio
from flask import Flask, jsonify

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Настройки из переменных окружения
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_USER = os.getenv("RABBITMQ_USER")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD")

# Проверка переменных окружения
def check_env_vars():
    required_vars = [
        "MINIO_ENDPOINT", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY",
        "RABBITMQ_HOST", "RABBITMQ_USER", "RABBITMQ_PASSWORD"
    ]
    for var in required_vars:
        if not os.getenv(var):
            raise ValueError(f"Необходимая переменная окружения {var} не установлена.")

check_env_vars()

# Инициализация Flask и MinIO
app = Flask(__name__)
client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# Анализ фото
def analyze_photo(photo_data):
    return {"result": "OK", "details": "This is a placeholder analysis result."}

# Обработка сообщений из RabbitMQ
def process_message(ch, method, properties, body):
    batch_id = body.decode()
    logging.info(f"Получено сообщение для пачки: {batch_id}")
    try:
        objects = client.list_objects("photos", prefix=f"{batch_id}/", recursive=True)
        results = []
        for obj in objects:
            try:
                response = client.get_object("photos", obj.object_name)
                photo_data = response.data
                analysis_result = analyze_photo(photo_data)
                results.append({"file": obj.object_name, "analysis": analysis_result})
            finally:
                response.close()
                response.release_conn()
        logging.info(f"Результаты анализа для пачки {batch_id}: {results}")
    except Exception as e:
        logging.error(f"Ошибка при обработке пачки {batch_id}: {e}")

# Подключение к RabbitMQ
connection = None

def start_consuming():
    global connection
    retries = 5
    delay = 5  # Секунды между попытками
    for attempt in range(retries):
        try:
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
            parameters = pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                credentials=credentials
            )
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()

            # Убедитесь, что очередь существует
            channel.queue_declare(queue='photo_analysis_queue', durable=False)

            channel.basic_consume(queue='photo_analysis_queue', on_message_callback=process_message, auto_ack=True)
            logging.info('Ожидание сообщений...')
            channel.start_consuming()
            return
        except pika.exceptions.AMQPConnectionError:
            logging.error(f"Не удалось подключиться к RabbitMQ. Попытка {attempt + 1}/{retries}. Повтор через {delay} секунд...")
            time.sleep(delay)
    logging.error("Не удалось подключиться к RabbitMQ после нескольких попыток.")

# Обработка сигналов завершения
def signal_handler(sig, frame):
    logging.info("Получен сигнал завершения. Закрываем соединение...")
    if connection and connection.is_open:
        connection.close()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# Эндпоинт для проверки состояния
@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "OK"}), 200

# Запуск приложения
if __name__ == "__main__":
    logging.info("Запуск сервиса анализа фото...")

    # Запуск Flask в отдельном потоке
    flask_thread = Thread(target=app.run, kwargs={"host": "0.0.0.0", "port": 5001})
    flask_thread.daemon = True
    flask_thread.start()

    # Запуск потребителя RabbitMQ
    start_consuming()
