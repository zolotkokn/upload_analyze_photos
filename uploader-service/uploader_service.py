import os
import uuid
from minio import Minio
from minio.error import S3Error
import pika
import time

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

def upload_files_to_minio(file_paths):
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    if not client.bucket_exists("photos"):
        client.make_bucket("photos")

    batch_id = str(uuid.uuid4())

    for file_path in file_paths:
        if not os.path.exists(file_path):
            print(f"Файл {file_path} не найден! Пропускаем.")
            continue

        try:
            file_name = os.path.basename(file_path)
            with open(file_path, 'rb') as file_data:
                client.put_object(
                    "photos",
                    f"{batch_id}/{file_name}",
                    file_data,
                    length=-1,
                    part_size=10*1024*1024,
                )
            print(f"Файл {file_name} успешно загружен в пачку {batch_id}.")
        except S3Error as err:
            print(f"Ошибка при загрузке файла {file_name}: {err}")

    notify_analyzer(batch_id)

def notify_analyzer(batch_id):
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
            channel.queue_declare(queue='photo_analysis_queue', durable=False)
            channel.basic_publish(
                exchange='',
                routing_key='photo_analysis_queue',
                body=batch_id,
                properties=pika.BasicProperties(delivery_mode=2)  # persistent message
            )
            print(f"Уведомление отправлено в очередь для пачки {batch_id}.")
            connection.close()
            return
        except pika.exceptions.AMQPConnectionError:
            print(f"Не удалось подключиться к RabbitMQ. Попытка {attempt + 1}/{retries}. Повтор через {delay} секунд...")
            time.sleep(delay)

    print("Не удалось подключиться к RabbitMQ после нескольких попыток.")

if __name__ == "__main__":
    # Пример списка файлов для загрузки
    files_to_upload = [
        "sent_photos/photo1.jpg",
        "sent_photos/photo2.jpg",
        "sent_photos/photo3.jpg"
    ]
    upload_files_to_minio(files_to_upload)
