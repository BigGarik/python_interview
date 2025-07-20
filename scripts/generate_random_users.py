import asyncio
import csv
import os
import random
import itertools
from faker import Faker
from asyncio import Lock

# ==== Глобальные настройки ====
MAX_FILE_SIZE_BYTES = 1 * 1024 * 1024 * 1024  # 1 ГБ
OUTPUT_FILE = 'users.csv'
FIELDS = ['ID', 'First Name', 'Last Name', 'Patronymic', 'Birth Date', 'Address', 'Status']
STATUSES = ['active', 'inactive', 'banned', 'pending']
NUM_WORKERS = 8         # количество параллельных задач
BATCH_SIZE = 500        # сколько строк генерирует каждая задача за раз
PRINT_EVERY = 10_000      # как часто печатаем прогресс

# ==== Инициализация Faker ====
faker = Faker('ru_RU')
lock = Lock()
total_written = 0

user_id_counter = itertools.count(1)


# ==== Генерация одной записи ====
def generate_user():
    return {
        'ID': next(user_id_counter),
        'First Name': faker.first_name(),
        'Last Name': faker.last_name(),
        'Patronymic': faker.middle_name(),
        'Birth Date': faker.date_of_birth(minimum_age=18, maximum_age=90).isoformat(),
        'Address': faker.address().replace('\n', ', '),
        'Status': random.choice(STATUSES)
    }

# ==== Генерация и запись пакета записей ====
async def worker(writer, writer_lock):
    global total_written
    while True:
        # Проверка размера файла
        if os.path.exists(OUTPUT_FILE) and os.path.getsize(OUTPUT_FILE) >= MAX_FILE_SIZE_BYTES:
            break

        batch = [generate_user() for _ in range(BATCH_SIZE)]

        async with writer_lock:
            writer.writerows(batch)
            total_written += BATCH_SIZE

            if total_written % PRINT_EVERY < BATCH_SIZE:
                size_now = os.path.getsize(OUTPUT_FILE)
                percent = (size_now / MAX_FILE_SIZE_BYTES) * 100
                print(f"Сгенерировано: {total_written} пользователей.")
                print(f"Размер файла: {size_now / (1024 ** 2):.2f} МБ / {MAX_FILE_SIZE_BYTES / (1024 ** 2)} МБ ({percent:.2f}%)")

        await asyncio.sleep(0)

# ==== Основная асинхронная функция ====
async def write_users_parallel():
    global total_written

    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=FIELDS)
        writer.writeheader()

        writer_lock = Lock()

        tasks = [asyncio.create_task(worker(writer, writer_lock)) for _ in range(NUM_WORKERS)]
        await asyncio.gather(*tasks)

    print(f"Генерация завершена. Итоговое количество пользователей: {total_written}")

# ==== Точка входа ====
if __name__ == '__main__':
    asyncio.run(write_users_parallel())
