import asyncio
import csv
import os
import pickle
import random
import time
from typing import List


def __save_to_db(*_, **__):
    # NOTE: Эмуляция вызова сохранения объекта в БД
    time.sleep(0.002)


def db_1_get_user_contracts(user_id: int) -> List[dict]:
    """Получение списка контрактов пользователя из БД 1

    :param user_id:
    :return: Список контрактов для конкретного пользователя
    """
    # NOTE: Эмуляция получения списка контрактов
    time.sleep(0.01)

    return [
        {'id': i, 'amount': int(1_000_000 * i / random.randint(100, 10_00))} for i in range(100)
    ]


async def db_2_save_user_contracts(user_id: int, contracts: List[dict]) -> None:
    """Сохранение списка контрактов пользователя из БД 2

    :param user_id: ID пользователя
    :param contracts: Список контрактов
    """

    for contract in contracts:
        __save_to_db(user_id, contract)


async def file_db_save_contracts_report(path: str) -> None:
    """Сохраняет большой отчет на файловый сервер.
        NOTE: В этой функции специально игнорируем сам файл, нужна только эмуляция работы с ним.

    :param path: Путь, по которому нужно положить отчет с контрактами
    """

    # NOTE: Эмуляция сохранения на файловый сервер большого отчета в формате html
    await asyncio.sleep(0.05)


def find_user_by_index(user_id: int, csv_path: str, index_path: str) -> dict | None:
    """Находит пользователя по ID используя индекс"""
    with open(index_path, 'rb') as idx_file:
        index = pickle.load(idx_file)

    offset = index.get(user_id)
    if offset is None:
        return None

    with open(csv_path, 'rb') as f:
        header_line = f.readline().decode('utf-8').strip()
        header = next(csv.reader([header_line]))

        f.seek(offset)
        line = f.readline().decode('utf-8').strip()
        if not line:
            return None

        try:
            fields = next(csv.reader([line]))
            return dict(zip(header, fields))
        except (StopIteration, csv.Error):
            return None


def build_user_id_index(csv_path: str, index_path: str):
    """Индексирование с отображением прогресса"""
    index = {}
    file_size = os.path.getsize(csv_path)
    BUFFER_SIZE = 128 * 1024 * 1024  # 128 МБ

    start_time = time.time()
    bytes_processed = 0

    with open(csv_path, 'rb') as f:
        f.readline()
        file_pos = f.tell()
        remainder = b''

        while True:
            chunk = f.read(BUFFER_SIZE)
            if not chunk:
                break

            bytes_processed += len(chunk)

            data = remainder + chunk
            last_newline = data.rfind(b'\n')

            if last_newline == -1:
                remainder = data
                continue

            pos = 0
            while pos < last_newline:
                line_end = data.find(b'\n', pos)
                if line_end == -1 or line_end > last_newline:
                    break

                comma_pos = data.find(b',', pos, line_end)
                if comma_pos > pos:
                    try:
                        user_id = int(data[pos:comma_pos])
                        index[user_id] = file_pos
                    except ValueError:
                        pass

                line_length = line_end - pos + 1
                file_pos += line_length
                pos = line_end + 1

            remainder = data[last_newline + 1:]

            progress = bytes_processed / file_size * 100
            speed = bytes_processed / (1024 ** 2) / (time.time() - start_time)
            print(f"\rПрогресс: {progress:.1f}% | Скорость: {speed:.1f} МБ/с | Записей: {len(index)}", end='')

    print(f"\n\nИндексация завершена за {time.time() - start_time:.1f} сек")

    with open(index_path, 'wb') as f:
        pickle.dump(index, f, protocol=pickle.HIGHEST_PROTOCOL)


if __name__ == '__main__':
    build_user_id_index('../../scripts/users.csv', '../../scripts/users.idx')