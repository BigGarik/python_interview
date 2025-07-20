import asyncio
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


def build_user_id_index(csv_path: str, index_path: str):
    index = {}
    with open(csv_path, 'r', encoding='utf-8') as f:
        header = f.readline()
        pos = f.tell()
        while True:
            line = f.readline()
            if not line:
                break
            offset = pos
            pos = f.tell()
            try:
                user_id = int(line.split(',')[0])
                index[user_id] = offset
            except (ValueError, IndexError):
                continue
    # Сохраняем индекс на диск
    with open(index_path, 'wb') as idx_file:
        pickle.dump(index, idx_file, protocol=pickle.HIGHEST_PROTOCOL)


def find_user_by_index(user_id: int, csv_path: str, index_path: str) -> dict | None:
    with open(index_path, 'rb') as idx_file:
        index = pickle.load(idx_file)
    offset = index.get(user_id)
    if offset is None:
        return None
    with open(csv_path, 'r', encoding='utf-8') as f:
        header = f.readline().strip().split(',')
        f.seek(offset)
        line = f.readline()
        if not line:
            return None
        fields = line.strip().split(',')
        return dict(zip(header, fields))