import asyncio
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
