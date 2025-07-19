import time
from typing import List


def calculate_score(debt: int, contracts: List = []) -> float:
    """Расчет ML моделью оценочного значения (N)

    :param debt: Текущий долг пользователя
    :param contracts: Список контрактов
    :return: Значение (N)
    """

    score = int((sum(contract['amount'] for contract in contracts) - debt) / 1_000_000)

    # NOTE: Эмуляция расчета ML моделью. Это является bottleneck-ом и одной из нескольких
    # причин почему ML модели нужно запускать в отдельных процессах.
    time.sleep(1)

    return max(0, min(100, score))


def __process_ml_score(queue_ml_request, queue_ml_response):
    """Функция запуска ML обработчиков.

    :param queue_ml_request: Очередь запроса в ML модель
    :param queue_ml_response: Очередь ответа из ML модели
    """
    while True:
        next_task = queue_ml_request.get()
        score = calculate_score(next_task['debt'], next_task['contracts'])
        queue_ml_response.put(score)
