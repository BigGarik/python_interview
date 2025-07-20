import asyncio

import uvicorn
import multiprocessing
from fastapi import FastAPI

from app.ml_model import __process_ml_score
from app.models import UserScoreRequest
from app.reports import generate_big_report
from app.sms import send_notifications
from app.storage import db_1_get_user_contracts, db_2_save_user_contracts, file_db_save_contracts_report

app = FastAPI()
COUNTED_SCORES = {}


@app.post("/user/score/count")    # почему post
async def post_user_score_count(data: UserScoreRequest):
    """Данная АПИ занимается расчетом (N).
        - Принимает запрос с указанием текущего долга пользователя
        - Получает список контрактов пользователя
        - Делает расчет (N) при помощи ML модели на основании долга и суммы контрактов
        - Генерирует большой отчет на основании имеющихся контрактов
        - Сохраняет информацию о результатах расчета в БД для каждого контракта
        - Сохраняет отчет на удаленный сервер
        - Отправляет SMS/PUSH сообщение пользователю с указанием его (N)
        - Возвращает сервису с которого поступил запрос success статус об обработке данных.

    :param data: ID пользователя и его текущий долг по непогашенным контрактам.
    :return: Статус успешной обработки. Результат (N)
    """
    # TODO: Найти потенциальные проблемы, оптимизировать. Текущее время работы ~4.82с
    contracts = db_1_get_user_contracts(data.user_id)
    ml_data = data.model_dump()
    ml_data['contracts'] = contracts
    app.state.queue_ml_request.put(ml_data)
    score = app.state.queue_ml_response.get()
    big_report_task = asyncio.create_task(generate_big_report(data.user_id, contracts))
    save_user_contracts_task = asyncio.create_task(db_2_save_user_contracts(data.user_id, contracts))
    # report_path = await generate_big_report(data.user_id, contracts)

    report_path, _ = await asyncio.gather(big_report_task, save_user_contracts_task)
    await file_db_save_contracts_report(report_path)
    # await db_2_save_user_contracts(data.user_id, contracts)

    send_notifications(data.user_id, report_path, score)   # ждем 3 секунды надо асинк
    COUNTED_SCORES[data.user_id] = score

    return {"status": True, 'score': score}


def __process_server(queue_ml_request, queue_ml_response):
    """Функция запуска сервера на FastAPI.

    :param queue_ml_request: Очередь запроса в ML модель.
    :param queue_ml_response: Очередь ответа из ML модели.
    """
    app.state.queue_ml_request = queue_ml_request
    app.state.queue_ml_response = queue_ml_response

    uvicorn.run(app, host="0.0.0.0", port=8000)


def main():
    manager = multiprocessing.Manager()
    queue_ml_request = manager.Queue()
    queue_ml_response = manager.Queue()

    # Процесс сервера
    server_proc = multiprocessing.Process(target=__process_server, args=(queue_ml_request, queue_ml_response))

    # Процессы ML моделей для расчета (N)
    ml_ops_proc = [
        multiprocessing.Process(
            target=__process_ml_score, args=(queue_ml_request, queue_ml_response), daemon=True
        ) for _ in range(multiprocessing.cpu_count() - 2)
    ]

    server_proc.start()
    _ = [p.start() for p in ml_ops_proc]

    server_proc.join()
    _ = [p.join() for p in ml_ops_proc]


if __name__ == "__main__":
    main()
