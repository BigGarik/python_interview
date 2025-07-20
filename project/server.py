import asyncio
import threading

import uvicorn
import multiprocessing
from fastapi import FastAPI, HTTPException

from app.ml_model import __process_ml_score
from app.models import UserScoreRequest
from app.reports import generate_big_report
from app.sms import send_notifications
from app.storage import db_1_get_user_contracts, db_2_save_user_contracts, file_db_save_contracts_report, \
    find_user_in_csv

app = FastAPI()


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

    report_path, _ = await asyncio.gather(
        generate_big_report(data.user_id, contracts),
        db_2_save_user_contracts(data.user_id, contracts)
    )

    asyncio.create_task(file_db_save_contracts_report(report_path))

    # в отдельном потоке
    threading.Thread(
        target=send_notifications,
        args=(data.user_id, report_path, score),
        daemon=True
    ).start()

    app.state.counted_scores[data.user_id] = score

    return {"status": True, 'score': score}


@app.get("/user/info/{user_id}")
async def get_user_info(user_id: int):
    """
    Возвращает информацию о пользователе по его ID и рассчитанный score.

    :param user_id: ID пользователя (int)
    :return: JSON с полями пользователя и его score
    """
    # 1. Проверка наличия score
    if user_id not in app.state.counted_scores:
        raise HTTPException(status_code=404, detail="Score not found for user")

    # 2. Поиск пользователя в users.csv
    user_data = find_user_in_csv(user_id, "../users.csv")
    if not user_data:
        raise HTTPException(status_code=404, detail="User not found")

    # 3. Добавляем score к результату
    user_data['score'] = app.state.counted_scores[user_id]

    return user_data


def __process_server(queue_ml_request, queue_ml_response, counted_scores):
    """Функция запуска сервера на FastAPI.

    :param queue_ml_request: Очередь запроса в ML модель.
    :param queue_ml_response: Очередь ответа из ML модели.
    """
    app.state.queue_ml_request = queue_ml_request
    app.state.queue_ml_response = queue_ml_response
    app.state.counted_scores = counted_scores

    uvicorn.run(app, host="0.0.0.0", port=8000)


def main():
    manager = multiprocessing.Manager()
    queue_ml_request = manager.Queue()
    queue_ml_response = manager.Queue()
    counted_scores = manager.dict()

    # Процесс сервера
    server_proc = multiprocessing.Process(target=__process_server, args=(queue_ml_request, queue_ml_response, counted_scores))

    # Процессы ML моделей для расчета (N)
    ml_ops_proc = [
        multiprocessing.Process(
            target=__process_ml_score, args=(queue_ml_request, queue_ml_response), daemon=True
        ) for _ in range(multiprocessing.cpu_count() - 3)
    ]

    server_proc.start()
    _ = [p.start() for p in ml_ops_proc]

    server_proc.join()
    _ = [p.join() for p in ml_ops_proc]


if __name__ == "__main__":
    main()
