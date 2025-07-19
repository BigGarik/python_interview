import time


def send_notifications(user_id: int, report_path: str, score: int):
    # NOTE: Эмуляция отправки СМС/PUSH уведомлений пользователю
    time.sleep(3)

    print(
        f"Уважаемый клиент user_id={user_id}, "
        f"Ваш рейтинг: {score}, "
        f"Ознакомиться с вашими контрактами вы можете по этой ссылке: {report_path}"
    )
