import asyncio
from typing import List


async def generate_big_report(user_id: int, contracts: List[dict]):
    """Симуляция генерации большого отчета в daemon режиме.
    """

    # NOTE: Эмуляция генерации большого отчета
    await asyncio.sleep(0.5)

    return f"/fake/path/report_{user_id}.pdf"
