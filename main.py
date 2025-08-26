import asyncio
import csv
import logging
from multiprocessing import Process, Queue

from browser_handling import ParserTask
from config import Config
from models import QueueMessage, ProxyData
from utils import Utils as Ut

logger = logging.getLogger(__name__)


async def main():
    logging.basicConfig(level=logging.INFO,
                        format=u'%(filename)s:%(lineno)d #%(levelname)-8s [%(asctime)s] - %(name)s - %(message)s')

    input_queries = await Config.load_queries()
    if not input_queries:
        logger.error("Файл queries.txt пуст! Завершаю работу...")
        return

    input_proxies = await Config.load_proxies()

    pages_count = len((await Ut.calculate_pages_count(input_queries, Config.MAX_BROWSERS))[0])
    tasks = []
    for _ in range(Config.MAX_BROWSERS):
        queue_in, queue_out = Queue(), Queue()
        new_proc = Process(
            target=ParserTask, kwargs={
                "queue_in": queue_out, "queue_out": queue_in,
                "pages_count": pages_count if Config.MAX_PAGES_PER_BROWSER >= pages_count else Config.MAX_PAGES_PER_BROWSER
            }
        )
        new_proc.start()

        tasks.append({"process": new_proc, "queue_in": queue_in, "queue_out": queue_out})

    while True:
        await asyncio.sleep(2)

        for task in tasks:
            msg = await Ut.get_message_from_queue(queue=task["queue_in"])
            if msg is None:
                continue

            if msg.msg_type == Ut.GET_NEW_PROXY:
                for proxy in input_proxies:
                    if proxy.available:
                        proxy.available = False
                        task["queue_out"].put(QueueMessage(msg_type=Ut.SEND_NEW_PROXY, data=proxy))
                        break

                if isinstance(msg.data, ProxyData):
                    for proxy in input_proxies:
                        if proxy.id == msg.data.id:
                            proxy.available = True
                            break

            elif msg.msg_type == Ut.GET_NEW_QUERY:
                query_for_send = input_queries[0] if len(input_queries) else None
                task["queue_out"].put(QueueMessage(msg_type=Ut.SEND_NEW_QUERY, data=query_for_send))

                if len(input_queries):
                    input_queries.pop(0)

            elif msg.msg_type == Ut.UPLOAD_DATA:
                with open(str(Config.OUT_FILEPATH), "a", newline="", encoding="utf-8-sig") as file:
                    writer = csv.writer(file, delimiter=";")
                    writer.writerows(msg.data)


if __name__ == '__main__':
    asyncio.run(main())
