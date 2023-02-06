import asyncio
import tracemalloc
from queries import db_start

db = db_start()
db.create_tables()  # TODO check if tables exist, create if not.

from fetch import fetch


def app():
    asyncio.run(fetch())


if __name__ == '__main__':
    app()
