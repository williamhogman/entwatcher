import databases

DATABASE_URL = "sqlite:///./test.db"
database = databases.Database(DATABASE_URL)

SETUP_QUERIES = [
]

async def setup():
    await database.connect()
    for query in SETUP_QUERIES:
        await database.execute(query=query)


async def teardown():
    await database.disconnect()
