import entwatcher.deps as deps


async def shutdown():
    await (await deps.http_client()).aclose()
