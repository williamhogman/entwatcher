import asyncio


async def amain():
    from .entwatcher import Entwatcher

    ew = Entwatcher()
    await ew.setup()

    await ew.wait_for_shutdown()


def main():
    asyncio.run(amain())


main()
