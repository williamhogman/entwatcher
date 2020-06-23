import asyncio


async def amain():
    ew = None
    try:
        from .entwatcher import Entwatcher

        ew = Entwatcher()
        await ew.setup()
    finally:
        if ew is not None:
            await ew.wait_for_shutdown()


def main():
    asyncio.run(amain())


main()
