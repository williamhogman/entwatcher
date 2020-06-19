import asyncio


async def amain():
    try:
        from .entwatcher import Entwatcher

        ew = Entwatcher()
        await ew.setup()


    except ex as Exception:
        print(ex)
    finally:
        await ew.wait_for_shutdown()



def main():
    asyncio.run(amain())


main()
