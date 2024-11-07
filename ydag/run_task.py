import asyncio
import logging
import time

from ydag.task import Task


logging.basicConfig(level=logging.DEBUG)


class GetOneTask(Task[int]):
    async def _run(self) -> int:
        return 1


class GetATask(Task[str]):
    async def _run(self) -> str:
        return "a"


class WaitTask(Task[None]):
    def __init__(self, id: str, delay: int | Task[int]):
        super().__init__(id)
        self.delay = delay

    async def _run(self, delay: int) -> None:
        logging.debug(f"Waiting {delay} seconds")
        await asyncio.sleep(delay)


class DoNothingTask(Task[None]):
    async def _run(self) -> None:
        pass


tic = time.time()
one = GetOneTask("one")
wait1sec = WaitTask(id="wait1sec", delay=one)
tf = one.transform(lambda x: x + 1)
wait2sec = WaitTask(id="wait2sec", delay=tf)
final = DoNothingTask(id="final", wait_on=[wait1sec, wait2sec])

res = final.run_sync()
toc = time.time()
if res.error:
    raise res.error
print(f"Result is {res.value}")
print(f"In {toc - tic}s")
