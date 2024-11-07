import asyncio
import logging
from typing import Any

from ydag.deprecated.it2.task import Task, FutureTaskResult


logging.basicConfig(level=logging.DEBUG)


class GetOneTask(Task[int]):
    async def run(self) -> int:
        return 1


class GetATask(Task[str]):
    async def run(self) -> str:
        return "a"


class WaitTask(Task[None]):
    def __init__(self, id: str, delay: int | FutureTaskResult[Any, int]):
        super().__init__(id)
        self.delay = delay

    async def run(self, delay: int) -> None:
        await asyncio.sleep(delay)


class DoNothingTask(Task[None]):
    async def run(self) -> None:
        pass


one = GetOneTask("one")
wait1sec = WaitTask(id="wait1sec", delay=one.future_result())
wait2sec = WaitTask(id="wait2sec", delay=one.future_transform(lambda x: x + 1))
final = DoNothingTask(id="final", wait_on=[wait1sec, wait2sec])

res = asyncio.run(final.result())
if res.error:
    raise res.error
print(f"Result is {res}")
