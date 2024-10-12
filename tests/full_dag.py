import asyncio
import logging
from datetime import datetime

from ydag.dag import Dag, Task, TaskResult, DagInput


logging.basicConfig(
    format="%(asctime)s %(levelname)8s %(name)s %(message)s",
    level=logging.DEBUG,
)


class WaitTask(Task[None]):
    def __init__(self, *, delay: int | TaskResult[int] | DagInput[int], **kwargs):
        super().__init__(**kwargs)
        self.delay = delay

    async def run(self, delay: int) -> None:
        await asyncio.sleep(delay)


class FailTask(Task[None]):
    async def run(self) -> None:
        raise RuntimeError("Some exception")


class GetOneTask(Task[int]):
    async def run(self) -> int:
        return 1


class LogTask(Task[None]):
    def __init__(
        self,
        *,
        log: str | TaskResult[str] | DagInput[str],
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.log = log

    async def run(self, log: str) -> None:
        res = log
        print(res)


class GetDateTask(Task[datetime]):
    async def run(self) -> datetime:
        return datetime.now()


with Dag[int](id="my_dag") as dag:
    today = GetDateTask(id="today")
    one = GetOneTask(id="one")
    wait1 = WaitTask(id="wait1", delay=dag.input())
    wait2 = WaitTask(id="wait2", delay=2)
    log = LogTask(
        id="log", log=today.transform(lambda x: str(x)), await_tasks=[wait1, wait2]
    )
    dag_run = dag.run(input=2)

logging.info(dag_run)
dag_run.raise_any_error()
