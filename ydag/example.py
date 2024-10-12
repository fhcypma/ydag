# https://stackoverflow.com/questions/78670718/how-to-execute-a-dag-of-tasks-using-async-io

from random import randrange
import asyncio


# Helper function for the creation of simple sample coroutine
def make_sample_coro(n):
    async def coro():
        print(f"Start of task {n} ...")
        await asyncio.sleep(randrange(1, 5))
        print(f"... End of task {n}")

    return coro


async def main():
    # Simple graph in standard representation (node => neighbours)
    graph = {1: {2, 5}, 2: {3}, 3: {4}, 4: set(), 5: {4}}
    tasks = {n: make_sample_coro(n) for n in graph}
    tasks_done = set()

    async def execute_task(ID):
        print(f"Trying to execute task {ID} ...")
        predecessors = {n for n, ns in graph.items() if ID in ns}
        while not predecessors <= tasks_done:  # Check if task can be started
            await asyncio.sleep(0.1)
        await tasks[ID]()
        tasks_done.add(ID)

    await asyncio.gather(*[execute_task(n) for n in graph])
    print("... Finished")


asyncio.run(main())
