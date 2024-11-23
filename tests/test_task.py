import asyncio
import logging

from ydag.task import Task, TaskArg, DagRun, State


class ReturnOneTask(Task[int]):
    async def run(self) -> int:
        return 1


class ReturnTrueTask(Task[bool]):
    async def run(self) -> bool:
        return True


class AddOneTask(Task[int]):
    def __init__(self, id: str, *, x: TaskArg[int], **kwargs):
        super().__init__(id, **kwargs)
        self.x = x

    async def run(self, x: int) -> int:
        return x + 1


class AddTask(Task[int]):
    def __init__(self, id: str, *, x: TaskArg[int], y: TaskArg[int], **kwargs):
        super().__init__(id, **kwargs)
        self.x = x
        self.y = y

    async def run(self, x: int, y: int) -> int:
        return x + y


class IncrementTask(Task[int]):
    def __init__(self, id: str, **kwargs):
        super().__init__(id, **kwargs)
        self.x = 0

    async def run(self, x: int) -> int:
        self.x += 1
        return self.x


class TestTask:
    def test_no_upstream_tasks(self):
        # Given a task
        task = ReturnOneTask("one")
        # Then the task should have no dependencies
        assert task.upstream_tasks == []

    def test_single_upstream_task(self):
        # Given a task
        task1 = ReturnOneTask("one")
        # When a dependent task is created
        task2 = AddOneTask("two", x=task1)
        # Then the dependent task should have the correct dependencies
        assert task2.upstream_tasks == [task1]

    def test_transformed_upstream_task(self):
        # Given a task
        task1 = ReturnOneTask("one")
        # When a dependent task is created
        task2 = AddOneTask("three", x=task1.transform(lambda x: x + 1))
        # Then the dependent task should have the correct dependencies
        assert task2.upstream_tasks == [task1]

    def test_wait_on_upstream(self):
        # Given a task
        task1 = ReturnOneTask("one")
        # When a dependent task is created via wait_on
        task2 = ReturnOneTask("also_one", wait_on=[task1])
        # Then the dependent task should have the correct dependencies
        assert task2.upstream_tasks == [task1]

    def test_skip_upstream(self):
        # Given a task
        task1 = ReturnTrueTask("true")
        # When a dependent task is created via skip
        task2 = ReturnOneTask("one", skip=task1)
        # Then the dependent task should have the correct dependencies
        assert task2.upstream_tasks == [task1]

    def test_skip_no_task(self):
        # Given a task that should be skipped
        task2 = ReturnOneTask("one", skip=True)
        # Then the task should have the correct dependencies
        assert task2.upstream_tasks == []


class TestDagRun:
    def test_simple_dag_run(self):
        # Given two tasks
        task1 = ReturnOneTask("one")
        task2 = AddOneTask("two", x=task1)
        # When the tasks are run
        run = DagRun()
        asyncio.run(run.run(task2))
        # Then the result should be correct
        assert run.get_result(task1).value == 1
        assert run.get_result(task2).value == 2

    def test_simple_dag_run_with_transformation(self):
        logging.basicConfig(level=logging.DEBUG)
        # Given two tasks
        task1 = ReturnOneTask("one")
        task2 = AddOneTask("three", x=task1.transform(lambda x: x + 1))
        # When the tasks are run
        run = DagRun()
        asyncio.run(run.run(task2))
        # Then the result should be correct
        assert run.get_result(task1).value == 1
        assert run.get_result(task2).value == 3

    def test_simple_dag_run_with_linked_transformation(self):
        logging.basicConfig(level=logging.DEBUG)
        # Given two tasks
        task1 = ReturnOneTask("one")
        task2 = AddOneTask("five", x=task1.transform(lambda x: x + 1).transform(lambda x: x + 2))
        # When the tasks are run
        run = DagRun()
        asyncio.run(run.run(task2))
        # Then the result should be correct
        assert run.get_result(task1).value == 1
        assert run.get_result(task2).value == 5

    def test_simple_skip(self):
        # Given two tasks where the upstream one must be skipped
        task1 = ReturnOneTask("one", skip=True)
        task2 = AddOneTask("two", x=task1)
        # When the tasks are run
        run = DagRun()
        asyncio.run(run.run(task2))
        # Then the tasks should be skipped
        assert run.get_result(task1).state == State.SKIPPED
        assert run.get_result(task2).state == State.UPSTREAM_SKIPPED

    def test_dependent_skip_task(self):
        # Given three tasks where the second one must be skipped based on the first
        task1 = ReturnTrueTask("true")
        task2 = ReturnOneTask("one", skip=task1)
        task3 = AddOneTask("two", x=task2)
        # When the tasks are run
        run = DagRun()
        asyncio.run(run.run(task3))
        # Then the tasks should be skipped
        assert run.get_result(task1).value is True
        assert run.get_result(task2).state == State.SKIPPED
        assert run.get_result(task3).state == State.UPSTREAM_SKIPPED

    def test_single_execution(self):
        # Given a task with 2 downstream tasks
        task1 = IncrementTask("one")
        task2a = AddOneTask("two", x=task1)
        task2b = AddOneTask("also_two", x=task1)
        task3 = AddTask("four", x=task2a, y=task2b)
        # When the tasks are run
        run = DagRun()
        asyncio.run(run.run(task3))
        # Then the first task should be run once
        assert run.get_result(task1).value == 1
        assert run.get_result(task3).value == 4
