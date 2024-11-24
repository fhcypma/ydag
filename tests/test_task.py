import asyncio
import logging
import time

import pytest

from ydag.task import Task, TaskArg, DagRun, State


class ReturnOneTask(Task[int]):
    async def run(self) -> int:
        return 1


class ReturnTrueTask(Task[bool]):
    async def run(self) -> bool:
        return True


class FailTask(Task[bool]):
    async def run(self) -> bool:
        raise ValueError("This task always fails")


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


class WaitTask(Task[None]):
    def __init__(self, id: str, delay: int):
        super().__init__(id)
        self.delay = delay

    async def run(self) -> None:
        logging.debug(f"Waiting {self.delay} seconds")
        await asyncio.sleep(self.delay)
        logging.debug(f"Waited {self.delay} seconds")


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
        # And the task shoudl have a skip task
        assert task2.has_skip_task
        # And the task should be the skip_task
        assert task2.skip_task == task1

    def test_skip_no_task(self):
        # Given a task that should be skipped
        task2 = ReturnOneTask("one", skip=True)
        # Then the task should have the correct dependencies
        assert task2.upstream_tasks == []
        # And the task should have no skip task
        assert not task2.has_skip_task
        # And therefore, there would be no reason to gather the skip task
        with pytest.raises(ValueError):
            task2.skip_task


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
        # Given two tasks
        task1 = ReturnOneTask("one")
        task2 = AddOneTask("four", x=task1.transform(lambda x: x + 1).transform(lambda x: x + 1))
        # When the tasks are run
        run = DagRun()
        asyncio.run(run.run(task2))
        # Then the result should be correct
        assert run.get_result(task1).value == 1
        assert run.get_result(task2).value == 4

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

    def test_skip_first(self):
        # Given two tasks
        task1a = ReturnTrueTask("true")
        task1b = ReturnOneTask("one")
        # When a dependent task is created via skip
        task2 = AddOneTask("two", x=task1b, skip=task1a, check_skip_first=True)
        # When the tasks are run
        run = DagRun()
        asyncio.run(run.run(task2))
        # Then the dependent task should not have run
        task2_result = run.get_result(task2)
        assert task2_result.state == State.SKIPPED
        # And the upstream tasks should not have run
        # TODO this is not the correct behavior; make it CREATED
        with pytest.raises(KeyError):
            run.get_result(task1b)

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

    def test_failed_task(self):
        # Given a task that always fails
        task1 = FailTask("fail")
        # And a task that depends on the failed task
        task2 = AddOneTask("will_not_start", x=task1)
        # When the task is run
        run = DagRun()
        asyncio.run(run.run(task2))
        # Then the first task's result should be correct
        task1_result = run.get_result(task1)
        assert task1_result.state == State.FAILED
        assert isinstance(task1_result.error, ValueError)
        assert task1_result.value is None
        # And the second task's result should be correct
        task2_result = run.get_result(task2)
        assert task2_result.state == State.UPSTREAM_FAILED
        assert task2_result.error is None
        assert task2_result.value is None

    def test_failed_skip(self):
        # Given a task that always fails
        task1 = FailTask("fail")
        # And a task that depends via skip on the failed task
        task2 = ReturnOneTask("will_not_start", skip=task1)
        # When the task is run
        run = DagRun()
        asyncio.run(run.run(task2))
        # Then the first task's result should be correct
        task1_result = run.get_result(task1)
        assert task1_result.state == State.FAILED
        assert isinstance(task1_result.error, ValueError)
        assert task1_result.value is None
        # And the second task's result should be correct
        task2_result = run.get_result(task2)
        assert task2_result.state == State.UPSTREAM_FAILED
        assert task2_result.error is None
        assert task2_result.value is None

    @pytest.mark.asyncio
    async def test_run_task_twice(self):
        # Given a task
        task = WaitTask("wait", delay=1)
        # When the task is run twice
        run = DagRun()
        await run.run(task)
        tic = time.time()
        await run.run(task)
        toc = time.time()
        # Then the result should be correct
        assert run.get_result(task).value is None
        # And the execution should be done immediately
        assert toc - tic < 0.1

    @pytest.mark.asyncio
    async def test_task_already_started(self):
        # Given a task that will take some time to complete
        task = WaitTask("wait", delay=1)
        run = DagRun()
        # When the task is run twice concurrently, giving the first one some head start
        tic = time.time()
        result1 = asyncio.create_task(run.run(task))
        await asyncio.sleep(0.9)
        result2 = asyncio.create_task(run.run(task))
        await asyncio.gather(result1, result2)
        toc = time.time()
        # Then all should result as normal
        assert run.get_result(task).value is None
        # And execution should be done as soon as that first one was done
        assert toc - tic < 1.4
