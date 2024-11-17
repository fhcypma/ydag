import asyncio
import inspect
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from time import time
from typing import Generic, TypeVar, Callable, Dict, Any, List
from uuid import uuid4, uuid1

InputType = TypeVar("InputType")
OutputType = TypeVar("OutputType")
TransformedType = TypeVar("TransformedType")


class State(Enum):
    CREATED = 0
    WAITING = (1,)
    RUNNING = (2,)
    # SKIPPED = (3,)
    SUCCEEDED = (4,)
    FAILED = (5,)
    UPSTREAM_FAILED = (6,)
    # UPSTREAM_SKIPPED = (7,)


FAILED_STATES = [State.FAILED, State.UPSTREAM_FAILED]
FINAL_STATES = [State.SUCCEEDED] + FAILED_STATES


@dataclass
class Result(Generic[OutputType]):
    """Outcome of a runnable class"""

    value: OutputType | None = None
    error: BaseException | None = None


class EmptyOutput:
    """Should use this as task definition without output instead of None"""

    pass


class RunError(RuntimeError):
    pass


class DagRun:
    """Class to hold all task results"""

    def __init__(self):
        self._run_id = uuid4()
        self._start_time = time()
        self._results: Dict[str, Result] = {}

    def add_result(self, task: "Task[InputType]", result: Result[InputType]):
        logging.debug(f"DagRun {self._run_id} received result {result} for task {task.id}")
        # if task.is_independent:
        self._results[task.id] = result

    def get_result(self, task: "Task[OutputType]") -> Result[OutputType]:
        return self._results[task.id]


class Task(Generic[OutputType], ABC):
    """Superclass of Task and FutureTaskResult"""

    def __init__(
            self, id: str | None = None, wait_on: List["Task"] | None = None
    ) -> None:
        self._id = id or str(uuid1())
        self._is_independent = id is not None
        self._wait_on = wait_on or []
        # self._state = State.CREATED
        # self._result = Result[OutputType]()

    @property
    def id(self):
        return self._id

    @property
    def is_independent(self):
        return self._is_independent

    @property
    def is_transparent(self):
        return not self._is_independent

    @abstractmethod
    async def _run(self, *args, **kwargs) -> OutputType:
        """Task to be executed"""
        raise NotImplementedError()

    def _get_run_kwargs(self) -> Dict[str, Any]:
        """Matches kwarg names to class variables"""
        arg_names = inspect.getfullargspec(self._run)[0]
        if len(arg_names) == 1:
            return {}

        arg_names = arg_names[1:]  # Remove 'self'
        return {arg: getattr(self, arg) for arg in arg_names}

    def _state_change(self, old_state: State, new_state: State) -> State:
        if old_state != new_state and self._is_independent:
            logging.debug(f"Task {self._id} moved to state {new_state}")
        return new_state

    @staticmethod
    async def _run_tasks(dag_run: DagRun, tasks: Dict[str, "Task"]) -> None:
        logging.debug(f"Running tasks {tasks}")
        async_tasks = []
        for task in tasks.values():
            async_tasks.append(asyncio.create_task(task.run(dag_run)))
        await asyncio.gather(*async_tasks)

    async def run(self, dag_run: DagRun) -> None:
        state = State.WAITING
        run_kwargs = self._get_run_kwargs()
        logging.debug(f"_run kwargs for {self._id}.run(): {run_kwargs}")
        upstream_tasks_for_input: Dict[str, Task] = {
            kw: arg for kw, arg in run_kwargs.items() if isinstance(arg, Task)
        }
        upstream_tasks_not_for_input: Dict[str, Task] = {
            f"wait_on_{i}": task for i, task in enumerate(self._wait_on)
        }
        upstream_tasks: Dict[str, Task] = (
                upstream_tasks_for_input | upstream_tasks_not_for_input
        )
        logging.debug(f"Upstream tasks for {self._id}.run(): {upstream_tasks}")
        await self._run_tasks(dag_run, upstream_tasks)

        upstream_task_results: Dict[str, Result] = {kw: dag_run.get_result(task) for kw, task in upstream_tasks.items()}
        failed_upstream_tasks: Dict[str, Result] = {
            k: v for k, v in upstream_task_results.items() if v.error is not None
        }
        if len(failed_upstream_tasks) > 0:
            if self.is_transparent:
                # There can only be one upstream task
                # Not wrapping in exception
                err = list(failed_upstream_tasks.values())[0].error
            else:
                # Wrapping exception in exception
                upstream_errors = [res.error for res in failed_upstream_tasks.values()]
                if len(upstream_errors) == 1:
                    upstream_errors = upstream_errors[0]
                err = RunError(
                    f"Failing task {self._id} because inputs on parameters {list(failed_upstream_tasks.keys())} failed.",
                    upstream_errors
                )
            res = Result[OutputType](
                error=err
            )
            state = self._state_change(state, State.UPSTREAM_FAILED)
            dag_run.add_result(self, res)
            return

        upstream_task_values = {k: v.value for k, v in upstream_task_results.items()}
        final_run_kwargs = run_kwargs | upstream_task_values
        state = self._state_change(state, State.RUNNING)
        try:
            logging.debug(f"Final kwargs for {self._id}._run(): {final_run_kwargs}")
            res = Result[OutputType](value=await self._run(**final_run_kwargs))
            dag_run.add_result(self, res)
            state = self._state_change(state, State.SUCCEEDED)

        except BaseException as e:
            res = Result[OutputType](error=e)
            state = self._state_change(state, State.FAILED)
            dag_run.add_result(self, res)

    def run_sync(self, dag_run: DagRun = DagRun()) -> DagRun:
        asyncio.run(self.run(dag_run))
        return dag_run

    def result(self) -> Result[OutputType]:
        """Wraps run"""
        if self._state not in FINAL_STATES:
            raise RunError(
                f"Requested result on task {self._id}, but task was not ready yet"
            )

        return self._result

    def transform(
            self, transformation_func: Callable[[OutputType], TransformedType]
    ) -> "TransformTask[OutputType, TransformedType]":
        return TransformTask(self, transformation_func)


class TransformTask(Generic[InputType, OutputType], Task[OutputType]):
    def __init__(
            self,
            input: Task[InputType],
            transformation_func: Callable[[InputType], OutputType],
    ):
        super().__init__()
        self.upstream_task = input
        self._transformation_func = transformation_func

    async def _run(self, upstream_task: InputType) -> OutputType:
        logging.debug(f"Running transformation on {upstream_task.id}")
        return self._transformation_func(upstream_task)


class FailableTask(Generic[OutputType], TransformTask[OutputType, OutputType]):
    def __init__(
            self,
            input: Task[OutputType],
            result_on_fail: OutputType,
    ):
        super().__init__(
            input=input,
            transformation_func=lambda x: x,
        )
        self._result_on_fail = result_on_fail

    def _on_upstream_fail(
            self, failed_upstream_task_results: Dict[str, Result[Any]]
    ) -> None:
        self._result = Result(value=self._result_on_fail)
        self._state = State.SUCCEEDED
