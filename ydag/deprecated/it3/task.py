import asyncio
import inspect
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Generic, TypeVar, Callable, Dict, Any, List
from uuid import UUID, uuid4

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


class Task(Generic[OutputType], ABC):
    """Superclass of Task and FutureTaskResult"""

    def __init__(
        self, id: str | None = None, wait_on: List["Task"] | None = None
    ) -> None:
        self._id = id or "<empty_id>"
        self._is_independent = id is not None
        self._wait_on = wait_on or []
        self._state = State.CREATED
        self._result = Result[OutputType]()

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

    def _set_state(self, state: State):
        if self._state != state and self._is_independent:
            logging.debug(f"Task {self._id} moved to state {self._state}")
        self._state = state

    @staticmethod
    async def _get_results_for_tasks(run_id: UUID, tasks: Dict[str, "Task"]):
        logging.debug(f"Getting results for tasks {tasks}")
        async_tasks = []
        for task in tasks.values():
            async_tasks.append(asyncio.create_task(task.run(run_id)))
        results = await asyncio.gather(*async_tasks)

        return dict(zip(tasks.keys(), results))

    async def run(self, run_id: UUID = uuid4()) -> Result[OutputType]:
        self._set_state(State.WAITING)
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
        upstream_task_results: Dict[str, Result] = await self._get_results_for_tasks(
            run_id, upstream_tasks
        )

        failed_upstream_tasks = {
            k: v for k, v in upstream_task_results.items() if v.error is not None
        }
        if len(failed_upstream_tasks) > 0:
            self._on_upstream_fail(failed_upstream_tasks)
            return self._result

        upstream_task_values = {k: v.value for k, v in upstream_task_results.items()}
        final_run_kwargs = {
            kw: upstream_task_values.get(kw, arg) for kw, arg in run_kwargs.items()
        }
        self._set_state(State.RUNNING)
        try:
            logging.debug(f"Kwargs for {self._id}.run(): {final_run_kwargs}")
            self._result = Result[OutputType](value=await self._run(**final_run_kwargs))
            self._set_state(State.SUCCEEDED)
        except BaseException as e:
            self._result = Result[OutputType](error=e)
            self._set_state(State.FAILED)
        return self._result

    def run_sync(self, run_id: UUID = uuid4()) -> Result[OutputType]:
        return asyncio.run(self.run(run_id))

    def _on_upstream_fail(
        self, failed_upstream_task_results: "Dict[str, Result[Any]]"
    ) -> None:
        self._result = Result[OutputType](
            error=RunError(
                f"Failing task {self._id} because inputs on parameters {list(failed_upstream_task_results.keys())} failed."
            )
        )
        self._set_state(State.UPSTREAM_FAILED)

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
        logging.debug(f"Running transformation on {upstream_task}")
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
