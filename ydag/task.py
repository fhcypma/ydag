import asyncio
import inspect
import logging
from abc import abstractmethod, ABC
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, TypeAlias, Callable
from typing import Generic, List
from typing import TypeVar
from uuid import uuid4


class State(Enum):
    CREATED = 0
    WAITING = (1,)
    RUNNING = (2,)
    SKIPPED = (3,)
    SUCCEEDED = (4,)
    FAILED = (5,)
    UPSTREAM_FAILED = (6,)
    UPSTREAM_SKIPPED = (7,)


InputType = TypeVar("InputType")
OutputType = TypeVar("OutputType")
TransformedType = TypeVar("TransformedType")


@dataclass
class TaskTransform(Generic[InputType, OutputType]):
    """Task with a transformation applied to the output"""

    task: "Task[InputType] | TaskTransform[Any, InputType]"
    transformation: Callable[[InputType], OutputType] | None = None
    alternative: OutputType | None = None

    def apply(self, upstream_result: "TaskResult[Any]") -> "TaskResult[OutputType]":
        """Apply the transformation to the task result"""
        if isinstance(self.task, TaskTransform):
            upstream_result = self.task.apply(upstream_result)
        if self.alternative is not None and upstream_result.error:
            return TaskResult[OutputType](value=self.alternative)
        return TaskResult[OutputType](value=self.transformation(upstream_result.value))  # type: ignore # TODO fix

    def transform(self, func: Callable[[OutputType], TransformedType]) -> "TaskTransform[OutputType, TransformedType]":
        """Apply a transformation to the result of this task"""
        return TaskTransform[OutputType, TransformedType](task=self, transformation=func)

    def or_else(self, alternative: OutputType) -> "TaskTransform[OutputType, OutputType]":
        """Apply a default value if the task fails"""
        return TaskTransform[OutputType, OutputType](task=self, alternative=alternative)

    @property
    def upstream_task(self) -> "Task[Any]":
        """Get the upstream task"""
        if isinstance(self.task, TaskTransform):
            return self.task.upstream_task
        return self.task

    def __eq__(self, another):
        """Test for equality to use object as key in a set"""
        return isinstance(another, TaskTransform) and self.__hash__() == another.__hash__()

    def __hash__(self):
        """Hash function to use object as key in a set"""
        return hash(hash(self.transformation) + self.task.__hash__())


@dataclass
class TaskResult(Generic[OutputType]):
    """Outcome of a runnable class"""

    state: State = State.CREATED
    value: OutputType | None = None
    error: BaseException | None = None


class DagRun:
    """Class to hold all task state and results"""

    def __init__(self):
        self._run_id = uuid4()
        self._running_tasks: Dict[Task, Any] = {}
        self._results: Dict[Task, TaskResult] = {}

    async def run(self, task: "Task[OutputType]") -> None:
        """
        Run a task and store the result in the dag run
        """
        logging.debug(f"{self._run_id} - Running task {task.id}")

        # Task was already completed
        if task in self._results.keys():
            logging.debug(f"{self._run_id} - Task {task.id} already completed")
            return

        # Task was already started
        if task in self._running_tasks.keys():
            logging.debug(f"{self._run_id} - Task {task.id} already started")
            return await self._running_tasks[task]

        # If the task can be skipped directly
        if not task.has_skip_task and task.should_be_skipped:
            logging.debug(f"{self._run_id} - Task {task.id} should be skipped")
            self._results[task] = TaskResult(state=State.SKIPPED)
            return

        # If the skip task needs to run first, run it
        if task.needs_to_run_skip_task_first:
            logging.debug(f"{self._run_id} - For task {task.id} waiting for skip task {task.skip_task.id}")
            await self.run(task.skip_task)

            if self._results[task.skip_task].value:
                logging.debug(f"{self._run_id} - Task {task.id} should be skipped")
                self._results[task] = TaskResult(state=State.SKIPPED)
                return

        # Run all upstream tasks in parallel
        # TODO do not await all, but re-evaluate next step each time a task finishes
        await asyncio.gather(*[asyncio.create_task(self.run(task))
                               for task in task.upstream_tasks])

        # Gather the results of the upstream tasks
        run_kwargs_before = task.get_run_kwargs_before_execution()
        try:
            # If gather_result fails, it means that the transformation(s) fai(s);
            # which should fail the current task
            run_kwargs_after_task_run: Dict[str, Any] = {
                kw: self._gather_value(arg) for kw, arg in run_kwargs_before.items()
            }
        except BaseException as e:
            logging.warning(f"{self._run_id} - Task {task.id} failed")
            self._results[task] = TaskResult(error=e, state=State.FAILED)

        # If any upstream task failed, mark this task as failed
        if any(self._results[upstream_task].state in [State.FAILED, State.UPSTREAM_FAILED]
               for upstream_task in task.upstream_tasks):
            logging.debug(f"{self._run_id} - Task {task.id} failed because of upstream task(s)")
            self._results[task] = TaskResult(state=State.UPSTREAM_FAILED)
            return

        # If any upstream task was skipped, mark this task as skipped
        if any(self._results[upstream_task].state in [State.SKIPPED, State.UPSTREAM_SKIPPED]
               for upstream_task in task.upstream_tasks):
            logging.debug(f"{self._run_id} - Task {task.id} skipped because of upstream task(s)")
            self._results[task] = TaskResult(state=State.UPSTREAM_SKIPPED)
            return

        # Test again if this task should be skipped
        if not task.needs_to_run_skip_task_first and task.has_skip_task and self._results[task.skip_task].value:
            logging.debug(f"{self._run_id} - Task {task.id} should be skipped")
            self._results[task] = TaskResult(state=State.SKIPPED)
            return

        # Run the task
        try:
            result = await task.run(**run_kwargs_after_task_run)
            logging.debug(f"{self._run_id} - Task {task.id} succeeded")
            self._results[task] = TaskResult(value=result, state=State.SUCCEEDED)
        except BaseException as e:
            logging.warning(f"{self._run_id} - Task {task.id} failed")
            self._results[task] = TaskResult(error=e, state=State.FAILED)

    def get_result(self, task: "Task[OutputType]") -> TaskResult[OutputType]:
        """
        Get the result of a task
        """
        return self._results[task]

    def _gather_value(self, task_arg: "TaskArg[OutputType]") -> OutputType | None:
        """
        Get the result of a task, applying transformations if necessary
        """
        if isinstance(task_arg, Task):
            return self.get_result(task_arg).value
        if isinstance(task_arg, TaskTransform):
            task_result = self.get_result(task_arg.upstream_task)
            transformed_value = task_arg.apply(task_result).value
            return transformed_value
        logging.debug(f"{self._run_id} - Getting value {task_arg}")
        return task_arg


class Task(Generic[OutputType], ABC):
    def __init__(
            self,
            id: str,
            *,
            wait_on: List["Task[Any]"] | None = None,
            skip: "Task[bool] | bool" = False,
            check_skip_first: bool = False,
    ):
        """
        Runnable node in a DAG.

        :param id: The unique identifier of the task
        :param wait_on: Upstream tasks that need to complete before
        :param skip: If the outcome of this task is true, this task should be skipped
        :param check_skip_first: If the "skip" task should be completed before the upstream tasks are run
        """
        self._id = id
        self._wait_on = wait_on or []
        self._skip = skip
        self._check_skip_first = check_skip_first

    @property
    def id(self) -> str:
        """Unique identifier of the task"""
        return self._id

    @property
    def needs_to_run_skip_task_first(self) -> bool:
        """If the skip task should be completed before the upstream tasks are run"""
        return isinstance(self._skip, Task) and self._check_skip_first

    @property
    def has_skip_task(self) -> bool:
        """If an upstream task defined whether this task should be skipped"""
        return isinstance(self._skip, Task)

    @property
    def should_be_skipped(self) -> bool:
        """If this task should be skipped"""
        if isinstance(self._skip, bool):
            return self._skip
        raise ValueError(f"Skip is not a bool for task {self._id}")

    @property
    def skip_task(self) -> "Task[bool]":
        """Get upstream task that defines whether this task should be skipped"""
        if isinstance(self._skip, Task):
            return self._skip
        raise ValueError(f"No skip task defined for task {self._id}")

    @abstractmethod
    async def run(self, *args, **kwargs) -> OutputType:
        pass

    def get_run_kwargs_before_execution(self) -> Dict[str, Any]:
        """
        Constructs _run() kwargs from class variables with matching names..

        E.g., if _run() has the signature `async def _run(self, x: Task[int], y: int):`,
        this method will return {'x': self.x, 'y': self.y}.
        """
        arg_names = inspect.getfullargspec(self.run)[0]
        if len(arg_names) == 1:
            return {}

        arg_names = arg_names[1:]  # Remove 'self'
        return {arg: getattr(self, arg) for arg in arg_names}

    @property
    def upstream_tasks(self) -> List["Task[Any]"]:
        """
        Gathers all upstream tasks that need to trigger before execution
        """
        run_kwargs = self.get_run_kwargs_before_execution()
        return (
                ([self._skip] if isinstance(self._skip, Task) else [])
                + self._wait_on
                + [
                    arg for arg in run_kwargs.values()
                    if isinstance(arg, Task)
                ]
                + [
                    arg.upstream_task for arg in run_kwargs.values()
                    if isinstance(arg, TaskTransform)
                ])

    def transform(self, func: Callable[[OutputType], TransformedType]) -> TaskTransform[OutputType, TransformedType]:
        """
        Apply a transformation to the result of this task
        """
        return TaskTransform[OutputType, TransformedType](task=self, transformation=func)

    def or_else(self, alternative: OutputType) -> "TaskTransform[OutputType, OutputType]":
        """Apply a default value if the task fails"""
        return TaskTransform[OutputType, OutputType](task=self, alternative=alternative)

    def __eq__(self, another):
        """Test for equality to use object as key in a set"""
        return isinstance(another, Task) and self._id == another.id

    def __hash__(self):
        """Hash function to use object as key in a set"""
        return hash(self._id)


TaskArg: TypeAlias = OutputType | Task[OutputType] | TaskTransform[Any, OutputType]
"""Input type for task init parameters that accept static and dynamic values"""
