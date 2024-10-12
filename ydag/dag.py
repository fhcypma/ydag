import asyncio
import inspect
import logging
import uuid
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass
from typing import Any
from typing import List, Dict, Generic, TypeVar, Callable

import networkx as nx

from ydag.trigger import Trigger, State


class DagFailedError(RuntimeError):
    pass


class TaskFailedError(RuntimeError):
    pass


class DagDefinitionError(RuntimeError):
    pass


class DagContext:
    """
    DAG context is used to keep the current DAG when DAG is used as ContextManager.

    You can use DAG as context:

    .. code-block:: python

        with DAG(...) as dag:
            ...

    If you do this the context stores the DAG and whenever new task is created, it will use
    such stored DAG as the parent DAG.

    """

    _context_managed_dags: deque["Dag"] = deque()

    @classmethod
    def push_context_managed_dag(cls, dag: "Dag"):
        cls._context_managed_dags.appendleft(dag)

    @classmethod
    def pop_context_managed_dag(cls) -> "Dag | None":
        dag = cls._context_managed_dags.popleft()
        return dag

    @classmethod
    def get_current_dag(cls) -> "Dag":
        try:
            return cls._context_managed_dags[0]
        except IndexError as e:
            logging.error("No DAG defined")
            raise e


U = TypeVar("U")
InputType = TypeVar("InputType")
OutputType = TypeVar("OutputType")


class TaskException(RuntimeError):
    pass


class TaskResultError(RuntimeError):
    pass


@dataclass
class TaskResult(Generic[OutputType]):
    task: "Task[Any]"
    transformation: Callable[[Any], OutputType]


class Task(ABC, Generic[OutputType]):
    def __init__(
        self,
        *,
        id: str,
        dag: "Dag | None" = None,
        soft_fail: bool = False,
        trigger: Trigger = Trigger.ALL_SUCCESS,
        await_tasks: List["Task"] | None = None,
    ):
        self.id = id
        self.dag: Dag = dag or DagContext.get_current_dag()
        self.dag.add(self)
        self.soft_fail = soft_fail
        self.trigger = trigger
        self.results: List[TaskResult] = []
        for task in await_tasks or []:
            task >> self

    async def gather_kwargs_and_run(self, dag_run: "DagRun") -> OutputType:
        """
        Derives the kwargs for the run() method of this task.
        * Gathers list of keywords from run() method inspection
        * Gets value for task attrs of same name(s)
        * If value is instance of TaskResult, gather task result from correct task instance in dag run
        """
        arg_names = inspect.getfullargspec(self.run)[0]
        if len(arg_names) == 1:
            kwargs = {}
        else:
            arg_names = arg_names[1:]  # Remove 'self'
            task_attributes = {arg: getattr(self, arg) for arg in arg_names}
            kwargs = {
                arg: (
                    attr.transformation(dag_run.get_result_for_task(attr.task))
                    if isinstance(attr, TaskResult)
                    else (
                        attr.transformation(dag_run.input)
                        if isinstance(attr, DagInput)
                        else attr
                    )
                )
                for arg, attr in task_attributes.items()
            }
        res = await self.run(**kwargs)
        return res

    @abstractmethod
    async def run(self, **kwargs) -> OutputType:
        raise NotImplementedError()

    def _add_predecessor(self, previous_task: "Task"):
        """Schedule this task after the given one"""
        self.dag.add_dependency(previous_task, self)

    def __rshift__(self, other_task: "Task") -> "Task":
        """Create dependency. Return right task to link multiple: a >> b >> c"""
        other_task._add_predecessor(self)
        return other_task

    def result(self) -> TaskResult[OutputType]:
        """Create future result to be used by downstream task"""

        def func(x: OutputType) -> OutputType:
            return x

        return self.transform(func)

    def transform(self, transformation: Callable[[OutputType], U]) -> TaskResult[U]:
        """"""
        result = TaskResult(task=self, transformation=transformation)
        self.results.append(result)
        return result


class TaskInstance(Generic[OutputType]):
    def __init__(
        self,
        *,
        task: Task[OutputType],
        dag_run: "DagRun",
    ):
        self.task: Task = task
        self.dag_run = dag_run
        self._log = logging.getLogger(__name__ + f" {dag_run.run_id} {task.id}")
        self._state = State.CREATED
        self._async_task: asyncio.tasks.Task[OutputType | None] = asyncio.create_task(
            self._execute()
        )
        self._result: OutputType | None = None
        self._error: RuntimeError | None = None

    def __str__(self):
        return str(
            {
                "task_id": self.task.id,
                "state": str(self._state),
                "result": self._result,
                "error": self._error,
            }
        )

    @staticmethod
    def _state_change(previous_state: State, next_state: State) -> None:
        """Perform action based on previous or next state"""
        pass

    def _next_state(self):
        """Determine next action (state) for this TaskRun, depending on its predecessors' states"""
        predecessor_task_run_states = self.dag_run.get_predecessor_task_instance_states(
            self
        )
        if predecessor_task_run_states:
            next_state = self.task.trigger.next_state(predecessor_task_run_states)
        else:
            # No predecessors; trunk task
            next_state = State.RUNNING

        if next_state != self._state:
            self._state_change(self._state, next_state)
        return next_state

    @property
    def result(self) -> OutputType:
        if not self._result:
            raise TaskResultError(
                f"Task {self.task.id} was not yet completed, but result was "
            )
        return self._result

    @property
    def state(self):
        return self._state

    @property
    def error(self):
        return self._error

    @property
    def async_task(self):
        return self._async_task

    async def run_and_save(self) -> OutputType:
        try:
            return await self.task.gather_kwargs_and_run(self.dag_run)
        except Exception as e:
            raise TaskFailedError(f"Task {self.task.id} failed", e)

    async def _execute(self) -> OutputType | None:
        self._log.debug(f"Waiting task {self.task.id}")
        self._state = State.WAITING

        while self._next_state() == State.WAITING:
            await asyncio.sleep(0.1)
        next_state = self._next_state()

        if next_state == State.UPSTREAM_FAILED:
            self._log.debug(f"Failing task {self.task.id} due to upstream")
            self._state = State.UPSTREAM_FAILED

        elif next_state == State.RUNNING:
            self._log.debug(f"Starting task {self.task.id}")
            self._state = State.RUNNING
            try:
                self._result = await self.run_and_save()
                self._log.debug(f"Completed task {self.task.id}")
                self._state = State.SUCCEEDED
                return self._result
            except RuntimeError as e:
                self._error = e
                if self.task.soft_fail:
                    self._log.warning(f"Failed. Skipping task {self.task.id}")
                    self._state = State.SKIPPED
                else:
                    self._log.warning(f"Failed task {self.task.id}")
                    self._state = State.FAILED

        else:
            raise NotImplementedError(
                f"Not sure which state to use; next state for {self.task.id} was {next_state}"
            )

        return None


@dataclass
class DagInput(Generic[InputType]):
    transformation: Callable[[InputType], Any] = lambda x: x


class Dag(Generic[InputType]):
    def __init__(self, *, id: str):
        self.id: str = id
        self._graph = nx.DiGraph()

    def input(
        self, transform: Callable[[InputType], Any] = lambda x: x
    ) -> DagInput[InputType]:
        return DagInput[InputType](transform)

    def add(self, task: Task):
        self._graph.add_node(task.id, task=task)

    def add_dependency(self, first_task: Task, second_task):
        self._graph.add_edge(first_task.id, second_task.id)
        if not nx.is_directed_acyclic_graph(self._graph):
            raise DagDefinitionError(
                f"Cannot create dependency between {first_task.id} and {second_task.id}; {self.id} is not longer a DAG"
            )

    def predecessor_ids(self, task: Task) -> List[str]:
        return self._graph.predecessors(task.id)

    def tasks(self) -> List[Task]:
        return [node[1]["task"] for node in self._graph.nodes.data()]

    def run(self, input: InputType | None = None):
        dag_run = DagRun(dag=self, input=input)
        return dag_run

    def __enter__(self):
        DagContext.push_context_managed_dag(self)
        return self

    def __exit__(self, _type, _value, _tb):
        DagContext.pop_context_managed_dag()


class DagRun(Generic[InputType]):
    def __init__(
        self,
        *,
        dag: Dag,
        input: InputType,
    ):
        self._run_id = uuid.uuid4()
        self._log = logging.getLogger(__name__ + f" {self.run_id}")
        self._dag: Dag = dag
        self._input = input
        self._state = State.CREATED
        self._task_instances: Dict[str, TaskInstance] = {}
        asyncio.run(self._execute())

    @property
    def run_id(self):
        return self._run_id

    @property
    def dag(self):
        return self._dag

    @property
    def input(self):
        return self._input

    async def _execute(self):
        self._log.info(f"Starting dag {self._dag.id}")
        self.state = State.RUNNING
        self.task_instances: Dict[str, TaskInstance] = {
            task.id: TaskInstance(task=task, dag_run=self) for task in self._dag.tasks()
        }
        await asyncio.gather(*[ti.async_task for ti in self.task_instances.values()])
        self._log.info(f"Completed dag {self._dag.id}")
        self.state = State.SUCCEEDED

    def get_predecessor_task_instance_states(self, task_run) -> List[State]:
        """Get list of states of all predecessors of the given TaskInstance"""
        predecessor_task_ids = self._dag.predecessor_ids(task_run.task)
        predecessor_task_run_states = [
            ti.state
            for ti in self.task_instances.values()
            if ti.task.id in predecessor_task_ids
        ]
        return predecessor_task_run_states

    def get_result_for_task(self, task: Task[OutputType]) -> OutputType:
        res = self.task_instances[task.id].result
        return res

    def __str__(self):
        return str(
            {
                "run_id": str(self._run_id),
                "tis": [f"{ti}" for ti in self.task_instances.values()],
            }
        )

    def raise_any_error(self):
        """Raises DagFailedError if any task failed"""
        errors = [ti.error for ti in self.task_instances.values() if ti.error]
        if errors:
            raise DagFailedError(errors)
