import inspect
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, TypeVar, Callable, Dict, Any, List
from uuid import UUID, uuid4

InputType = TypeVar("InputType")
OutputType = TypeVar("OutputType")
TransformedType = TypeVar("TransformedType")


@dataclass
class Result(Generic[OutputType]):
    """Outcome of a runaable class"""

    value: OutputType | None = None
    error: BaseException | None = None


class EmptyOutput:
    """Should use this as task definition without output instead of None"""

    pass


class RunError(RuntimeError):
    pass


class Runnable(Generic[OutputType], ABC):
    """Superclass of Task and FutureTaskResult"""

    def __init__(self) -> None:
        self._result: Result[OutputType] | None = None

    @abstractmethod
    async def run(self, *args, **kwargs) -> OutputType:
        """Task to be executed"""
        raise NotImplementedError()

    @abstractmethod
    async def _run_and_set_result(self, run_id: UUID) -> None:
        raise NotImplementedError()

    async def result(self, run_id: UUID = uuid4()) -> Result[OutputType]:
        """Wraps run"""
        if not self._result:
            await self._run_and_set_result(run_id)

        if not self._result:
            raise RunError("Returning result, but result was not set")

        return self._result


class FutureTaskResult(Generic[InputType, OutputType], Runnable[OutputType]):
    def __init__(
        self,
        input: Runnable[InputType],
        transformation: Callable[[InputType], OutputType],
        alternative: OutputType | None = None,
    ):
        super().__init__()
        self._input = input
        self._transformation = transformation
        self._alternative = alternative

    async def run(self, **kwargs) -> OutputType:
        raise NotImplementedError()

    async def _run_and_set_result(self, run_id: UUID) -> None:
        upstream_result = await self._input.result()
        if not upstream_result.value:
            self._result = Result[OutputType](error=upstream_result.error)
        else:
            try:
                self._result = Result[OutputType](
                    value=self._transformation(upstream_result.value)
                )
            except BaseException as e:
                if self._alternative:
                    self._result = Result(value=self._alternative, error=e)
                else:
                    self._result = Result[OutputType](error=e)

    def or_else(self, alternative: OutputType):
        return FutureTaskResult[InputType, OutputType](
            input=self._input,
            transformation=self._transformation,
            alternative=alternative,
        )


class Task(Generic[OutputType], Runnable[OutputType], ABC):
    def __init__(self, id: str, wait_on: List["Task"] | None = None):
        super().__init__()
        self._id = id
        self._wait_on: List["Task"] = wait_on or []

    def _get_run_kwargs(self) -> Dict[str, Any]:
        arg_names = inspect.getfullargspec(self.run)[0]
        if len(arg_names) == 1:
            return {}

        arg_names = arg_names[1:]  # Remove 'self'
        return {arg: getattr(self, arg) for arg in arg_names}

    def _get_upstream_tasks(self) -> Dict[str, FutureTaskResult]:
        run_kwargs = self._get_run_kwargs()
        return {k: v for k, v in run_kwargs.items() if isinstance(v, FutureTaskResult)}

    async def _get_upstream_results(self, run_id: UUID) -> Dict[str, Result[Any]]:
        upstream_tasks = self._get_upstream_tasks()
        upstream_results = {
            k: (await v.result(run_id)).value for k, v in upstream_tasks.items()
        }
        return upstream_results  # type: ignore

    async def _run_and_set_result(self, run_id: UUID):
        logging.info(f"Running {self._id}")
        run_kwargs = self._get_run_kwargs() | await self._get_upstream_results(run_id)
        logging.debug(f"Kwargs for {self._id}.run(): {run_kwargs}")
        try:
            self._result = Result[OutputType](value=await self.run(**run_kwargs))
        except BaseException as e:
            self._result = Result[OutputType](error=e)

    def future_result(self) -> FutureTaskResult[OutputType, OutputType]:
        def identity(input: OutputType) -> OutputType:
            return input

        return self.future_transform(identity)

    def future_transform(self, transformation: Callable[[OutputType], TransformedType]):
        return FutureTaskResult(
            input=self,
            transformation=transformation,
        )
