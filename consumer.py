import asyncio
import redis
import redis.typing
import redis.asyncio as async_redis
from functools import wraps
from typing import (
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Generic,
    Optional,
    Protocol,
    Type,
    TypeVar,
    Union,
)
from typing_extensions import ParamSpec, Self
from types import TracebackType
from dataclasses import dataclass, field


T = TypeVar("T")
U = TypeVar("U")
R = TypeVar("R")
P = ParamSpec("P")
RET = TypeVar("RET", covariant=True)

T_OR_STOP = Union[T, StopAsyncIteration]
FN_REF = Callable[[], Awaitable[T]]
FN_REF_RESULT = tuple[FN_REF[T], T_OR_STOP[T]]

_SENTINEL = object()


async def _fn_ref_and_result(
    fn: FN_REF[T],
) -> FN_REF_RESULT[T]:
    try:
        result = await fn()
        return fn, result
    except StopAsyncIteration as e:
        return fn, e


class FanIn(Generic[T]):
    """Treat a variadic number of async iterators like a single async iterator."""

    def __init__(self, *iters: AsyncIterator[T]):
        self._queue: asyncio.Queue[Union[T, object]] = asyncio.Queue()
        self._iters = iters
        self._tasks: set[asyncio.Task[FN_REF_RESULT[T]]] = set()

    def _done_callback(self, t: asyncio.Task[FN_REF_RESULT[T]]):
        global _SENTINEL

        fn, result = t.result()
        self._tasks.remove(t)
        if isinstance(result, StopAsyncIteration):
            self._queue.put_nowait(_SENTINEL)
            return
        task = asyncio.create_task(_fn_ref_and_result(fn))
        task.add_done_callback(self._done_callback)
        self._tasks.add(task)

        self._queue.put_nowait(result)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]] = None,
        exc_value: Optional[BaseException] = None,
        exc_traceback: Optional[TracebackType] = None,
    ):
        await self.stop()

    def __aiter__(self) -> AsyncIterator[T]:
        for iterator in self._iters:
            self._tasks.add(asyncio.create_task(_fn_ref_and_result(iterator.__anext__)))

        for task in self._tasks:
            task.add_done_callback(self._done_callback)

        return self

    async def __anext__(self) -> T:
        while self._tasks:
            value = await self._queue.get()
            if value == _SENTINEL:
                continue
            return value
        raise StopAsyncIteration

    async def stop(self):
        for task in self._tasks:
            task.remove_done_callback(self._done_callback)
            task.cancel()


class Closer(Protocol):
    def is_closed(self) -> bool:
        ...

    async def close(self):
        """Calling this coroutine closes the Closer. Subsequent calls are noops."""
        ...


class AsyncCallable(Protocol, Generic[RET]):
    async def __call__(self) -> RET:
        ...


@dataclass
class BoolCloser:
    _closed: bool = field(default=False, init=False, repr=True)

    def is_closed(self) -> bool:
        return self._closed

    async def close(self):
        """Calling this method closes the Closer. Subsequent calls are noops."""
        self._closed = True


def partial_coro(
    fn: Callable[P, Awaitable[R]], *args: P.args, **kwargs: P.kwargs
) -> AsyncCallable[R]:
    @wraps(fn)
    async def wrapped_coro() -> R:
        result = await fn(*args, **kwargs)
        return result

    return wrapped_coro


class CoroLoop(Generic[R]):
    """
    Wraps an async callable in a loop and exposes it's results as an async iterator.

    Example:
    ```
    async def get_feed(url: str) -> dict[str, str]:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                return await response.json()

    async def poll_feed(url: str, poll_time: int = 5) -> dict[str, str]:
        await asyncio.sleep(poll_time)
        return await get_feed(url)

    async for response in CoroLoop.with_bool_closer(
        partial_coro(poll_feed, "https://api.weather.gov/alerts/active?area=AL")
    ):
        print(response)
    ```
    """

    def __init__(self, async_callable: AsyncCallable[R], closer: Closer):
        self._async_partial = async_callable
        self._closer: Closer = closer

    @classmethod
    def with_bool_closer(cls, async_callable: AsyncCallable[R]):
        return cls(async_callable=async_callable, closer=BoolCloser())

    def __aiter__(self) -> AsyncIterator[R]:
        return self

    async def __anext__(self) -> R:
        if self._closer.is_closed():
            raise StopAsyncIteration

        result = await self._async_partial()
        return result

    async def close(self):
        await self._closer.close()

    @property
    def closed(self) -> bool:
        return self._closer.is_closed()


@dataclass
class ConsumerMetadata:
    group_name: str
    consumer_name: str


StreamName = bytes
StreamEntryId = bytes
StreamEntryKey = bytes
StreamEntryValue = bytes
StreamEntry = tuple[StreamEntryId, dict[StreamEntryKey, StreamEntryValue]]
XReadGroupCollection = list[list[Union[StreamName, list[StreamEntry]]]]


@dataclass
class GroupReader:
    client: async_redis.Redis

    def xread_group(
        self,
        groupname: str,
        consumername: str,
        streams: Dict[redis.typing.KeyT, redis.typing.StreamIdT],
        count: Union[int, None] = None,
        block: Union[int, None] = None,
        noack: bool = False,
    ) -> CoroLoop[tuple[ConsumerMetadata, XReadGroupCollection]]:
        async def wrapper() -> tuple[ConsumerMetadata, XReadGroupCollection]:
            consumer_metadata = ConsumerMetadata(
                group_name=groupname, consumer_name=consumername
            )

            result = await self.client.xreadgroup(
                groupname=groupname,
                consumername=consumername,
                streams=streams,
                count=count,
                block=block,
                noack=noack,
            )
            return consumer_metadata, result

        return CoroLoop.with_bool_closer(wrapper)


async def create_group(
    client: async_redis.Redis,
    name: str,
    groupname: str,
    id: str = "$",
    mkstream: bool = False,
    entries_read: Optional[int] = None,
    ignore_exists: bool = True,
):
    try:
        await client.xgroup_create(
            name=name,
            groupname=groupname,
            id=id,
            mkstream=mkstream,
            entries_read=entries_read,
        )
    except redis.exceptions.ResponseError as e:
        if (
            not ignore_exists
            or e.args[0] != "BUSYGROUP Consumer Group name already exists"
        ):
            raise e


async def main():
    r = async_redis.Redis()
    print(f"Ping successful: {await r.ping()}")

    stream = "stream"
    groups = ["foo", "bar"]
    group_names = {"foo": "1", "bar": "1"}

    await asyncio.gather(
        *[
            create_group(
                client=r,
                name=stream,
                groupname=groupname,
                id="0",
                mkstream=True,
                ignore_exists=True,
            )
            for groupname in groups
        ]
    )
    print(f"created groups: {','.join(groups)!r}")

    gr = GroupReader(client=r)
    # create async iterator XREAD_GROUP readers
    readers: list[CoroLoop[tuple[ConsumerMetadata, XReadGroupCollection]]] = [
        gr.xread_group(
            groupname=groupname,
            consumername=group_names[groupname],
            streams={stream: "0"},
            count=1,
            block=1000 * 60 * 60,
            noack=False,
        )
        for groupname in groups
    ]

    i = 0
    async with FanIn(*readers) as async_iter:
        async for msg in async_iter:
            if i == 10:
                break
            print(msg)
            i += 1

    # all unfulfilled reader tasks are closed when the context manager exits. This covers
    # effectively stopping the iterators, but does not do any clean up work. You can imagine, for
    # example if this were close down logic, needing to clean up resources or perform other tasks.
    for r in readers:
        await r.close()


if __name__ == "__main__":
    asyncio.run(main())
