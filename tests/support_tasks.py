import asyncio

from support_specs import ECHO, GRACED, SLOW

from taskline import task


@task(ECHO)
def echo(text: str) -> str:
    return text


@task(SLOW)
async def slow() -> None:
    await asyncio.sleep(60)


@task(GRACED)
def graced() -> None:
    return None
