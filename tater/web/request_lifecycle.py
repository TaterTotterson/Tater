"""Keep external model requests tied to the lifetime of their HTTP caller."""

from __future__ import annotations

import asyncio

from fastapi import HTTPException


async def run_until_disconnected(request, operation):
    """Cancel this operation, including queued/local generation, on disconnect."""
    task = asyncio.create_task(operation)

    async def watch():
        # The route has already consumed its JSON body. Waiting for ASGI's
        # disconnect message also works through BaseHTTPMiddleware, where the
        # non-blocking Request.is_disconnected probe can miss the notification.
        while True:
            message = await request.receive()
            if message["type"] == "http.disconnect":
                return

    watcher = asyncio.create_task(watch())
    try:
        done, _ = await asyncio.wait((task, watcher), return_when=asyncio.FIRST_COMPLETED)
        if task in done:
            return task.result()
        watcher.result()
        raise HTTPException(
            status_code=499, detail="The model request was cancelled by its caller."
        )
    finally:
        for pending in (watcher, task):
            if not pending.done():
                pending.cancel()
        await asyncio.gather(watcher, task, return_exceptions=True)
