import asyncio

# This event is used to signal a graceful shutdown to all background tasks.
shutdown_event = asyncio.Event()
