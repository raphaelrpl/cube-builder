#
# This file is part of Brazil Data Cube Collection Builder.
# Copyright (C) 2019-2020 INPE.
#
# Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

"""Defines the utility functions to use among celery tasks."""

import json
from concurrent.futures import ProcessPoolExecutor
from typing import Any, Dict, Tuple

from celery import current_app
from kombu.simple import SimpleQueue


Message = Tuple[Any, Any, Any]
ActivityMessages = Dict[str, Message]
"""Type which has keys as defined in models Activity (merge, blend, publish) and points to the body task messages."""


def list_running_tasks():
    """List all running tasks in celery cluster."""
    inspector = current_app.control.inspect()

    return inspector.active()


def list_pending_tasks():
    """List all pending tasks in celery cluster."""
    inspector = current_app.control.inspect()

    return inspector.reserved()


def _get_messages(queue: str, cube: str) -> ActivityMessages:
    """List all messages from a queue/data cube in broker server.

    Args:
        queue - Queue name
        cube - Filter tasks by data cube

    Notes:
        Do not use this method directly.
        Whenever you use this method, make sure to call using multiprocessing that close python process
        on exit, otherwise the messages in broker will be "unacked" and the celery worker
        may be stuck. Use `get_tasks_in_queue`.

        This method may take too long to respond.
    """
    with current_app.connection_or_acquire() as conn:
        q: SimpleQueue = conn.SimpleQueue(queue)

        messages = {}

        for _ in range(q.qsize()):
            message = q.get(block=False)

            parsed = json.loads(message.body)

            message_args, _, _ = parsed

            if len(message_args) == 2 and not isinstance(message_args[0], str):
                if message_args[0].get('collection_id') == cube:
                    activity_type = message_args[0]['activity_type']
                    messages.setdefault(activity_type, list())
                    messages[activity_type].append(parsed)

        q.close()

        return messages


def get_tasks_in_queue(queue: str, cube: str) -> ActivityMessages:
    """Try to get all tasks in broken context.

    Args:
        queue - Broker queue name
        cube - Data cube name to filter

    Notes:
        This method does not count tasks that is available in worker.
        See `cube_builder.celery.utils.list_pending_tasks` and `cube_builder.celery.utils.list_running_tasks` for
        further details or use `celery.control.inspect`.
    """
    with ProcessPoolExecutor(max_workers=1) as executor:
        task = executor.submit(_get_messages, queue, cube)

        tasks = task.result(10)

    return tasks
