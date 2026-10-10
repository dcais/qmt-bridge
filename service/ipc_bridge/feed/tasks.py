"""External-process download task cache; lifecycle is intentionally in memory."""

import time
import uuid

from .qmt_api import FeedError

MAX_DOWNLOAD_TASKS = 1000


def download_task_public(task):
    public = {
        "task_id": task["task_id"],
        "status": task["status"],
        "stockcode": task["stockcode"],
        "period": task["period"],
        "startTime": task["startTime"],
        "endTime": task["endTime"],
        "created_at": task["created_at"],
        "updated_at": task["updated_at"],
        "message": task["message"],
    }
    if "incrementally" in task:
        public["incrementally"] = task["incrementally"]
    if task["error"] is not None:
        public["error"] = dict(task["error"])
    return public

def create_download_task(state, params):
    with state.download_tasks_lock:
        while len(state.download_tasks) >= MAX_DOWNLOAD_TASKS:
            evicted = False
            for task_id, task in list(state.download_tasks.items()):
                if task["status"] in ("completed", "failed"):
                    del state.download_tasks[task_id]
                    evicted = True
                    break
            if not evicted:
                raise FeedError(
                    429,
                    "DOWNLOAD_TASKS_FULL",
                    "download task cache is full",
                )

        task_id = "download-{0}".format(uuid.uuid4().hex)
        now = time.time()
        task = {
            "task_id": task_id,
            "status": "queued",
            "stockcode": params["stockcode"],
            "period": params["period"],
            "startTime": params["startTime"],
            "endTime": params["endTime"],
            "created_at": now,
            "updated_at": now,
            "message": "queued",
            "error": None,
        }
        if "incrementally" in params:
            task["incrementally"] = params["incrementally"]
        state.download_tasks[task_id] = task
        return download_task_public(task)

def remove_download_task(state, task_id):
    with state.download_tasks_lock:
        state.download_tasks.pop(task_id, None)

def update_download_task(state, task_id, status, message, error=None):
    with state.download_tasks_lock:
        task = state.download_tasks.get(task_id)
        if task is None:
            return None
        task["status"] = status
        task["updated_at"] = time.time()
        task["message"] = message
        task["error"] = error
        state.download_tasks.move_to_end(task_id)
        return download_task_public(task)

def get_download_task_snapshot(state, task_id):
    with state.download_tasks_lock:
        task = state.download_tasks.get(task_id)
        if task is None:
            raise FeedError(
                404,
                "DOWNLOAD_TASK_NOT_FOUND",
                "download task was not found",
            )
        return download_task_public(task)

def mark_download_task_failed(state, task_id, code, message):
    return update_download_task(
        state,
        task_id,
        "failed",
        message,
        {"code": code, "message": message},
    )
