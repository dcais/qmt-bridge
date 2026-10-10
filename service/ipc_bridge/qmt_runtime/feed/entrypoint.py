"""Feed executor and QMT strategy entrypoint."""
from ..entrypoint import start
from ..executor import DeferredResult
from .api import bind_feed_apis, BATCH_METHODS, BatchJob, process_batch_request, dispatch_request


def create_feed_executor(context, apis, account_id, request_context):
    bind_feed_apis(apis)
    def execute(method, params):
        request = {'method': method, 'params': params}
        if method in BATCH_METHODS:
            job = BatchJob(request)
            def step():
                return process_batch_request(context, job), job.result
            return DeferredResult(step)
        return dispatch_request(context, request)
    return execute, None


def start_feed(context, apis):
    start(context, apis, 'feed', create_feed_executor)
