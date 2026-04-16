"""
Background serve script — started automatically by pbp-batch, do not call directly.

Sets PREFECT_API_URL *before* any Prefect imports so the serve process connects
to the persistent local server (port 4200) rather than starting an ephemeral one.
"""
import os
os.environ["PREFECT_API_URL"] = "http://127.0.0.1:4200/api"
os.environ["PREFECT_SERVER_ALLOW_EPHEMERAL_MODE"] = "false"
os.environ["PREFECT_LOGGING_LEVEL"] = "DEBUG"
os.environ["MPLBACKEND"] = "Agg"  # non-interactive backend — required for use in Dask worker processes and Prefect task threads
# NTFY_TOPIC is injected by _deploy.py from ~/.pbp_batch/ntfy_topic.txt — set with: pbp-batch set-ntfy-topic <topic>

from pbp_batch.core import submit_job  # noqa: E402

if __name__ == "__main__":
    # limit=1 ensures jobs are processed sequentially (one at a time).
    # Additional submitted runs queue up and are picked up when the current one finishes.
    submit_job.serve(name="pbp-batch", limit=1)
