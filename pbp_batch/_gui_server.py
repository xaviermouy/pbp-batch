"""
GUI server script — started automatically by pbp-batch gui, do not call directly.
"""
import os
os.environ["PREFECT_API_URL"] = "http://127.0.0.1:4200/api"
os.environ["PREFECT_SERVER_ALLOW_EPHEMERAL_MODE"] = "false"
os.environ["MPLBACKEND"] = "Agg"

from pbp_batch.gui import app, on_refresh  # noqa: E402
import panel as pn

if __name__ == "__main__":
    on_refresh(None)
    pn.serve(app, title="pbp-batch", show=False, port=5007, threaded=True)
