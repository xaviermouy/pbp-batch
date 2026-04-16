"""
Panel-based GUI for pbp-batch job submission.

Launch with:  pbp-batch gui
"""

import os
import yaml
from datetime import datetime
from pathlib import Path

import panel as pn

pn.extension(sizing_mode="stretch_width")

# ── Widgets ───────────────────────────────────────────────────────────────────

yaml_path = pn.widgets.TextInput(
    name="",
    placeholder="Full path to YAML config file",
)
browse_btn = pn.widgets.Button(name="Browse…", width=90)

workers = pn.widgets.IntInput(
    name="Dask workers",
    value=os.cpu_count() or 4,
    start=1,
    end=64,
    width=100,
)
tone_cal = pn.widgets.IntInput(
    name="Exclude tone calibration (s)",
    value=0,
    start=0,
    width=120,
)
compress = pn.widgets.Checkbox(name="Compress NetCDF", value=True)
quality  = pn.widgets.Checkbox(name="Add quality flag", value=True)
notify   = pn.widgets.Checkbox(name="Send notifications", value=True)

validate_btn = pn.widgets.Button(name="Validate", button_type="primary", width=120)
submit_btn   = pn.widgets.Button(name="Submit Job", button_type="success", width=120, disabled=True)
refresh_btn  = pn.widgets.Button(name="↻ Refresh", width=100)

validation_pane = pn.pane.Markdown(
    "",
    styles={"background": "#f5f5f5", "padding": "12px", "border-radius": "4px"},
    visible=False,
)
status_pane = pn.pane.Markdown("_Click ↻ Refresh to check infrastructure status._")


# ── Callbacks ─────────────────────────────────────────────────────────────────

def on_browse(event):
    """Open a native file dialog (local mode only)."""
    try:
        import tkinter as tk
        from tkinter import filedialog
        root = tk.Tk()
        root.withdraw()
        root.call("wm", "attributes", ".", "-topmost", True)
        path = filedialog.askopenfilename(
            title="Select YAML config file",
            filetypes=[("YAML files", "*.yaml *.yml"), ("All files", "*.*")],
        )
        root.destroy()
        if path:
            yaml_path.value = path
    except Exception:
        pass  # tkinter unavailable in remote/server mode — user types the path


def on_validate(event):
    path_str = yaml_path.value.strip()
    validation_pane.visible = True

    if not path_str:
        validation_pane.object = "⚠️ Please select a YAML config file."
        submit_btn.disabled = True
        return

    if not Path(path_str).exists():
        validation_pane.object = f"❌ File not found: `{path_str}`"
        submit_btn.disabled = True
        return

    try:
        with open(path_str) as f:
            config = yaml.safe_load(f)
        from pbp_batch.config import PbpYamlConfig
        PbpYamlConfig.model_validate(config)
    except ValueError as ex:
        validation_pane.object = f"❌ **Invalid configuration**\n\n```\n{ex}\n```"
        submit_btn.disabled = True
        return
    except Exception as ex:
        validation_pane.object = f"❌ **Unexpected error:** {ex}"
        submit_btn.disabled = True
        return

    agent  = config["pbp_job_agent"]
    start  = agent["start"]
    end    = agent["end"]
    ndays  = (datetime.strptime(end, "%Y%m%d") - datetime.strptime(start, "%Y%m%d")).days + 1

    validation_pane.object = (
        f"✅ **Configuration valid**\n\n"
        f"| | |\n"
        f"|---|---|\n"
        f"| Recorder | `{agent['recorder']}` |\n"
        f"| Date range | `{start}` → `{end}` ({ndays} day(s)) |\n"
        f"| Output prefix | `{agent['output_prefix']}` |\n"
        f"| Audio dir | `{agent['audio_base_dir']}` |\n"
        f"| NetCDF output | `{agent['nc_output_dir']}` |"
    )
    submit_btn.disabled = False


def on_submit(event):
    path = Path(yaml_path.value.strip())
    exclude = tone_cal.value

    submit_btn.disabled = True
    submit_btn.name = "Submitting…"
    try:
        from pbp_batch._deploy import ensure_infrastructure, queue_run
        ensure_infrastructure()
        queue_run(
            yaml_file=path,
            dask_workers=workers.value,
            compress_netcdf=compress.value,
            add_quality_flag=quality.value,
            exclude_tone_calibration=exclude,
            notifications=notify.value,
        )
        validation_pane.object += f"\n\n✅ **Job queued:** `{path.name}`"
        on_refresh(None)
    except Exception as ex:
        validation_pane.object += f"\n\n❌ **Submit failed:** {ex}"
    finally:
        submit_btn.name = "Submit Job"
        submit_btn.disabled = False


def on_refresh(event):
    try:
        from pbp_batch._deploy import is_server_running, is_serve_running, _is_port_open
        server_ok = is_server_running()
        serve_ok  = is_serve_running()
        dask_ok   = _is_port_open(8787)

        server_str = "🟢 [http://127.0.0.1:4200](http://127.0.0.1:4200)" if server_ok else "🔴 stopped"
        serve_str  = "🟢 running" if serve_ok else "🔴 stopped"
        dask_str   = "🟢 [http://localhost:8787](http://localhost:8787)" if dask_ok else "🔴 stopped"

        status_pane.object = (
            f"| | |\n|---|---|\n"
            f"| **Prefect server** | {server_str} |\n"
            f"| **Serve process**  | {serve_str} |\n"
            f"| **Dask dashboard** | {dask_str} |"
        )
    except Exception as ex:
        status_pane.object = f"_Status unavailable: {ex}_"


def on_yaml_path_change(event):
    """Reset validation state whenever the YAML path changes."""
    validation_pane.visible = False
    validation_pane.object  = ""
    submit_btn.disabled     = True


browse_btn.on_click(on_browse)
validate_btn.on_click(on_validate)
submit_btn.on_click(on_submit)
refresh_btn.on_click(on_refresh)
yaml_path.param.watch(on_yaml_path_change, "value")


# ── Layout ────────────────────────────────────────────────────────────────────

app = pn.template.FastListTemplate(
    title="pbp-batch",
    accent="#1a5276",
    main=[
        pn.Column(
            # — Config file ——————————————————————————————————————
            pn.pane.Markdown("### Configuration file"),
            pn.Row(yaml_path, pn.Spacer(width=8), browse_btn, align="end"),

            pn.layout.Divider(),

            # — Processing parameters ————————————————————————————
            pn.pane.Markdown("### Processing parameters"),
            pn.Row(workers, pn.Spacer(width=20), tone_cal),
            pn.Row(compress, pn.Spacer(width=20), quality, pn.Spacer(width=20), notify),

            pn.layout.Divider(),

            # — Actions ——————————————————————————————————————————
            pn.Row(validate_btn, pn.Spacer(width=8), submit_btn),
            validation_pane,

            pn.layout.Divider(),

            # — Infrastructure status ————————————————————————————
            pn.pane.Markdown("### Infrastructure status"),
            pn.Row(status_pane, pn.Spacer(), refresh_btn, align="center"),

            sizing_mode="stretch_width",
            max_width=720,
        )
    ],
)


def launch():
    import sys
    import time
    import webbrowser
    from pbp_batch._deploy import _start_background_process, _kill_port, PID_DIR

    # Kill any stale GUI server before starting a fresh one
    _kill_port(5007, "Existing GUI server")
    time.sleep(0.5)  # brief pause so the port is fully released

    gui_script = str(Path(__file__).parent / "_gui_server.py")
    pid = _start_background_process([sys.executable, gui_script], log_name="gui")
    (PID_DIR / "gui.pid").write_text(str(pid))

    # Wait briefly for the server to start before opening the browser
    url = "http://localhost:5007"
    print(f"Starting GUI server", end="", flush=True)
    for _ in range(10):
        time.sleep(0.5)
        print(".", end="", flush=True)
        try:
            import urllib.request
            urllib.request.urlopen(url, timeout=1)
            break
        except Exception:
            pass
    print(f" ready.")
    webbrowser.open(url)
    print(f"GUI available at {url}  (PID {pid})")
