import yaml
import os
import urllib.request
from pathlib import Path
from datetime import datetime, timedelta
from argparse import Namespace
from pbp.meta_gen.main_meta_generator import run_main_meta_generator
from pbp.hmb_gen.main_hmb_generator import run_main_hmb_generator
from pbp.hmb_plot.main_plot import run_main_plot
import pandas as pd
import matplotlib.pyplot as plt

from prefect import flow, task, get_run_logger
from prefect.runtime import flow_run as _current_flow_run
from prefect_dask import DaskTaskRunner
from pydantic import ValidationError

from pbp_batch.config import PbpYamlConfig


def format_path(path):
    p = Path(path)
    return str(p.as_posix()).replace('/', '\\') if os.name == 'nt' else str(p)


@task(name="Write global attributes file",retries=4, retry_delay_seconds=30)
def write_pbp_globalAttributes_file(config):
    yaml_template = {
        "title": "", "Summary": "", "Conventions": "", "keywords": "",
        "keywords_vocabulary": "", "history": "", "source": "", "acknowledgement": "",
        "license": "", "standard_name_vocabulary": "", "date_created": "",
        "creator_name": "", "creator_email": "", "creator_url": "", "institution": "",
        "publisher_name": "", "publisher_email": "", "publisher_url": "",
        "geospatial_bounds": "", "comment": "", "time_coverage_start": "",
        "time_coverage_end": "", "time_coverage_duration": "", "time_coverage_resolution": "",
        "platform": "", "instrument": "", "cdm_data_type": "", "references": ""
    }
    yaml_template.update({key: config[key] for key in yaml_template if key in config})
    yaml_template.update({key: config['pbp_job_agent'][key] for key in yaml_template if key in config['pbp_job_agent']})

    orig_file_name = config['pbp_job_agent']['variable_attrs_orig']
    output_file = os.path.join(
        os.path.dirname(orig_file_name),
        config['pbp_job_agent']['output_prefix'] + 'globalAttributes_pbp.yaml'
    )
    with open(output_file, "w") as file:
        yaml.dump(yaml_template, file, default_flow_style=False, sort_keys=False)
    return output_file

@task(name="PBP Load YAML", retries=4, retry_delay_seconds=30)
def load_yaml_file(yaml_file):
    with open(yaml_file, "r") as file:
        config = yaml.safe_load(file)

    # Validate structure and field values before any processing
    try:
        PbpYamlConfig.model_validate(config)
    except ValidationError as e:
        lines = [f"Invalid configuration in '{yaml_file}':"]
        for err in e.errors():
            field = " -> ".join(str(loc) for loc in err["loc"])
            lines.append(f"  {field}: {err['msg']}")
        raise ValueError("\n".join(lines)) from None

    # Store original path before any URI conversion
    config['pbp_job_agent']['variable_attrs_orig'] = config['pbp_job_agent']['global_attrs']
    if os.name == "nt":
        def to_file_uri(p):
            return "file:" + Path(p).as_posix()
        config['pbp_job_agent']['audio_base_dir'] = to_file_uri(config['pbp_job_agent']['audio_base_dir'])
        config['pbp_job_agent']['global_attrs'] = to_file_uri(config['pbp_job_agent']['global_attrs'])
        config['pbp_job_agent']['variable_attrs'] = to_file_uri(config['pbp_job_agent']['variable_attrs'])
    if os.name == "posix":
        config['pbp_job_agent']['audio_base_dir'] = "file://" + format_path(config['pbp_job_agent']['audio_base_dir'])
        config['pbp_job_agent']['global_attrs'] = "file://" + format_path(config['pbp_job_agent']['global_attrs'])
        config['pbp_job_agent']['variable_attrs'] = "file://" + format_path(config['pbp_job_agent']['variable_attrs'])

    config['pbp_job_agent']['json_base_dir'] = format_path(config['pbp_job_agent']['json_base_dir'])
    if config['pbp_job_agent'].get('xml_dir', ''):
        print(f"WARNING: 'xml_dir' is defined in the YAML file but will be ignored. "
              f"The current version of pbp does not use XML files for metadata generation.")
    config['pbp_job_agent']['nc_output_dir'] = format_path(config['pbp_job_agent']['nc_output_dir'])
    config['pbp_job_agent']['meta_output_dir'] = format_path(config['pbp_job_agent']['meta_output_dir'])
    config['pbp_job_agent']['log_dir'] = format_path(config['pbp_job_agent']['log_dir'])
    if config['pbp_job_agent']['sensitivity_uri'] == '':
        config['pbp_job_agent']['sensitivity_uri'] = None
    if config['pbp_job_agent']['sensitivity_flat_value'] == '':
        config['pbp_job_agent']['sensitivity_flat_value'] = None
    if config['pbp_job_agent']['voltage_multiplier'] == '':
        config['pbp_job_agent']['voltage_multiplier'] = None
    if not config['pbp_job_agent']['output_prefix'].endswith('_'):
        config['pbp_job_agent']['output_prefix'] += '_'
    config['pbp_job_agent']['subset_to'] = [int(num) for num in config['pbp_job_agent']['subset_to'].split()]
    config['pbp_job_agent']['latlon'] = tuple(map(float, config['pbp_job_agent']['latlon'].split()))
    config['pbp_job_agent']['cmlim'] = tuple(map(float, config['pbp_job_agent']['cmlim'].split()))
    config['pbp_job_agent']['ylim'] = tuple(map(float, config['pbp_job_agent']['ylim'].split()))
    return config


@task(name="PBP MetaGen",retries=4, retry_delay_seconds=30)
def run_pbp_meta_gen(recorder=None, uri=None, output_dir=None, json_base_dir=None,
                     start=None, end=None, prefix=None):
    args = {
        "recorder": recorder, "uri": uri, "output_dir": output_dir,
        "json_base_dir": json_base_dir, "start": start, "end": end, "prefix": [prefix]
    }
    run_main_meta_generator(Namespace(**args))


@task(name="PBP HMD Gen daily", retries=4, retry_delay_seconds=30)
def run_pbp_hmd_gen(json_base_dir=None, audio_base_dir=None, date=None, output_dir=None,
                    prefix=None, sensitivity_uri=None, sensitivity_flat_value=None,
                    voltage_multiplier=None, subset_to=None, global_attrs=None,
                    variable_attrs=None, compress_netcdf=True, add_quality_flag=True,
                    exclude_tone_calibration=0):
    args = {
        "json_base_dir": json_base_dir,
        "audio_base_dir": audio_base_dir,
        "date": date,
        "output_dir": output_dir,
        "output_prefix": prefix,
        "global_attrs": global_attrs,
        "variable_attrs": variable_attrs,
        "subset_to": subset_to,
        "sensitivity_flat_value": float(sensitivity_flat_value) if sensitivity_flat_value is not None else None,
        "sensitivity_uri": sensitivity_uri,
        "voltage_multiplier": float(voltage_multiplier) if voltage_multiplier is not None else None,
        "s3_unsigned": False,
        "s3": False,
        "gs": False,
        "assume_downloaded_files": False,
        "retain_downloaded_files": False,
        "max_segments": float(0),
        "audio_path_map_prefix": "",
        "audio_path_prefix": "",
        "compress_netcdf": compress_netcdf,
        "add_quality_flag": add_quality_flag,
        "exclude_tone_calibration": exclude_tone_calibration,
        "input_file": None,
        "timestamp_pattern": None,
        "time_resolution": None,
        "download_dir": None,
        "set_global_attrs": None,
    }
    run_main_hmb_generator(Namespace(**args))


@flow(name="PBP HMD Batch", timeout_seconds=21600)
def run_pbp_hmd_gen_batch(json_base_dir=None, audio_base_dir=None, start=None, end=None,
                           output_dir=None, prefix=None, sensitivity_uri=None,
                           sensitivity_flat_value=None, voltage_multiplier=None,
                           subset_to=None, global_attrs=None, variable_attrs=None,
                           compress_netcdf=True, add_quality_flag=True,
                           exclude_tone_calibration=0):
    """Process each day in the date range as a parallel Dask task."""
    get_run_logger().info("Dask dashboard: http://localhost:8787")
    date_format = "%Y%m%d"
    start_date = datetime.strptime(start, date_format)
    end_date = datetime.strptime(end, date_format)
    delta = timedelta(days=1)

    futures = {}
    while start_date <= end_date:
        date_str = start_date.strftime(date_format)
        f = run_pbp_hmd_gen.submit(
            json_base_dir=json_base_dir,
            audio_base_dir=audio_base_dir,
            date=date_str,
            output_dir=output_dir,
            prefix=prefix,
            sensitivity_uri=sensitivity_uri,
            sensitivity_flat_value=sensitivity_flat_value,
            voltage_multiplier=voltage_multiplier,
            subset_to=subset_to,
            global_attrs=global_attrs,
            variable_attrs=variable_attrs,
            compress_netcdf=compress_netcdf,
            add_quality_flag=add_quality_flag,
            exclude_tone_calibration=exclude_tone_calibration,
        )
        futures[date_str] = f
        start_date += delta

    # Wait for all days to complete before raising — every day gets a full attempt
    # (including retries) regardless of whether other days fail.
    failed = []
    for date_str, f in futures.items():
        try:
            f.result()
        except Exception:
            failed.append(date_str)

    if failed:
        raise RuntimeError(
            f"{len(failed)} of {len(futures)} day(s) failed after retries: "
            f"{', '.join(failed)}"
        )


@task(name="PBP daily plots",retries=4, retry_delay_seconds=30)
def run_pbp_main_plot(netcdf_dir, latlon=None, title="", ylim=0, cmlim=None, dpi=100,
                      show=False, only_show=False, engine="h5netcdf"):
    # Reset plotting overrides set in pypam
    # (these cause the PBP plot_dataset_summary function to fail
    # https://github.com/mbari-org/pbp/issues/21#issuecomment-2261642486)
    plt.rcParams.update({"text.usetex": False})
    pd.plotting.deregister_matplotlib_converters()

    nc_files_path_list = list(Path(netcdf_dir).rglob("*.nc"))
    nc_files_str_list = [str(p) for p in nc_files_path_list]
    args = {
        "latlon": latlon, "title": title, "ylim": ylim, "cmlim": cmlim,
        "dpi": dpi, "show": show, "only_show": only_show,
        "engine": engine, "netcdf": nc_files_str_list
    }
    run_main_plot(Namespace(**args))


@task(name="Output audit", retries=4, retry_delay_seconds=30)
def audit_outputs(nc_output_dir: str, output_prefix: str, start: str, end: str):
    """Check that every day in the date range produced exactly one .nc and one .jpg file."""
    logger = get_run_logger()
    date_format = "%Y%m%d"
    start_date = datetime.strptime(start, date_format)
    end_date = datetime.strptime(end, date_format)

    expected_dates = []
    d = start_date
    while d <= end_date:
        expected_dates.append(d.strftime(date_format))
        d += timedelta(days=1)

    nc_files  = list(Path(nc_output_dir).rglob("*.nc"))
    jpg_files = list(Path(nc_output_dir).rglob("*.jpg"))

    missing_nc  = []
    missing_jpg = []
    for date_str in expected_dates:
        if not any(date_str in f.name for f in nc_files):
            missing_nc.append(date_str)
        if not any(date_str in f.name for f in jpg_files):
            missing_jpg.append(date_str)

    n = len(expected_dates)
    logger.info(f"Output audit: {n} day(s) expected, "
                f"{n - len(missing_nc)} .nc found, "
                f"{n - len(missing_jpg)} .jpg found.")

    if missing_nc:
        logger.warning(f"Missing .nc files ({len(missing_nc)}): {', '.join(missing_nc)}")
    if missing_jpg:
        logger.warning(f"Missing .jpg files ({len(missing_jpg)}): {', '.join(missing_jpg)}")

    if not missing_nc and not missing_jpg:
        logger.info("All expected output files are present.")


def _submit_job_run_name() -> str:
    yaml_file = _current_flow_run.parameters.get("yaml_file", "")
    return Path(yaml_file).stem if yaml_file else "pbp-job"


# ---------------------------------------------------------------------------
# ntfy.sh notifications
# ---------------------------------------------------------------------------

def _ntfy(title: str, message: str, priority: str, tags: str) -> None:
    """POST a notification to ntfy.sh. Silently skipped if NTFY_TOPIC is not set."""
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    if not topic:
        return
    try:
        req = urllib.request.Request(
            f"https://ntfy.sh/{topic}",
            data=message.encode(),
            headers={"Title": title, "Priority": priority, "Tags": tags},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=10)
    except Exception:
        pass  # notifications are best-effort — never crash the flow


def _notify_completion(flow, flow_run, state):
    if not flow_run.parameters.get("notifications", True):
        return
    _ntfy(
        title="pbp-batch: completed",
        message=flow_run.name,
        priority="default",
        tags="white_check_mark",
    )


def _notify_failure(flow, flow_run, state):
    if not flow_run.parameters.get("notifications", True):
        return
    _ntfy(
        title="pbp-batch: failed",
        message=f"{flow_run.name}\n{state.message}",
        priority="high",
        tags="x",
    )


def _notify_crashed(flow, flow_run, state):
    if not flow_run.parameters.get("notifications", True):
        return
    _ntfy(
        title="pbp-batch: crashed",
        message=flow_run.name,
        priority="urgent",
        tags="boom",
    )


@flow(name="pbp-job", flow_run_name=_submit_job_run_name, timeout_seconds=21600,
      on_completion=[_notify_completion],
      on_failure=[_notify_failure],
      on_crashed=[_notify_crashed])
def submit_job(yaml_file: str, dask_workers: int = 4, compress_netcdf: bool = True,
               add_quality_flag: bool = True, exclude_tone_calibration: int = 0,
               notifications: bool = True):
    """
    Top-level Prefect flow for a single pbp-batch deployment.

    Parameters
    ----------
    yaml_file : str
        Path to the YAML configuration file.
    dask_workers : int
        Number of parallel Dask workers for per-day HMD generation.
    compress_netcdf : bool
        Enable NetCDF compression (default True).
    add_quality_flag : bool
        Add quality flag variable to NetCDF output (default True).
    exclude_tone_calibration : int, optional
        Seconds to exclude from start of each audio file.
    """
    config = load_yaml_file(Path(yaml_file))
    new_global_attrs = write_pbp_globalAttributes_file(config)

    run_pbp_meta_gen(
        recorder=config['pbp_job_agent']['recorder'],
        uri=config['pbp_job_agent']['audio_base_dir'],
        output_dir=config['pbp_job_agent']['meta_output_dir'],
        json_base_dir=config['pbp_job_agent']['json_base_dir'],
        start=config['pbp_job_agent']['start'],
        end=config['pbp_job_agent']['end'],
        prefix=config['pbp_job_agent']['prefix'],
    )

    # Run each day in parallel using Dask; task_runner is set dynamically
    # so dask_workers can be a runtime parameter rather than fixed at decoration time.
    run_pbp_hmd_gen_batch.with_options(
        flow_run_name=Path(yaml_file).stem,
        task_runner=DaskTaskRunner(
            cluster_kwargs={
                "n_workers": dask_workers,
                "threads_per_worker": 1,
                "processes": True,  # process-based workers for CPU-bound audio processing
                "dashboard_address": ":8787",
            }
        )
    )(
        json_base_dir=config['pbp_job_agent']['json_base_dir'],
        audio_base_dir=config['pbp_job_agent']['audio_base_dir'],
        start=config['pbp_job_agent']['start'],
        end=config['pbp_job_agent']['end'],
        output_dir=config['pbp_job_agent']['nc_output_dir'],
        prefix=config['pbp_job_agent']['output_prefix'],
        sensitivity_uri=config['pbp_job_agent']['sensitivity_uri'],
        sensitivity_flat_value=config['pbp_job_agent']['sensitivity_flat_value'],
        voltage_multiplier=config['pbp_job_agent']['voltage_multiplier'],
        subset_to=config['pbp_job_agent']['subset_to'],
        global_attrs=new_global_attrs,
        variable_attrs=config['pbp_job_agent']['variable_attrs'],
        compress_netcdf=compress_netcdf,
        add_quality_flag=add_quality_flag,
        exclude_tone_calibration=exclude_tone_calibration,
    )

    run_pbp_main_plot(
        netcdf_dir=config['pbp_job_agent']['nc_output_dir'],
        latlon=config['pbp_job_agent']['latlon'],
        title=config['pbp_job_agent']['title'],
        ylim=config['pbp_job_agent']['ylim'],
        cmlim=config['pbp_job_agent']['cmlim'],
    )

    audit_outputs(
        nc_output_dir=config['pbp_job_agent']['nc_output_dir'],
        output_prefix=config['pbp_job_agent']['output_prefix'],
        start=config['pbp_job_agent']['start'],
        end=config['pbp_job_agent']['end'],
    )
