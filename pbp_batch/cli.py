import argparse
import os
from pathlib import Path

from pbp_batch._deploy import ensure_infrastructure, open_ui, queue_run, set_ntfy_topic, show_logs, status, stop_all


def main():
    parser = argparse.ArgumentParser(
        description="pbp-batch: batch acoustic processing with Prefect and Dask.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  pbp-batch submit_job                     # start server and open Prefect UI\n"
            "  pbp-batch submit_job config.yaml         # queue a processing job\n"
            "  pbp-batch submit_job config.yaml --dask-workers 8\n"
            "  pbp-batch stop                           # shut down server and worker\n"
        ),
    )

    subparsers = parser.add_subparsers(dest="command")

    # ------------------------------------------------------------------
    # submit_job
    # ------------------------------------------------------------------
    run_parser = subparsers.add_parser(
        "submit_job",
        help=(
            "Queue a processing job. "
            "Omit YAML_FILE to start the server and open the Prefect UI."
        ),
    )
    run_parser.add_argument(
        "yaml_file",
        type=Path,
        nargs="?",
        default=None,
        metavar="YAML_FILE",
        help="Path to the YAML configuration file.",
    )
    run_parser.add_argument(
        "--dask-workers",
        type=int,
        default=None,
        metavar="N",
        help=(
            "Number of parallel Dask workers for per-day HMD processing. "
            "Defaults to the number of CPU cores on this machine."
        ),
    )
    run_parser.add_argument(
        "--no-compress-netcdf",
        action="store_true",
        default=False,
        help="Disable NetCDF compression (compression is enabled by default).",
    )
    run_parser.add_argument(
        "--no-quality-flag",
        action="store_true",
        default=False,
        help="Disable the quality flag variable in the NetCDF output (enabled by default).",
    )
    run_parser.add_argument(
        "--exclude-tone-calibration",
        type=int,
        default=0,
        metavar="seconds",
        help="Seconds to exclude from the start of each audio file (e.g. tone calibration). Default: 0.",
    )
    run_parser.add_argument(
        "--no-notifications",
        action="store_true",
        default=False,
        help="Disable phone notifications for this run (notifications are enabled by default).",
    )

    # ------------------------------------------------------------------
    # stop
    # ------------------------------------------------------------------
    subparsers.add_parser(
        "stop",
        help="Gracefully stop the Prefect server and worker.",
    )

    # ------------------------------------------------------------------
    # status
    # ------------------------------------------------------------------
    subparsers.add_parser(
        "status",
        help="Show server status and running/scheduled jobs.",
    )

    # ------------------------------------------------------------------
    # logs
    # ------------------------------------------------------------------
    logs_parser = subparsers.add_parser(
        "logs",
        help="Show the serve process log.",
    )
    logs_parser.add_argument(
        "-n", "--lines",
        type=int,
        default=50,
        metavar="N",
        help="Number of lines to show from the end of the log (default: 50).",
    )
    logs_parser.add_argument(
        "-f", "--follow",
        action="store_true",
        default=False,
        help="Follow the log in real time (like tail -f).",
    )

    # ------------------------------------------------------------------
    # gui
    # ------------------------------------------------------------------
    subparsers.add_parser(
        "gui",
        help="Open the graphical job submission interface in a browser.",
    )

    # ------------------------------------------------------------------
    # set-ntfy-topic
    # ------------------------------------------------------------------
    ntfy_parser = subparsers.add_parser(
        "set-ntfy-topic",
        help="Set the ntfy.sh topic for phone notifications (leave blank to disable).",
    )
    ntfy_parser.add_argument(
        "topic",
        nargs="?",
        default="",
        metavar="TOPIC",
        help="ntfy.sh topic name (e.g. 'pbp-yourname'). Omit to disable notifications.",
    )

    # ------------------------------------------------------------------
    # Dispatch
    # ------------------------------------------------------------------
    args = parser.parse_args()

    if args.command == "submit_job":
        if args.yaml_file is None:
            # No file given — start infrastructure and open the UI
            ensure_infrastructure()
            open_ui()
        else:
            if not args.yaml_file.exists():
                print(f"Error: '{args.yaml_file}' does not exist.")
                raise SystemExit(1)
            dask_workers = args.dask_workers or os.cpu_count() or 4
            ensure_infrastructure()
            queue_run(
                yaml_file=args.yaml_file,
                dask_workers=dask_workers,
                compress_netcdf=not args.no_compress_netcdf,
                add_quality_flag=not args.no_quality_flag,
                exclude_tone_calibration=args.exclude_tone_calibration,
                notifications=not args.no_notifications,
            )

    elif args.command == "stop":
        stop_all()

    elif args.command == "status":
        status()

    elif args.command == "logs":
        show_logs(lines=args.lines, follow=args.follow)

    elif args.command == "gui":
        from pbp_batch.gui import launch
        launch()

    elif args.command == "set-ntfy-topic":
        set_ntfy_topic(args.topic)

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
