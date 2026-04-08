import argparse
from pathlib import Path
#from pbp_batch.core import submit_job
import pbp_batch.core
from pbp_batch._deploy import create_deployment  # Import deployment function

def main():
    parser = argparse.ArgumentParser(
        description="Run pbp-batch with a YAML configuration file."
    )

    subparsers = parser.add_subparsers(dest="command")

    # Run command
    run_parser = subparsers.add_parser("submit_job", help="Run pbp-batch processing")
    run_parser.add_argument(
        "yaml_files",
        type=Path,
        default=None,
        nargs="+",
        help="Path to the YAML configuration files."
    )
    run_parser.add_argument(
        "--no-compress-netcdf",
        action="store_true",
        default=False,
        help="Disable NetCDF compression (compression is enabled by default)."
    )
    run_parser.add_argument(
        "--no-quality-flag",
        action="store_true",
        default=False,
        help="Disable the quality flag variable in the NetCDF output (enabled by default)."
    )
    run_parser.add_argument(
        "--exclude-tone-calibration",
        type=int,
        default=None,
        metavar="seconds",
        help="Number of seconds to exclude from the beginning of each audio file (e.g. to skip a tone calibration signal)."
    )

    # Deploy command
    deploy_parser = subparsers.add_parser("deploy", help="Deploy the Prefect flow")
    
    args = parser.parse_args()

    if args.command == "submit_job":
        for yaml_file in args.yaml_files:
            if yaml_file and not yaml_file.exists():
                print(f"Error: The file '{yaml_file}' does not exist.")
                exit(1)
        pbp_batch.core.submit_job(
            args.yaml_files,
            compress_netcdf=not args.no_compress_netcdf,
            add_quality_flag=not args.no_quality_flag,
            exclude_tone_calibration=args.exclude_tone_calibration,
        )

    elif args.command == "deploy":
        create_deployment()  # Run the Prefect deployment

    else:
        parser.print_help()

if __name__ == "__main__":
    main()