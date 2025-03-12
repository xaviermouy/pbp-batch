import argparse
from pathlib import Path
from pbp_batch.core import submit_job
from pbp_batch._deploy import create_deployment  # Import deployment function

def main():
    parser = argparse.ArgumentParser(
        description="Run pbp-batch with a YAML configuration file."
    )

    subparsers = parser.add_subparsers(dest="command")

    # Run command
    run_parser = subparsers.add_parser("submit_job", help="Run pbp-batch processing")
    run_parser.add_argument(
        "-y", "--yaml_file",
        type=Path,
        default=None,
        help="Path to the YAML configuration file. If not provided, a default config will be used."
    )

    # Deploy command
    deploy_parser = subparsers.add_parser("deploy", help="Deploy the Prefect flow")
    
    args = parser.parse_args()

    if args.command == "submit_job":
        if args.yaml_file and not args.yaml_file.exists():
            print(f"Error: The file '{args.yaml_file}' does not exist.")
            exit(1)
        submit_job(args.yaml_file)

    elif args.command == "deploy":
        create_deployment()  # Run the Prefect deployment

    else:
        parser.print_help()