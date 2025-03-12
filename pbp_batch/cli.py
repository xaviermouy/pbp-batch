import argparse
from pathlib import Path
from pbp_batch.core import submit_job

def cli_args_parser():
    parser = argparse.ArgumentParser(description="Run pbp job with an YAML configuration file.")
    parser.add_argument(
        "-y", "--yaml_file",
        type=Path,
        default=None,
        help="Path to the YAML configuration file. If not provided, a default config will be used."
    )
    args = parser.parse_args()
    if args.yaml_file and not args.yaml_file.exists():
        print(f"Error: The file '{args.yaml_file}' does not exist.")
        exit(1)
    return args
    
def main():
    args = cli_args_parser()
    submit_job(args.yaml_file)

if __name__ == "__main__":
    main()
    