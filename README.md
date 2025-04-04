# pbp-batch

Wrapper for the mbari-pbp allowing to create metadata files, HMD computation, and daily plots for each deployment. Supports multiprocessing.

## Installation

To install the latest version:

```bash
pip install git+https://github.com/xaviermouy/pbp-batch.git
```

To install a specific version (tag):

```bash
pip install git+https://github.com/xaviermouy/pbp-batch.git@v0.0.1
```

## Usage

Use `pbp_batch submit_job` followed by the path to the YAML files to process (separated by a space character). Each dataset (associated with a YAML file) is processed in parallel.

```bash
pbp_batch submit_job "C:\pbp_test_dataset\globalAttributes_file_1.yaml" "C:\pbp_test_dataset\globalAttributes_file_2.yaml"
```

## Versions

- v0.0.1 (2025-04-03): Initial version