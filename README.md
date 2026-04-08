# pbp-batch

Wrapper for the mbari-pbp allowing to create metadata files, HMD computation, and daily plots for each deployment. Supports multiprocessing.

## Step-by-step instructions

### 1. Install Anaconda

The easiest way to install Python on your machine is to use Anaconda. It is an easy-to-install package manager, environment manager, and Python distribution. To install Anaconda follow the instructions on their [website](https://www.anaconda.com/).

### 2. Create a new virtual environment

- Open Anaconda Navigator ([instructions](https://docs.anaconda.com/navigator/getting-started/))
- Create a new virtual environment with **Python 3.11** ([instructions](https://docs.anaconda.com/navigator/tutorials/manage-environments/))

### 3. Install pbp-batch

Open a terminal window in the virtual environment ([instructions](https://docs.anaconda.com/navigator/getting-started/#navigator-starting-a-terminal)) and type:

```bash
pip install git+https://github.com/xaviermouy/pbp-batch.git
```

This will download and install pbp-batch along with all its dependencies. It may take a few minutes to complete.

To install a specific version (tag):

```bash
pip install git+https://github.com/xaviermouy/pbp-batch.git@v0.0.1
```

### 4. Create configuration files

Two YAML configuration files are required per deployment:

- **globalAttributes YAML** — deployment-specific information (location, instrument, dates, paths, etc.)
- **variableAttributes YAML** — generic metadata for units and analysis protocol; can be reused across multiple deployments

Example files from the Galapagos processing are provided in the `yaml_templates/` folder of this repository.

### 5. Run pbp-batch

Open a terminal window in Anaconda from your Python 3.11 environment (see Step 2) and type:

```bash
pbp-batch submit_job "C:\pbp_test_dataset\globalAttributes_file_1.yaml"
```

To process multiple deployments in parallel, provide multiple YAML files separated by a space:

```bash
pbp-batch submit_job "C:\pbp_test_dataset\globalAttributes_file_1.yaml" "C:\pbp_test_dataset\globalAttributes_file_2.yaml"
```

## Versions

| pbp-batch | mbari-pbp | Python | Notes |
|-----------|-----------|--------|-------|
| v0.0.2    | 1.8.74    | 3.11   | Updated to mbari-pbp 1.8.74 (new module structure) |
| v0.0.1    | 1.6.3     | 3.11   | Initial version |