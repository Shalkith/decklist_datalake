
# Magic: The Gathering Data Lake Project

## Overview
This repository contains a comprehensive setup for a Magic: The Gathering (MTG) data lake project, utilizing dbt (Data Build Tool) for data modeling and Python for data processing and job scheduling. The project is structured to handle datasets related to MTG, with a focus on deck types and card interactions.

## Repository Structure

### `datasets`
Contains the raw data files for the project.
- `raw/moxfield`: Example YAML files (`commander_decks_table.yaml`, `pauperEdh_decks_table.yaml`) representing different deck types in MTG.

### `dbt`
Houses the dbt project files.
- `logs`: Contains log files for dbt processes.
- `mtg_datalake`: The core dbt project directory, including configurations (`dbt_project.yml`), README, and subdirectories for dbt models, macros, analyses, seeds, and snapshots.

### `python`
Includes Python scripts for data processing and job management.
- `job`: Scripts for job execution (`job.py`).
- `lib/utils`: Utility modules, including `moxfield_util` for specific operations and `parquet_reader` for reading Parquet files.

## Setup

### Prerequisites
- Python 3.x
- dbt CLI

### Installation
1. Clone the repository.
2. Install required Python dependencies.
3. Set up dbt by navigating to the `dbt` directory and configuring your environment.

## Usage

### dbt Commands
Navigate to the `dbt` directory and run dbt commands as needed. For example:
- `dbt run` to execute models.
- `dbt test` to run tests on your models.

### Python Jobs
Navigate to the `python/job` directory.
- Run `python job.py path/to/yamlfile.yaml` to execute the main job.


## Contributing
Contributions to the project are welcome! Please follow standard practices for contributing to GitHub repositories, including creating pull requests for proposed changes.


