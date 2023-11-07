# Multinational Retail Data Centralisation

An ETL pipeline designed to streamline and centralize data processing operations across various retail locations of a multinational company, enabling efficient and robust data analysis for strategic decision-making.

## Table of Contents
- [Skills and Technologies](#skills-and-technologies)
- [Introduction](#introduction)
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Known Issues and Future Improvements](#known-issues-and-future-improvements)
- [License](#license)
- [Contact](#contact)
- [Note](#note)

## Skills and Technologies

This project demanded a wide array of skills and a strong command over various technologies, including but not limited to:

- **Python Programming**: Comprehensive use of Python for data manipulation and pipeline creation.
- **Data Extraction & Transformation**: Proficiency in extracting data from diverse sources and transforming it for analytical readiness.
- **Data Cleaning**: In-depth understanding and application of data cleaning techniques to ensure the integrity of data.
- **PostgreSQL**: Extensive use of PostgreSQL for data storage and complex SQL queries for data retrieval.
- **Pandas & NumPy**: Employed these powerful Python libraries for data analysis and manipulation tasks.
- **Jupyter Notebooks**: Made use of Jupyter Notebooks for iterative coding, debugging and initial data exploration.
- **Version Control**: Managed code and changes using Git, demonstrating best practices in continuous integration and deployment.
- **Database Design**: Skills in conceptualizing and implementing database schemas and relationships.
- **Object-Oriented Programming (OOP)**: Applied OOP concepts for creating maintainable and reusable code.
- **Virtual Environments**: Used virtual environments for managing dependencies and ensuring consistent project setups.

Through this project, I have demonstrated my ability to learn and adapt to various technologies quickly, showing my commitment to personal and professional growth.

## Introduction

This repository contains the implementation of an ETL pipeline for consolidating sales data across multiple geographic locations of a multinational retail corporation. The pipeline is responsible for extracting data from various sources, cleaning and transforming the data, and loading it into a centralized PostgreSQL database to support business intelligence and analytics.

## Installation

Instructions on setting up the environment and installing the necessary dependencies for running the ETL pipeline.

```bash
# Clone the repository
git clone https://github.com/ASEIcode/multinational-retail-data-centralisation.git

# Navigate to the repository directory

# Set up a Python virtual environment (optional but recommended)
python -m venv venv # 2nd venv = location of your virtual envs
source venv/bin/activate  # On Windows use `venv\Scripts\activate`

# Install the required packages
pip install -r requirements.txt
```
## Usage

main.ipynb is a notebook containing all of the commands to run the ETL pipeline. There are also markdown comments at each critical stage to help you keep track of what each script does.

## Configuration

Before running the pipeline in main.ipynb Youll need two configuration yaml files:

1. sales_data_db_creds.yaml

    This will contain the crednetials to access your postgresql database.

        HOST: hostname
        PASSWORD: password
        USER: username
        DATABASE: sales_data
        PORT: port number

2. db_creds.yaml

    This will contain the credentials to access your amazon RDS database for the data extractor methods that utilise AWS RDS.

        RDS_HOST: host address
        RDS_PASSWORD: xxxxx
        RDS_USER: username
        RDS_DATABASE: database name
        RDS_PORT: port number

## Known issues and Future improvements

- Phone numbers are in many different formats in the tables. Regex could clean and standardise this
- Try / Except blocks to catch errors more elegently in the extraction and upload classes
- has_numbers and has_alpha are used more than once and could be written into the class as a method to avoid duplication
- The retrieve_stores_data method in the DataExtractor class currently has to make many 451 consecutive requests and append them all to a list. This takes a long time. Parallel processing could be employed here to make this more efficient.
- Currently each cleaning method has been written to fit specifically to the dummy data. If this were to be used to clean future data coming from similar sources in an automated way rather than being supervised manually by an engineer the cleaning would need to involve extra lines to cater to a larger variation of errors that could be present. Automatic duplicate and null checking for instance.
- Create unit tests for each cleaning function to check for bugs as they are improved