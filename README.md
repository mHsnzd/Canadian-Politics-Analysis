
## Overview

In this project, we developed an automated ETL pipeline to crawl Reddit comments and apply multiple NLP models from Hugging Face Transformers, gaining insights into Canadian public sentiment towards the Liberals and Conservatives. Big data tools such as Apache Spark and cloud technologies like AWS S3 and EC2 were used to manage data extraction and aggregation computations on a large dataset. Additionally, technologies like Airflow, PostgreSQL, Docker, Amazon Lambda, and Amazon Athena were leveraged to automate and streamline the pipeline.

## Documentation 

[Report](https://github.sfu.ca/zta23/CMPT732Project/blob/9078fd23bc3c5b6b8530fa5f7ac9bd5dee0eca4f/CMPT_732___Report.pdf)

[Video Presentation](https://drive.google.com/file/d/12Frf1zKLjWzxIBXkFeni1EzBZjPkalFC/view)

[Dashboard](https://github.sfu.ca/zta23/CMPT732Project/blob/9d967937164c6450a035288f67d2a7a150c7a398/Final%20Project%20Dashboard-2.pbix)

## Crawling Reddit Data Setup
1. Clone the repository.
   ```bash
    git clone https://github.sfu.ca/zta23/CMPT732Project.git
   ```
2. Create a virtual environment.
   ```bash
    python3 -m venv venv
   ```
3. Activate the virtual environment.
   ```bash
    source venv/bin/activate
   ```
4. Install the dependencies.
   ```bash
    pip install -r requirements.txt
   ```
5. Rename the configuration file and the credentials to the file.
   ```bash
    mv config/config.conf.example config/config.conf
   ```
6. Starting the containers
   ```bash
    docker-compose up -d
   ```
7. Launch the Airflow web UI.
   ```bash
    open http://localhost:8080
   ```

