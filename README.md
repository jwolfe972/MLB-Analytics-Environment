# MLB Game Prediction Model Project CSCE 5214 ⚠️**Work-In-Progress**⚠️

## Author: Jordan D. Wolfe [jwolfe972@gmail.com](mailto:jwolfe972@gmail.com)

### Expanded Idea from: https://github.com/laplaces42/mlb_game_predictor
<br>

<p><a target="_blank" href="https://raw.githubusercontent.com/devicons/devicon/master/icons/python/python-original.svg" style="display: inline-block;"><img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/python/python-original.svg" alt="python" width="42" height="42" /></a>
<a target="_blank" href="https://raw.githubusercontent.com/devicons/devicon/master/icons/postgresql/postgresql-original-wordmark.svg" style="display: inline-block;"><img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/postgresql/postgresql-original-wordmark.svg" alt="postgresql" width="42" height="42" /></a>
<a target="_blank" href="https://raw.githubusercontent.com/devicons/devicon/master/icons/redis/redis-original-wordmark.svg" style="display: inline-block;"><img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/redis/redis-original-wordmark.svg" alt="redis" width="42" height="42" /></a>
<a target="_blank" href="https://www.vectorlogo.zone/logos/grafana/grafana-icon.svg" style="display: inline-block;"><img src="https://www.vectorlogo.zone/logos/grafana/grafana-icon.svg" alt="grafana" width="42" height="42" /></a>
<a target="_blank" href="https://raw.githubusercontent.com/devicons/devicon/master/icons/docker/docker-original-wordmark.svg" style="display: inline-block;"><img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/docker/docker-original-wordmark.svg" alt="docker" width="42" height="42" /></a>
<a target="_blank" href="https://www.vectorlogo.zone/logos/gnu_bash/gnu_bash-icon.svg" style="display: inline-block;"><img src="https://www.vectorlogo.zone/logos/gnu_bash/gnu_bash-icon.svg" alt="bash" width="42" height="42" /></a>
<a target="_blank" href="https://www.vectorlogo.zone/logos/getpostman/getpostman-icon.svg" style="display: inline-block;"><img src="https://www.vectorlogo.zone/logos/getpostman/getpostman-icon.svg" alt="postman" width="42" height="42" /></a>
<a target="_blank" href="https://raw.githubusercontent.com/devicons/devicon/master/icons/linux/linux-original.svg" style="display: inline-block;"><img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/linux/linux-original.svg" alt="linux" width="42" height="42" /></a>
<a target="_blank" href="https://upload.wikimedia.org/wikipedia/commons/0/05/Scikit_learn_logo_small.svg" style="display: inline-block;"><img src="https://upload.wikimedia.org/wikipedia/commons/0/05/Scikit_learn_logo_small.svg" alt="scikit_learn" width="42" height="42" /></a>
<a target="_blank" href="https://raw.githubusercontent.com/devicons/devicon/2ae2a900d2f041da66e950e4d48052658d850630/icons/pandas/pandas-original.svg" style="display: inline-block;"><img src="https://raw.githubusercontent.com/devicons/devicon/2ae2a900d2f041da66e950e4d48052658d850630/icons/pandas/pandas-original.svg" alt="pandas" width="42" height="42" /></a>
<a target="_blank" href="https://seaborn.pydata.org/_images/logo-mark-lightbg.svg" style="display: inline-block;"><img src="https://seaborn.pydata.org/_images/logo-mark-lightbg.svg" alt="seaborn" width="42" height="42" /></a>
<a target="_blank" href="https://www.vectorlogo.zone/logos/git-scm/git-scm-icon.svg" style="display: inline-block;"><img src="https://www.vectorlogo.zone/logos/git-scm/git-scm-icon.svg" alt="git" width="42" height="42" /></a>
<a target="_blank" href="https://github.com/jwolfe972/mlb_prediction_app/blob/main/imgs/mlflow_logo.png" style="display: inline-block;"><img src="imgs/mlflow_logo.png" alt="mlflow" width="42" height="42" /></a>
<a target="_blank" href="https://github.com/jwolfe972/mlb_prediction_app/blob/main/imgs/airflow.png" style="display: inline-block;"><img src="imgs/airflow.png" alt="airflow" width="70" height="42" /></a>

</p>

<br>

## Description

### This project expands upon the baseball prediction model presented above for my CSCE 5214 Software Development for AI class, through the creation of an UI for showing predictions for handling game outcome predictions. Also upon further inspection this setup can also double as a built from scratch environment for doing baseball research and analysis
<br> 

## Why Should You Use This Program?
    1. This program provides a Game Prediction Model Fully Automated (WIP 🚧)
    2. This program provides a fully automated Baseball Savant Pitch-By-Pitch ETL Process into a Postgres Datawarehouse for easy querying
    3. This program provides custom ML model creation and usage from the MLB Data
    4. This program provides an underlying open-source dashboard tool to build custom visualizations from the data
    5. Everything built in to this project can be ran locally for free and modified to be ran on Cloud Environments


<br>

## Software Used
    - Python Streamlit (for the UI)
    - Apache Airflow (Automated Data Pipeline for updating data)
    - PostgreSQL (Storing Historical Data)
    - MLFlow (For ML Tracking, Storage and Deployment)
    - ** Extra Grafana (For Dashboarding Savant Data)
    - ** Extra Jupyter Notebook Server (For creating notebooks for analysis)
    - Docker and Docker Compose (Stacking the application all together)



## How To Use This Application

### Requirements
    - Docker CLI or Docker Desktop (I use Docker Desktop)
    - Atleast 12-16 GB of RAM
    - Internet Access for downloading data
    - A prefered Database Connection Application: I chose PgAdmin: https://www.pgadmin.org/download/
    - A Slack account (for sending notifications on the status of DAG Runs)
  
### Windows Docker Setup Guide: [Windows](https://docs.docker.com/desktop/setup/install/windows-install/)

### Mac OS Docker Setup Guide: [Mac OS](https://docs.docker.com/desktop/setup/install/mac-install/)

### Mac OS zsh Command Line Fix [zsh](https://stackoverflow.com/questions/64009138/docker-command-not-found-when-running-on-mac)
<br>

## Guide on Getting Started
    Step 1: clone this repository

    Step 2: Make Sure the Docker Engine is running (Open Docker Desktop or confirm the engine is running)

    Step 3: Run the command: docker-compose up -d  or docker-compose up -d --build for a rebuild<- This command builds the compose stacks. It takes a while to initially start.
    
    Step 4: Once Apache Airflow is up, first complete the steps specified in the Setting_Up_Slack_Connection_Airflow.pdf

    Step 5: If you are interested in the Pitch By Pitch Data unpause the 'baseball-savant-etl' dag. This should start the DAG since it is a scheduled DAG. This should run and populate the Data Warehouse tables for the DIM and FACT tables in the sql_scripts/schema.sql file.

    Step 6: If you are interested in the Game Prediction Model run the 'load_mlb_game_prediction'
    DAG to populate the baseball_stats table and deploy an ML model for predicting wins (DAG still in progress 🚧)

### Commands
``` docker-compose up -d ```

### Applications Running

```localhost:8051 -> Streamlit app```
<br>
```localhost:8081 -> Apache Airflow Web UI (default login is user:airflow pass:airflow)```
<br>
```localhost:8080 -> SparkUI```
<br>
```localhost:5432 -> PostgreSQL Database (default login is user:user pass: password)```
<br>
```localhost:5000 -> ML Flow UI ```
<br>
```localhost:4000 -> Grafana (default login is user:admin pass: admin) ```
<br>
```localhost:8888 -> Jupyter Notebook Server ```


### Note: For Freshly restarting the Containers and Volumes run this command:
```docker compose down --volumes --remove-orphans ```

Def recommend running this after I push new changes as I continue 
until this project is in a stable state. Dont also forget to recreate the Slack Connection to Airflow upon freshly restarting the containers and volumes

Also for the baseball savant ETL Dag that is scheduled so to start the job just unpause the DAG



### Note: Sometimes the airflow-webserver.pid file can become stale and cause the airflow UI not to come up. To fix this just exec into the airflow-webserver container remove the .pid file and restart the container

### Note: For baseball savant ETL for loading past seasons data just modify the START_DATE and END_DATE variables at the top of the file. The intial start and end dates are for pulling 2024 season data, you can adjust for this season by changing the start season to 2025 and uncommeting the END_DATE variable with the datetime.now()

### Also due to RAM limitiations only do one full season per load or sometime less than that depending on the RAM for your machine. Or adjust the Memory Allocation for your Docker Container setup

## UI Demo
# ![Streamlit Dashboard](/imgs/UI.png)

## Airflow ETL
# ![ Airflow Dag](/imgs/statcast_etl_dag.png)

## MLFlow UI
# ![ MLFlow UI](/imgs/mlflow.png)

## Grafana Dashboard Example
# ![Grafana Dashboard](/imgs/grafana.png)

## Jupyter Notebook Server
# ![Jupyter Notebook](/imgs/jupyter-notebook.png)


# Project Phase Status and Progress
## **Phase 1:** Description of the Existing System *(Due 02/25/2025)* ✅

## **Phase 2:** Description of the Used Machine Learning and Design of User Interface *(Due 03/28/2025)* ✅

## **Phase 3:** Extending the Architecture of an existing ML-based system *(Due 04/20/2025)* 🚧

## **Phase 4:** Develop a User Interface *(Due 05/04/2025)* 🚧

