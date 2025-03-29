# MLB Game Prediction Model Project CSCE 5214 ⚠️**Work-In-Progress**⚠️

## Author: Jordan D. Wolfe [jwolfe972@gmail.com](mailto:jwolfe972@gmail.com)

### Expanded Idea from: https://github.com/laplaces42/mlb_game_predictor
<br>

## Description

### This project expands upon the baseball prediction model presented above for my CSCE 5214 Software Development for AI class, through the creation of an UI for showing predictions for handling game outcome predictions

## Software Used
    - Python Streamlit (for the UI)
    - Apache Airflow (Automated Data Pipeline for updating data)
    - PostgreSQL (Storing Historical Data)
    - MLFlow (For ML Tracking, Storage and Deployment)
    - ** Extra Spark (For Big Data Processing) **
    - ** Extra Grafana (For Dashboarding Savant Data)
    - Docker and Docker Compose (Stacking the application all together)
    

## How To Use This Application

### Requirements
    - Docker CLI or Docker Desktop (I use Docker Desktop)
    - Atleast 8-12 GB of RAM
    - Internet Access for downloading data
    - A prefered Database Connection Application: I chose PgAdmin: https://www.pgadmin.org/download/
    - A Slack account (for sending notifications on the status of DAG Runs)

### Guide on Getting Started
    Step 1: clone this repository

    Step 2: Make Sure the Docker Engine is running

    Step 3: Run the command: docker-compose up -d  or docker-compose up -d --build for a rebuild<- This command builds the compose stacks. It takes a while to initially start.
    
    Step 4: Once Apache Airflow is up, first complete the steps specified in the Setting_Up_Slack_Connection_Airflow.pdf, then search for the ``load_mlb_game_prediction`` DAG and run the DAG to both create and populate the ML model database ** Also since this is web scraping sometimes it will error out, for that just clear the final state to re run from the error state so you won't lose progress in scraping

    *Extra* Step 5: Login to airflow and run the **baseball-savant-etl-workflow** DAG to populate the Datawarehouse for query the baseball savant pitch-by-pitch data. I also created the ETL Process that will store that in a data warehouse as well. This can be used for creating whatever kind of visualizations you could want from this data and could possibly be used for generating a Hidden Markov model (see hmm.py for example) or for anything simulation based.

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


### Note: For Freshly restarting the Containers and Volumes run this command:
```docker compose down --volumes --remove-orphans ```

Def recommend running this after I push new changes as I continue 
until this project is in a stable state. Dont also forget to recreate the Slack Connection to Airflow upon freshly restarting the containers and volumes

Also for the baseball savant ETL Dag that is scheduled so to start the job just unpause the DAG



### Note: Sometimes the airflow-webserver.pid file can become stale and cause the airflow UI not to come up. To fix this just exec into the airflow-webserver container remove the .pid file and restart the container
### Note: For baseball savant ETL for loading past seasons data just modify the START_DATE and END_DATE variables at the top of the file
### Also due to RAM limitiations only do one full season per load

## UI Demo
# ![Streamlit Dashboard](/imgs/UI.png)

## Airflow ETL
# ![ Airflow Dag](/imgs/statcast_etl_dag.png)

## MLFlow UI
# ![ MLFlow UI](/imgs/mlflow.png)

## Grafana Dashboard Example
# ![Grafana Dashboard](/imgs/grafana.png)


# Project Phase Status and Progress
## **Phase 1:** Description of the Existing System *(Due 02/25/2025)* ✅

## **Phase 2:** Description of the Used Machine Learning and Design of User Interface *(Due 03/28/2025)* ✅

## **Phase 3:** Extending the Architecture of an existing ML-based system *(Due 04/20/2025)* 🚧

## **Phase 4:** Develop a User Interface *(Due 05/04/2025)* 🚧

