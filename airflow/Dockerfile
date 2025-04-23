FROM apache/airflow:3.0.0

#https://robust-dinosaur-2ef.notion.site/How-to-run-PySpark-with-Apache-Airflow-PUBLIC-1449e45d4dbe8077828be971b0078495
# Switch to root to install system dependencies
#USER root
#
## Install OpenJDK-17
#RUN apt update && \
#    apt-get install -y openjdk-17-jdk && \
#    apt-get install -y ant && \
#    apt-get clean;
#
## Set JAVA_HOME
#ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/
#RUN export JAVA_HOME

# Switch back to airflow user
USER airflow

# Copy and install Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
