
FROM bitnami/spark:latest

USER root
# Set environment variables for Java
ENV JAVA_HOME=/opt/bitnami/java
ENV PATH="$JAVA_HOME/bin:$PATH"

# Set the working directory
WORKDIR /opt/bitnami/spark

# Expose Spark ports
EXPOSE 8080 7077
