# Use the official Python image
FROM python:3.11
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


# Expose port 5000
EXPOSE 5000



# Run the Streamlit app
CMD ["mlflow", "server", \
     "--backend-store-uri", "sqlite:///mlflow.db", \
     "--default-artifact-root", "s3://mlflow-mlb-prediction/", \
     "--host", "0.0.0.0", \
     "--port", "5000"]


