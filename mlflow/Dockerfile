# Use the official Python image
FROM python:3.11
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


# Expose port 3000
EXPOSE 5000

ENV MLFLOW_TRACKING_URI=http://localhost:5000


# Run the Streamlit app
CMD ["mlflow", "server", "--host", "0.0.0.0", "--port", "5000"]


