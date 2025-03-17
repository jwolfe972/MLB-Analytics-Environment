# Use the official Python image
FROM python:3.11

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file
COPY requirements.txt .




# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application files
COPY . .

ENV MLB_TEAM_LOGOS=/home/mlb_images/team_logos

# Expose port 3000
EXPOSE 8051

# Run the Streamlit app
CMD ["streamlit", "run", "main.py", "--server.port=8051", "--server.address=0.0.0.0"]
