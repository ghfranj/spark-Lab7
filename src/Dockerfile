# Use the official Bitnami Spark image as the base image
FROM bitnami/spark:latest

# Copy your Python script and dependencies into the container
COPY .. /app2/.

# Set the working directory
WORKDIR /app2

RUN pip install -r requirements.txt
# Define the command to run your Python script
CMD ["spark-submit", "--jars", "ojdbc8.jar", "main.py"]
