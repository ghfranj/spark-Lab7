# Use the official Bitnami Spark image as the base image
FROM bitnami/spark:latest

# Copy your Python script and dependencies into the container
#COPY main.py /app/main.py
#COPY src/ /app/src/
#COPY config.ini /app/config.ini
COPY . /app/.

# Set the working directory
WORKDIR /app
#ENV CLASSPATH="/app/ojdbc8.jar"
# Install any additional dependencies if required
# For example, if you need to install Python packages, you can use pip
#RUN python -m pip install oracledb
RUN pip install -r requirements.txt
# Define the command to run your Python script
CMD ["spark-submit", "--jars", "ojdbc8.jar", "main.py"]
