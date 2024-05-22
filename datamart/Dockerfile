# Use a Scala base image
FROM sbtscala/scala-sbt:eclipse-temurin-17.0.4_1.7.1_3.2.0
#FROM akkasamples/akka-sample-cluster-scala:latest
# Set the working directory
WORKDIR /app

# Copy the Scala code into the container
COPY . /app
#RUN apt-get update && apt-get install -y curl
# Compile the Scala code
#RUN scalac main.scala

# Define the command to run your Scala application
#CMD ["sbt", "reload"]
CMD ["sbt", "compile"]
CMD ["sbt", "runMain Main"]
#CMD ["scala", "main"]
