# Use OpenJDK 17 as base image
FROM openjdk:17-jdk-slim

# Set working directory
WORKDIR /app

# Copy Maven wrapper and pom.xml
COPY mvnw .
COPY .mvn .mvn
COPY pom.xml .

# Make mvnw executable
RUN chmod +x mvnw

# Download dependencies
RUN ./mvnw dependency:go-offline -B

# Copy source code
COPY src ./src

# Build the application
RUN ./mvnw clean package -DskipTests

# Create data directory
RUN mkdir -p /app/data

# Expose ports
EXPOSE 8080 8081 9090

# Set environment variables
ENV JAVA_OPTS="-Xmx2g -Xms1g"
ENV SPRING_PROFILES_ACTIVE=docker

# Run the application
CMD ["java", "-jar", "target/kvstore-0.0.1-SNAPSHOT.jar"] 