# Use a lightweight Python Linux image
FROM python:3.11-slim

# 1. Install Java (Required for Spark)
# We update the package manager and install OpenJDK 17
RUN apt-get update && \
    apt-get install -y default-jdk && \
    apt-get clean;

# Set JAVA_HOME automatically
ENV JAVA_HOME=/usr/lib/jvm/default-java

# 2. Set the working directory inside the container
WORKDIR /app

# 3. Copy requirements and install them
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Copy your entire project code into the container
COPY . .

# 5. Create a directory for data (Volume mount point)
RUN mkdir -p /app/data

# 6. Default command (Can be overridden by Airflow)
CMD ["python", "--version"]