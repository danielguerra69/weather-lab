# Use an official slim runtime as a parent image
FROM python:3.9-slim
LABEL maintainer="Daniel Guerra"

# Install build dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libhdf5-dev \
    libnetcdf-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory in the container
WORKDIR /app

# Copy requirements.txt and install dependencies
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Set the PYTHONPATH environment variable
ENV PYTHONPATH=/app/src

# Install test dependencies
RUN pip install --no-cache-dir pytest

# Copy the current directory contents into the container at /app
COPY . .

# Run the tests
# RUN pytest tests/

# Ensure the setup script and healthcheck script are executable
RUN chmod +x bin/*

# Set the entrypoint to bin/entrypoint
ENTRYPOINT ["bin/entrypoint.sh"]

# Run main.py when the container launches
CMD ["python", "src/main.py"]
