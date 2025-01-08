# Use an official slim runtime as a parent image
FROM python:3.9-slim
LABEL maintainer="Daniel Guerra"

# Install build dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libhdf5-dev \
    libnetcdf-dev \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory in the container
WORKDIR /app

# Copy requirements.txt and install dependencies
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the current directory contents into the container at /app
COPY . .

# Ensure the setup script and healthcheck script are executable
RUN chmod +x bin/*

# Set the entrypoint to bin/entrypoint
ENTRYPOINT ["bin/entrypoint"]

# Run main.py when the container launches
CMD ["python", "src/main.py"]
