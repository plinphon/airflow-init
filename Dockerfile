FROM apache/airflow:2.11.0

# Switch to root to install system dependencies if needed
USER root

# Install any system dependencies (optional)
# RUN apt-get update && apt-get install -y gcc python3-dev && apt-get clean

# Switch back to airflow user for pip installations
USER airflow

# Install Python packages
RUN pip install faker pandas

# Set back to airflow user (already set, but explicit)
USER airflow