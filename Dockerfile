FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements_cronjob.txt .
RUN pip install --no-cache-dir -r requirements_cronjob.txt

# Copy cronjob server code
COPY cronjob_server.py .

# Run the cronjob server
CMD ["python", "cronjob_server.py"]
