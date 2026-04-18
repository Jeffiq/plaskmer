FROM python:3.10

WORKDIR /app

# Install system dependencies if needed
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Upgrade pip and install requirements
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
# Brute force install regex just in case
RUN pip install regex

# Copy the rest of the app
COPY . .

# Set environment variables to handle the port correctly
ENV STREAMLIT_SERVER_PORT=7860
ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0

CMD ["streamlit", "run", "scripts/app.py"]
