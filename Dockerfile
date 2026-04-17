FROM python:3.10

WORKDIR /code

# Copy requirements and install them
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your files
COPY . .

# Force Streamlit to run your specific script
CMD ["streamlit", "run", "scripts/app.py", "--server.port=7860", "--server.address=0.0.0.0"]
