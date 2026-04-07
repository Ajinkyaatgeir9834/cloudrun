# 1. Use official Python image
FROM python:3.10-slim

# 2. Set working directory
WORKDIR /app

# 3. Copy requirements file first (for caching)
COPY requirements.txt .

# 4. Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copy application code
COPY . .

# 6. Expose port (important for Cloud Run)
EXPOSE 8080

# 7. Run the application
CMD ["python", "main.py"]