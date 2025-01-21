# Use an official Python image as the base
FROM python:3.9-slim

# Set the working directory
WORKDIR /app

# Copy the requirements file from the root directory
COPY requirements.txt /app/

# Install dependencies
RUN pip install -r requirements.txt

# Copy the rest of the application code
COPY . /app

# Expose the port for Streamlit
EXPOSE 8501

# Set Streamlit environment variables (optional)
ENV STREAMLIT_SERVER_PORT=8501 \
    STREAMLIT_SERVER_ADDRESS=0.0.0.0

# Run the Streamlit app
# CMD ["streamlit", "run", "code/app/streamlit_app.py"]
CMD ["streamlit", "run", "code/app/streamlit_app.py", "--server.headless=true", "--logger.level=debug"]
