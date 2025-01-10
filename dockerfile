# FROM balenalib/amd64-python:3.9
FROM python:3.9-slim
WORKDIR /DSP


# Copy the current directory contents into the container at /app
COPY . /DSP

# Install necessary dependencies
# RUN mkdir -p /opt/google

# RUN apt-get update && apt-get install -y \
#     wget \
#     curl \
#     ca-certificates \
#     gnupg

# USER root
# RUN apt-get update && apt-get install -y \
#     curl \
#     ca-certificates \
#     gnupg && \
#     rm -rf /var/lib/apt/lists/*

# Download and install Google Chrome
# RUN apt-get update && apt-get install -y unzip

# RUN wget https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/117.0.5938.92/linux64/chrome-linux64.zip

# RUN unzip -o chrome-linux64.zip -d /opt/google/chrome

RUN pip install --upgrade pip
RUN pip install -r requirements.txt
# RUN conda env create -n deep -f environment.yml


# Run app.py when the container launches
ENTRYPOINT ["python"]
CMD ["code/app/streamlit_app.py"]

EXPOSE 5000