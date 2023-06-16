# Pull official base image
FROM python:3.10-slim-buster

# Set working directory
WORKDIR /usr/src/app

# Update apt-get before installing packages
RUN apt-get update

# Upgrade pip and install required packages
COPY ./requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

