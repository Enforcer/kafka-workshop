FROM python:3.12-bullseye
RUN mkdir /app
WORKDIR /app
ADD requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt
ADD ./verification/hello_world.py /debug/hello_world.py
