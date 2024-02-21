FROM python:3.9-slim-buster
 RUN apt-get update
 RUN apt-get install nano
 
 RUN mkdir wd
 WORKDIR wd
 COPY requirements.txt .
 RUN pip3 install -r requirements.txt
  
 COPY pyrtsched_display pyrtsched_display
 COPY app.py app.py
  
 CMD [ "python", "app.py"]