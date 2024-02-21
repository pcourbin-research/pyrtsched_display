FROM python:3.9-slim-buster
RUN apt-get update
RUN apt-get install nano
 
RUN mkdir wd
WORKDIR wd

 # install dependencies
RUN pip install --upgrade pip
COPY requirements.txt .
RUN pip3 install -r requirements.txt
  
COPY pyrtsched_display pyrtsched_display
COPY app.py app.py

EXPOSE 8050
  
CMD [ "python", "app.py"]
# CMD [ "gunicorn", "--workers=5", "--threads=1", "-b 0.0.0.0:8000", "app:app"]