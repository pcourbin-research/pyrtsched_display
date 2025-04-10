# rtsched

## Docker

```
docker compose build
docker compose up -d
```

Go to http://localhost:5000


## Local
### Linux
```
python -m venv ./venv/
source ./venv/bin/activate
pip install -r requirements.txt
python .\app.py
```

Go to http://localhost:8050

### Windows
``` 
python -m venv .\venv\
# If necessary : Set-ExecutionPolicy Unrestricted -Scope Process
.\venv\Scripts\activate

pip install -r requirements.txt
python .\app.py
```

Go to http://localhost:8050