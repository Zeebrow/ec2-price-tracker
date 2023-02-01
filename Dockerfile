FROM --platform=linux/amd64 python:3.11-buster

RUN apt update && apt -y install firefox-esr

RUN wget -O /tmp/geckodriver.tar.gz https://github.com/mozilla/geckodriver/releases/download/v0.32.0/geckodriver-v0.32.0-linux64.tar.gz

RUN  tar -xvf /tmp/geckodriver.tar.gz -C /usr/local/bin && rm /tmp/geckodriver.tar.gz

COPY scrpr.py .
COPY requirements.txt .

RUN pip install -r requirements.txt

RUN echo 'docker run --rm -d -v $HOME/.local/share/scrpr/logs:/logs -v $HOME/.local/share/scrpr/csv-data:/csv-data local/scrpr:latest -t 8 --compress'

ENV DISPLAY=:99
ENV TZ=America/New_York

ENTRYPOINT ["python3", "scrpr.py", "--data-dir", "csv-data/ec2", "--log-file", "logs/scrpr.log", "--metric-data-file", "logs/metric-data.txt"]
