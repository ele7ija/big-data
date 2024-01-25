FROM python:3.11
COPY . /code/
WORKDIR /code
RUN pip install -r requirements.txt

ENTRYPOINT ["faust", "-A", "predictor", "worker", "-l", "info"]