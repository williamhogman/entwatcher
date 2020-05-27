FROM tiangolo/uvicorn-gunicorn-fastapi:python3.8

COPY Pipfile Pipfile.lock /app/
WORKDIR /app
RUN pip install pipenv
RUN pipenv install --system --deploy
COPY entwatcher /app/entwatcher
CMD ["uvicorn", "--host", "0.0.0.0", "entwatcher.main:app"]
