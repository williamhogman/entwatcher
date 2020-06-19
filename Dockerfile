FROM python:3.8
RUN pip3 install pipenv
COPY Pipfile Pipfile.lock /app/
WORKDIR /app
RUN pipenv install --three --deploy --ignore-pipfile
COPY entwatcher /app/entwatcher
CMD ["pipenv", "run", "python", "-u", "-m", "entwatcher"]
