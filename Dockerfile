FROM python:3.11-slim

WORKDIR /app

COPY preprocesar.py .
COPY requirements-preprocesar.txt .

RUN pip install --upgrade pip && \
    pip install -r requirements-preprocesar.txt

CMD ["python", "preprocesar.py"]
