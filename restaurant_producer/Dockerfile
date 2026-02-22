FROM python:3.12.12-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY data/ data/

COPY load_static_data.py .
COPY producer.py .

CMD ["sh", "-c", "python load_static_data.py && python producer.py"]
