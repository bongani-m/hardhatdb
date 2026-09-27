FROM python:3.13-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py store.py ./

EXPOSE 8080
ENV HTTP_ADDR=:8080
CMD ["python", "main.py"]
