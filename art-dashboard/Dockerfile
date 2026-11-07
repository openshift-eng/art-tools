FROM registry.access.redhat.com/ubi9/python-311:latest

USER 0
WORKDIR /app

RUN pip install --no-cache-dir uv

COPY requirements.txt .
RUN uv pip install --system --no-cache -r requirements.txt

COPY main.py .
COPY templates/ templates/

USER 1001

EXPOSE 8080
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
