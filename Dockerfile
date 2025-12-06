FROM python:3.12
PYTHON_ENVIRONMENT="production"
CT_SERVER_URL="https://ct-service.fly.dev/"
RA_SERVER_URL="https://ra-service.fly.dev/"
CRM_SERVER_URL="https://crm-service.fly.dev/"
BC_SERVER_URL="https://d4k-bc-service.fly.dev/"
SDTM_SERVER_URL="https://sdtm-service.fly.dev/"
EXPOSE 8000
WORKDIR /code
COPY ./requirements.txt /code/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt
COPY ./app /code/app
CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
