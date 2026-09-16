FROM python:3.12-slim

WORKDIR /app
COPY . /app
RUN pip install --no-cache-dir '.[ml,serving]'

ENV FRAUDTWIN_MODEL_ARTIFACT=/models/logistic_regression.joblib
EXPOSE 8000
CMD ["uvicorn", "examples.model_service.app:app", "--host", "0.0.0.0", "--port", "8000"]
