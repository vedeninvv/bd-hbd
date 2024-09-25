@echo off
cd /d C:\Users\vedev\Desktop\BD\airflow_project\python-server
echo Запуск FastAPI приложения...
uvicorn main:app --reload --host 0.0.0.0 --port 8001
pause