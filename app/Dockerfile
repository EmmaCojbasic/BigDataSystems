FROM bde2020/spark-python-template:3.1.2-hadoop3.2

RUN cd /app pip install -r requirements.txt
ENV SPARK_APPLICATION_PYTHON_LOCATION /app/main2.py
ENV SPARK_APPLICATION_ARGS "0.00 10.00 100 450.00 440.00 | pollution"