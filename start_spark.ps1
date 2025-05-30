# start_spark.ps1
$env:SPARK_HOME = "D:\spark\spark-3.5.6-bin-hadoop3"
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-11.0.27.6-hotspot"
$env:PYSPARK_PYTHON = "D:\Rajendra-Tech\PySparkProjects\.venv\Scripts\python.exe"
$env:PATH = "$env:SPARK_HOME\bin;$env:JAVA_HOME\bin;" + $env:PATH

.venv\Scripts\python.exe spark_test.py
