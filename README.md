# Air_Quality_Data_pipeline

•	Built an end to end ETL pipeline that fetches real time weather data from the Open Meteo API and loads it into a PostgreSQL based data warehouse (raw_sensor_staging).
•	Designed a feature ready analytics table (fact_sensor_features) and implemented AI enabled data quality checks such as outlier temperature detection and high precipitation flagging.
•	Logged quality metrics and anomaly counts into a (data_quality_log) table for monitoring pipeline health and AI ready dataset reliability.
•	Orchestrated the pipeline locally in VS Code using Python scripts, with each run being versioned and logged for reproducibility.
