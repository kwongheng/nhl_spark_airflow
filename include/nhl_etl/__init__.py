"""NHL medallion ETL (landing -> bronze -> silver -> gold) built on PySpark + Delta Lake.

Everything under this package is plain Python with no Airflow imports, so it can be
run or debugged outside Airflow. The DAG in ``dags/nhl_etl_dag.py`` only orchestrates.
"""
