from airflow import DAG
from datetime import datetime
from airflow.operators.dummy import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.task_group import TaskGroup

TABLE_SOURCES = {
    # __table_name : __file_name
    "ref_customers": "customers.csv",
    "ref_products": "products.csv",
    "trx_transactions": "transactions.csv"
}

#inisiasi dag
with DAG(
    "main_daily",
    start_date = datetime(2022, 11, 27),
    schedule_interval = "0 21 * * *"
    ) as dag: 
    
    
    job_start = DummyOperator(
            task_id = "job_start"
    )
    
    with TaskGroup("load_data_to_postgres") as load_data_to_postgres:
        for tablename, filename in TABLE_SOURCES.items():
            
            operator = PostgresOperator(
                task_id = f"load_data_to_postgres-{tablename}",
                postgres_conn_id = "postgres_conn",
                sql = "{% include './sql/load_data_to_postgres.sql'%}",
                params = {
                    "tablename": tablename,
                    "filename": filename
                }
            )
            
    upsert_ff_sales = PostgresOperator(
        task_id = "upsert_ff_sales",
        postgres_conn_id = "postgres_conn",
        sql = "{% include './sql/upsert_ff_sales.sql' %}"
    )


    upsert_sd_sales_dtls = PostgresOperator(
        task_id = "upsert_sd_sales_dtls",
        postgres_conn_id = "postgres_conn",
        sql = "{% include './sql/upsert_sd_sales_dtls.sql' %}"
    )
   
    
    job_finish = DummyOperator(
        task_id = "job_finish"
    )
    
    #Orchestration
    (
        job_start
        >> load_data_to_postgres
        >> upsert_ff_sales
        >> upsert_sd_sales_dtls
        >> job_finish
    )
