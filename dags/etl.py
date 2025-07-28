from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum
import logging

# Define the DAG
with DAG(
    dag_id='nasa_apod_postgres',
    start_date=pendulum.today('UTC').add(days=-1),
    schedule='@daily',
    catchup=False,
    default_args={
        'owner': 'airflow',
        'retries': 1,
    }
) as dag:

    # Step 1: Create a table if it doesn't exist
    @task
    def create_table():
        postgres_hook = PostgresHook(postgres_conn_id='my_postgres_connection')
        create_table_query = """
        CREATE TABLE IF NOT EXISTS public.apod_data (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            date DATE,
            media_type VARCHAR(50)
        );
        """
        try:
            postgres_hook.run(create_table_query)
            logging.info("Table 'public.apod_data' created successfully or already exists.")
            # Verify table creation
            tables = postgres_hook.get_records("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'apod_data');")
            logging.info(f"Table exists check: {tables}")
        except Exception as e:
            logging.error(f"Failed to create table: {str(e)}")
            raise

    # Step 2: Extract data from the NASA APOD API
    extract_apod = HttpOperator(
        task_id='extract_apod',
        http_conn_id='nasa_api',
        endpoint='planetary/apod',
        method='GET',
        data={"api_key": "{{ conn.nasa_api.extra_dejson.api_key }}"},
        response_filter=lambda response: response.json(),
    )

    # Step 3: Transform the data
    @task
    def transform_apod_data(response):
        if not response:
            logging.error("No data received from NASA API")
            raise ValueError("Empty response from NASA API")
        apod_data = {
            'title': response.get('title', ''),
            'explanation': response.get('explanation', ''),
            'date': response.get('date', ''),
            'media_type': response.get('media_type', ''),
        }
        logging.info(f"Transformed data: {apod_data}")
        return apod_data

    # Step 4: Load the data into Postgres
    @task
    def load_data_to_postgres(apod_data):
        postgres_hook = PostgresHook(postgres_conn_id='my_postgres_connection')
        insert_query = """
        INSERT INTO public.apod_data (title, explanation, date, media_type)
        VALUES (%s, %s, %s, %s);
        """
        try:
            postgres_hook.run(insert_query, parameters=(
                apod_data['title'],
                apod_data['explanation'],
                apod_data['date'],
                apod_data['media_type']
            ))
            logging.info("Data inserted successfully.")
        except Exception as e:
            logging.error(f"Failed to insert data: {str(e)}")
            raise

    # Step 5: Define task dependencies
    create_table() >> extract_apod
    api_response = extract_apod.output
    transform_data = transform_apod_data(api_response)
    load_data_to_postgres(transform_data)