from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum
import logging

# Define the DAG
with DAG(
    dag_id='jupiter_price_postgres',  # Change to 'crypto_etl' if preferred
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
        CREATE TABLE IF NOT EXISTS public.jupiter_prices (
            id SERIAL PRIMARY KEY,
            token_id VARCHAR(50),
            symbol VARCHAR(10),
            usd_price FLOAT,
            block_id BIGINT,
            decimals INTEGER,
            price_change_24h FLOAT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        try:
            postgres_hook.run(create_table_query)
            logging.info("Table 'public.jupiter_prices' created successfully or already exists.")
            tables = postgres_hook.get_records(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'jupiter_prices');"
            )
            logging.info(f"Table exists check: {tables}")
        except Exception as e:
            logging.error(f"Failed to create table: {str(e)}")
            raise

    # Step 2: Extract data from the Jupiter Price API V3
    extract_price = HttpOperator(
        task_id='extract_price',
        http_conn_id='jupiter_api',
        endpoint='price/v3',
        method='GET',
        data={
            "ids": (
                "So11111111111111111111111111111111111111112,"  # SOL
                "9n4nbM75f5Ui33ZbPYXn59EwSgE8CGsHtAeTH5YFeJ9E,"  # BTC
                "2FPyTwcZLUg1MDrwsyoP4D6s1tM7hAkHYRjkNb5w6Pxk,"  # ETH
                "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v,"  # USDC
                "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"   # USDT
            )
        },
        response_filter=lambda response: response.json(),
    )

    # Step 3: Transform the data
    @task
    def transform_price_data(response):
        logging.info(f"Raw API response: {response}")
        if not response or not isinstance(response, dict):
            logging.error("No data or invalid response from Jupiter API")
            raise ValueError("Empty or invalid response from Jupiter API")
        price_data_list = []
        token_symbol_map = {
            "So11111111111111111111111111111111111111112": "SOL",
            "9n4nbM75f5Ui33ZbPYXn59EwSgE8CGsHtAeTH5YFeJ9E": "BTC",
            "2FPyTwcZLUg1MDrwsyoP4D6s1tM7hAkHYRjkNb5w6Pxk": "ETH",
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": "USDC",
            "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB": "USDT"
        }
        expected_tokens = list(token_symbol_map.keys())
        for token_id in expected_tokens:
            data = response.get(token_id, {})
            usd_price = data.get('usdPrice', 0.0)
            if usd_price == 0.0:
                logging.warning(f"No valid usdPrice for token {token_id}: {data}")
            price_data = {
                'token_id': token_id,
                'symbol': token_symbol_map.get(token_id, token_id[:4]),
                'usd_price': usd_price,
                'block_id': data.get('blockId', 0),
                'decimals': data.get('decimals', 0),
                'price_change_24h': data.get('priceChange24h', 0.0),
                'timestamp': pendulum.now('UTC').isoformat()
            }
            price_data_list.append(price_data)
            logging.debug(f"Processed token {token_id}: {price_data}")
        logging.info(f"Transformed data: {price_data_list}")
        return price_data_list

    # Step 4: Load the data into NeonDB
    @task
    def load_data_to_postgres(price_data_list):
        postgres_hook = PostgresHook(postgres_conn_id='my_postgres_connection')
        insert_query = """
        INSERT INTO public.jupiter_prices (token_id, symbol, usd_price, block_id, decimals, price_change_24h, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s);
        """
        try:
            for price_data in price_data_list:
                logging.debug(f"Inserting record: {price_data}")
                postgres_hook.run(insert_query, parameters=(
                    price_data['token_id'],
                    price_data['symbol'],
                    price_data['usd_price'],
                    price_data['block_id'],
                    price_data['decimals'],
                    price_data['price_change_24h'],
                    price_data['timestamp']
                ))
            logging.info(f"Inserted {len(price_data_list)} records successfully.")
        except Exception as e:
            logging.error(f"Failed to insert data: {str(e)}")
            raise

    # Step 5: Optional verification task
    @task
    def verify_data():
        postgres_hook = PostgresHook(postgres_conn_id='my_postgres_connection')
        try:
            records = postgres_hook.get_records(
                "SELECT * FROM public.jupiter_prices ORDER BY timestamp DESC LIMIT 5;"
            )
            logging.info(f"Latest records in jupiter_prices: {records}")
        except Exception as e:
            logging.error(f"Failed to verify data: {str(e)}")
            raise

    # Step 6: Define task dependencies
    create_table() >> extract_price
    api_response = extract_price.output
    transform_data = transform_price_data(api_response)
    load_data_to_postgres(transform_data) >> verify_data()