# Airflow ETL pipeline with Postgres and API Integration

## Data Pipeline
![ETL](https://raw.githubusercontent.com/Excergic/images/main/etl.png)   

## Project Flow (Getting Price of BTC, SOL, ETH, USDC, USDT from Jupiter Price API V3)

```bash
git clone https://github.com/Excergic/ETL-airflow.git
cd ETL-airflow
pip install -r requirements.txt
```

## Install Astro CLI (If you don't have it installed)
- MacOS


```bash
# Prerequisites: You should have Docker Desktop installed on your machine.
brew install astro
```

- Windows with winget
```bash
winget install -e --id Astronomer.Astro
```

- Linux

```bash
curl -sSL install.astronomer.io | sudo bash -s
```


## Get Jupiter Price API V3
- You do not need API key to use Jupiter Price API V3. You can use the free plan. You can get the API key from the following link:
```bash
https://lite-api.jup.ag/price/v3
```

- ID's for tokens are as follows: (In Jupiter BTC and ETH are wrapped coin NOT a main coin so price will be different)
```bash
SOL: So11111111111111111111111111111111111111112
BTC: 9n4nbM75f5Ui33ZbPYXn59EwSgE8CGsHtAeTH5YFeJ9E
ETH: 2FPyTwcZLUg1MDrwsyoP4D6s1tM7hAkHYRjkNb5w6Pxk
USDC: EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v
USDT: Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB
```

## Create a Postgres Database

- This is project uses Postgres as a database via NeonDB.
- Go to NeonDB website and create a free account.
```bash
https://neon.com/
```

- Create a new database and copy the connection string and save it.

## Run the project

```bash
astro dev start
```

- This will start the project and you can access the Airflow UI at http://localhost:8080/

## Airflow UI
 - Go to Admin -> Connection and create a new connection with the following details:

   - Connection Type: HTTP
   - Connection Name: jupiter_api (this must be extact name as it is used in the DAG crypto_etl.py)
   - Save

   - Create new connection for postgreSQL
   - Connection Type: Postgres
   - Connection Name: my_postgres_connection (this must be extact name as it is used in the DAG crypto_etl.py)
   - Host: [host] (postgresql://[user]:[password]@[host]/[database]?[parameters] (from your postgres connection string))
   - Schema: neondb
   - Login: [user] (from your postgres connection string)
   - Password: [password] (from your postgres connection string)
   - Port: 5432
   - Extra: {"sslmode": "require", "channel_binding": "require"}

## Trigger the DAG
- Go to Admin -> DAGs and click on the DAG you want to trigger.
-It will run the DAG and you can see the logs in the UI.

## To stop airflow UI
```bash
astro dev stop
```
## Contact
If you have any questions or suggestions, please feel free to contact me at:  
Twitter: [@dhaivat00](https://x.com/dhaivat00)  
LinkedIn: [Dhaivat Jambudia](https://www.linkedin.com/in/dhaivat-jambudia/)