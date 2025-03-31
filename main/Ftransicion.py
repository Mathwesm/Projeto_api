import pandas as pd  
from dotenv import load_dotenv 
import os 
from def_url import chamar_api_myfinance


load_dotenv()


url = "https://myfin-financial-management.bubbleapps.io/api/1.1/obj/transactions"
token = os.getenv("API_TOKEN")
headers = {"Authorization": f"Bearer {token}"}

chamar_api_myfinance(url)

lista_dados_todas_paginas = chamar_api_myfinance(url)

df = pd.DataFrame(lista_dados_todas_paginas)  

df.to_excel('fTransactions.xlsx', index=False)
df.to_parquet('fTransactions.parquet')