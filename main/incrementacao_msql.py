import pandas as pd
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DECIMAL, DATETIME, text, inspect
from sqlalchemy.exc import IntegrityError
from def_url import chamar_api_myfinance



load_dotenv()

url = "https://myfin-financial-management.bubbleapps.io/api/1.1/obj/transactions"
token = os.getenv("API_TOKEN")
headers = {"Authorization": f"Bearer {token}"}

chamar_api_myfinance(url)

print("Obtendo dados da API...")
lista_dados_todas_paginas = chamar_api_myfinance(url)
df = pd.DataFrame(lista_dados_todas_paginas)
print(f"Total de registros obtidos: {len(df)}")


data_columns = ['Modified Date', 'Created Date', 'estimated_date', 'payment_date']
for col in data_columns:
    df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')

db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")


engine = create_engine(f'mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}')

try:
    with engine.connect() as conn:
        print(f"Conexão bem-sucedida com o banco de dados: {db_name}")
except Exception as e:
    print(f"Erro ao conectar ao banco de dados: {e}")
    exit()

metadata = MetaData()


transactions_table = Table('transactions', metadata,
    Column('Modified Date', DATETIME),
    Column('Created Date', DATETIME),
    Column('Created By', String(255)),
    Column('estimated_date', DATETIME),
    Column('recipient_ref', String(255)),
    Column('status', String(255)),
    Column('amount', DECIMAL(10, 2)),
    Column('year_ref', Integer),
    Column('payment_date', DATETIME),
    Column('OS_type-transaction', String(255)),
    Column('user_ref', String(255)),
    Column('cod_ref', String(255)),
    Column('month_ref', Integer),
    Column('OS_frequency-type', String(255)),
    Column('_id', String(255), primary_key=True)
)


inspector = inspect(engine)
if not inspector.has_table('transactions'):
    print("Tabela não existe. Criando nova tabela...")
    metadata.create_all(engine)
else:
    print("Tabela já existe. Verificando colunas...")
    existing_columns = [col['name'] for col in inspector.get_columns('transactions')]
    defined_columns = [col.name for col in transactions_table.columns]
    
 
    with engine.begin() as conn:
        for column in defined_columns:
            if column not in existing_columns:
                col_def = next(col for col in transactions_table.columns if col.name == column)
                col_type = col_def.type.compile(engine.dialect)
                print(f"Adicionando coluna faltante: {column} ({col_type})")
                stmt = text(f"ALTER TABLE transactions ADD COLUMN `{column}` {col_type}")
                conn.execute(stmt)


df_columns = df.columns.tolist()
table_columns = [col.name for col in transactions_table.columns]
columns_to_keep = [col for col in df_columns if col in table_columns]
df = df[columns_to_keep]


print("Tentando inserção em lote...")
try:
    df.to_sql('transactions', con=engine, if_exists='append', index=False, method='multi')
    print(f"Sucesso! {len(df)} registros inseridos em lote.")
except Exception as bulk_error:
    print(f"Falha na inserção em lote: {bulk_error}. Tentando registro por registro...")

    inseridos_com_sucesso = 0
    duplicados = 0
    outros_erros = 0
    
    for index, row in df.iterrows():
        try:
            row.to_frame().T.to_sql(
                'transactions', 
                con=engine, 
                if_exists='append', 
                index=False
            )
            inseridos_com_sucesso += 1
        except IntegrityError as e:
            if 'Duplicate entry' in str(e.orig):
                duplicados += 1
            else:
                outros_erros += 1
                print(f"Erro ao inserir registro {index}: {e}")
        except Exception as e:
            outros_erros += 1
            print(f"Erro inesperado ao inserir registro {index}: {e}")
    
    print("\nResumo da inserção:")
    print(f"- Registros inseridos com sucesso: {inseridos_com_sucesso}")
    print(f"- Registros duplicados (ignorados): {duplicados}")
    print(f"- Registros com outros erros: {outros_erros}")

print("Processo concluído.")    