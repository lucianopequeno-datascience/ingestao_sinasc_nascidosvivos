import os
import sys
import pandas as pd
from google.cloud import storage
from pysus.online_data import SINASC 

def run_oda_pipeline():
    # 1. Configurações
    BUCKET_NAME = "dados_alagoinhas_bronze"
    DESTINATION_FOLDER = "saude/natalidade"
    COD_ALAGOINHAS = "2900702" 
    UF = "BA"
    
    # Intervalo da série histórica
    ANOS = range(20015, 2028) 
    
    print("Iniciando pipeline de Natalidade (SINASC)...")
    try:
        sinasc = SINASC.SINASC().load()
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
    except Exception as e:
        print(f"ERRO CRÍTICO NA INICIALIZAÇÃO: {e}")
        sys.exit(1)
    
    for year in ANOS:
        print(f"--- Processando ano: {year} ---")
        
        try:
            # CORREÇÃO AQUI: Inserido o parâmetro group='DN'
            arquivos = sinasc.get_files(group='DN', uf=UF, year=year)
            
            if not arquivos:
                print(f"INFO: Nenhum dado disponível no servidor para o ano {year}. Pulando...")
                continue

            print(f"Baixando dados de {year}...")
            df = arquivos[0].download().to_dataframe()
            
            # Filtro de município
            if 'CODMUNRES' in df.columns:
                df_alagoinhas = df[df['CODMUNRES'].astype(str) == COD_ALAGOINHAS]
            else:
                print(f"AVISO: Coluna 'CODMUNRES' não encontrada em {year}. Pulando.")
                continue
            
            if df_alagoinhas.empty:
                print(f"INFO: Nenhum registro para Alagoinhas em {year}.")
                continue
                
            # Upload:
            local_filename = f"natalidade_alagoinhas_{year}.parquet"
            df_alagoinhas.to_parquet(local_filename, index=False)
            
            # Subindo para o bucket particionado por ano
            blob = bucket.blob(f"{DESTINATION_FOLDER}/ano={year}/{local_filename}")
            blob.upload_from_filename(local_filename)
            print(f"SUCESSO: Arquivo {local_filename} enviado para a camada bronze.")
            
            # Limpeza local
            if os.path.exists(local_filename):
                os.remove(local_filename)
                
        except Exception as e:
            print(f"AVISO: Não foi possível processar o ano {year}. Detalhe: {e}")
            continue

    print("Pipeline concluído com sucesso.")

if __name__ == "__main__":
    run_oda_pipeline()
