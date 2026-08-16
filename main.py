import os
import sys
import pandas as pd
from google.cloud import storage
from pysus.online_data.SINASC import download  # Importação corrigida

def run_oda_pipeline():
    # 1. Configurações
    BUCKET_NAME = "dados_alagoinhas_bronze"
    DESTINATION_FOLDER = "saude/natalidade"
    
    # CORREÇÃO 1: DataSUS usa código IBGE de 6 dígitos (sem o dígito verificador)
    COD_ALAGOINHAS = "290070" 
    UF = "BA"
    
    # Intervalo da série histórica
    ANOS = range(2015, 2028) 
    
    print("Iniciando pipeline de Natalidade (SINASC)...")
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
    except Exception as e:
        print(f"ERRO CRÍTICO NA INICIALIZAÇÃO: {e}")
        sys.exit(1)
    
    for year in ANOS:
        print(f"\n--- Processando ano: {year} ---")
        
        try:
            # CORREÇÃO 2: Uso direto da função download do PySUS
            print(f"Baixando dados do servidor DataSUS para {UF} em {year}...")
            df = download(UF, year)
            
            if df is None or df.empty:
                print(f"INFO: Nenhum dado retornado para {year}. Pulando...")
                continue
            
            # Filtro de município
            if 'CODMUNRES' in df.columns:
                # Limpa espaços em branco e garante que é string para comparar
                df['CODMUNRES'] = df['CODMUNRES'].astype(str).str.strip()
                df_alagoinhas = df[df['CODMUNRES'] == COD_ALAGOINHAS]
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
                
        except ValueError:
            # O PySUS costuma retornar ValueError quando o arquivo de um ano não existe no FTP
            print(f"INFO: Os dados de {year} ainda não estão disponíveis no DataSUS.")
        except Exception as e:
            print(f"AVISO: Erro inesperado ao processar o ano {year}. Detalhe: {e}")
            continue

    print("\nPipeline concluído com sucesso.")

if __name__ == "__main__":
    run_oda_pipeline()
