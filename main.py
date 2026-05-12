import os
import pandas as pd
from google.cloud import storage
from pysus.online_data import SINASC

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """Faz o upload de um arquivo para o bucket do GCS."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f"Sucesso: {source_file_name} -> gs://{bucket_name}/{destination_blob_name}")

def main():
    bucket_name = "dados_alagoinhas_bronze"
    prefix = "natalidade"
    uf = "BA"
    # O DATASUS utiliza o código IBGE de 6 dígitos (sem o dígito verificador)
    cod_ibge_alagoinhas = "290070" 
    
    anos = list(range(2000, 2029))

    print("Carregando metadados do SINASC...")
    sinasc = SINASC().load()
    
    for ano in anos:
        print(f"Buscando arquivos para {uf} no ano {ano}...")
        try:
            files = sinasc.get_files("DN", uf=uf, year=ano)
            
            if not files:
                print(f"Nenhum arquivo encontrado no DATASUS para o ano {ano}. Pulando...")
                continue
                
            downloaded_files = sinasc.download(files)
            
            for file_path in downloaded_files:
                file_path_str = str(file_path)
                file_name = os.path.basename(file_path_str)
                
                # Carrega o parquet completo da Bahia
                df_ba = pd.read_parquet(file_path_str)
                
                # Filtra os dados: CODMUNRES é o Município de Residência da mãe
                # Se preferir por ocorrência, mude para 'CODMUNNASC'
                df_alagoinhas = df_ba[df_ba['CODMUNRES'] == cod_ibge_alagoinhas]
                
                if df_alagoinhas.empty:
                    print(f"Sem registros de Alagoinhas encontrados em {file_name}. Pulando upload.")
                    os.remove(file_path_str)
                    continue
                
                # Salva temporariamente o arquivo filtrado no container
                filtered_file_name = f"alagoinhas_{file_name}"
                df_alagoinhas.to_parquet(filtered_file_name, index=False)
                
                # Novo particionamento adequado para a modelagem:
                # natalidade/cod_mun=290070/ano=YYYY/arquivo.parquet
                destination_blob_name = f"{prefix}/cod_mun={cod_ibge_alagoinhas}/ano={ano}/{file_name}"
                
                # Upload do dado filtrado para a camada bronze
                upload_to_gcs(bucket_name, filtered_file_name, destination_blob_name)
                
                # Limpeza dupla do disco local do container para liberar espaço
                os.remove(file_path_str)
                os.remove(filtered_file_name)
                
        except Exception as e:
            print(f"Aviso: Falha ao processar o ano {ano}. Erro: {e}")

if __name__ == "__main__":
    main()