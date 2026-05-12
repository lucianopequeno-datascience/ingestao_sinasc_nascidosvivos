import os
from google.cloud import storage
from pysus import SINASC
from pysus.utilities.readdbc import read_dbc

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(f"Upload concluído: {destination_blob_name}")

def main():

    bucket_name = "dados_alagoinhas_bronze"

    prefix = "natalidade"

    uf = "BA"

    cod_alagoinhas = "2900702"

    anos = list(range(2000, 2026))

    print("Inicializando SINASC...")

    sinasc = SINASC().load()

    for ano in anos:

        try:

            print(f"Processando ano {ano}")

            files = sinasc.get_files("DN", uf=uf)

            # filtra apenas arquivos do ano
            files = [f for f in files if str(ano) in str(f)]

            if not files:
                print(f"Nenhum arquivo encontrado para {ano}")
                continue

            downloaded_files = sinasc.download(files)

            for file_ref in downloaded_files:

                print(f"Lendo arquivo: {file_ref}")

                df = read_dbc(str(file_ref))

                df_filtrado = df[
                    df["CODMUNRES"].astype(str) == cod_alagoinhas
                ]

                if not df_filtrado.empty:

                    file_name = os.path.basename(str(file_ref))

                    parquet_name = f"{file_name}.parquet"

                    df_filtrado.to_parquet(
                        parquet_name,
                        index=False
                    )

                    destination = (
                        f"{prefix}/"
                        f"cod_mun={cod_alagoinhas}/"
                        f"ano={ano}/"
                        f"{parquet_name}"
                    )

                    upload_to_gcs(
                        bucket_name,
                        parquet_name,
                        destination
                    )

                    os.remove(parquet_name)

                # limpa dbc local
                if os.path.exists(str(file_ref)):
                    os.remove(str(file_ref))

        except Exception as e:
            print(f"Erro no ano {ano}: {e}")

if __name__ == "__main__":
    main()