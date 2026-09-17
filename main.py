import os
import sys
from google.cloud import storage
import pysus


def run_oda_pipeline():

    # ==========================================
    # 1. CONFIGURAÇÕES
    # ==========================================

    BUCKET_NAME = "dados_alagoinhas_bronze"
    DESTINATION_FOLDER = "saude/natalidade"

    # Código IBGE de Alagoinhas utilizado pelo DataSUS
    COD_ALAGOINHAS = "290070"

    UF = "BA"

    # Intervalo da série histórica
    ANOS = range(2015, 2028)

    print("Iniciando pipeline de Natalidade (SINASC)...")

    # ==========================================
    # 2. INICIALIZAÇÃO
    # ==========================================

    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)

        print("Conexão com Google Cloud Storage estabelecida.")

    except Exception as e:
        print(f"ERRO CRÍTICO NA INICIALIZAÇÃO: {e}")
        sys.exit(1)

    # ==========================================
    # 3. PROCESSAMENTO POR ANO
    # ==========================================

    for year in ANOS:

        print(f"\n--- Processando ano: {year} ---")

        try:

            # ==========================================
            # DOWNLOAD SINASC - PYSUS MODERNO
            # ==========================================

            print(f"Baixando SINASC {UF}/{year}...")

            df = pysus.dadosgov.sinasc(
                state=UF,
                year=year,
                as_dataframe=True
            )

            print(
                f"Download concluído: "
                f"{len(df):,} registros na UF {UF}."
            )

            # ==========================================
            # VALIDAÇÃO DA COLUNA MUNICIPAL
            # ==========================================

            if "CODMUNRES" not in df.columns:

                print(
                    f"AVISO: Coluna 'CODMUNRES' "
                    f"não encontrada em {year}. Pulando."
                )

                continue

            # ==========================================
            # PADRONIZAÇÃO DO CÓDIGO MUNICIPAL
            # ==========================================

            df["CODMUNRES"] = (
                df["CODMUNRES"]
                .astype(str)
                .str.strip()
                .str.replace(".0", "", regex=False)
                .str.zfill(6)
            )

            # ==========================================
            # FILTRO DE ALAGOINHAS
            # ==========================================

            df_alagoinhas = df[
                df["CODMUNRES"] == COD_ALAGOINHAS
            ].copy()

            print(
                f"Registros de Alagoinhas em {year}: "
                f"{len(df_alagoinhas):,}"
            )

            # ==========================================
            # VALIDAÇÃO
            # ==========================================

            if df_alagoinhas.empty:

                print(
                    f"INFO: Nenhum registro para "
                    f"Alagoinhas em {year}."
                )

                continue

            # ==========================================
            # GERAÇÃO DO PARQUET
            # ==========================================

            local_filename = (
                f"natalidade_alagoinhas_{year}.parquet"
            )

            df_alagoinhas.to_parquet(
                local_filename,
                index=False
            )

            print(
                f"Parquet gerado: {local_filename}"
            )

            # ==========================================
            # UPLOAD PARA GOOGLE CLOUD STORAGE
            # ==========================================

            blob_path = (
                f"{DESTINATION_FOLDER}/"
                f"ano={year}/"
                f"{local_filename}"
            )

            blob = bucket.blob(blob_path)

            blob.upload_from_filename(
                local_filename
            )

            print(
                f"SUCESSO: {local_filename} "
                f"enviado para a camada Bronze."
            )

            print(
                f"Destino: gs://{BUCKET_NAME}/{blob_path}"
            )

            # ==========================================
            # LIMPEZA DO ARQUIVO LOCAL
            # ==========================================

            if os.path.exists(local_filename):
                os.remove(local_filename)

                print(
                    f"Arquivo temporário removido: "
                    f"{local_filename}"
                )

        except Exception as e:

            print(
                f"AVISO: Erro inesperado ao processar "
                f"o ano {year}."
            )

            print(f"Detalhe: {e}")

            continue

    # ==========================================
    # 4. FINALIZAÇÃO
    # ==========================================

    print("\nPipeline concluído com sucesso.")


if __name__ == "__main__":
    run_oda_pipeline()

