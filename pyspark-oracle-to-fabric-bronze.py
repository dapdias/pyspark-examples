from pyspark.sql import SparkSession

def main():
    # =========================================================================
    # PASSO 1: Inicializar a SparkSession
    # =========================================================================
    # No Microsoft Fabric (Synapse Data Engineering), a SparkSession geralmente
    # já está disponível como `spark`, mas instanciá-la assim é uma boa prática
    # para rodar o script em outros ambientes ou testes locais.
    spark = SparkSession.builder \
        .appName("OracleToFabricBronze") \
        .config("spark.jars.packages", "com.oracle.database.jdbc:ojdbc8:21.3.0.0") \
        .getOrCreate()
        # O .config acima é para caso precise baixar o driver JDBC do Oracle.
        # No Fabric, talvez você precise fazer o upload do .jar do Oracle nos
        # "Environment settings" do workspace.

    # =========================================================================
    # PASSO 2: Configurar as variáveis de conexão com o banco Oracle
    # =========================================================================
    # Substitua pelas credenciais e dados reais do seu banco.
    # É fortemente recomendado usar o Azure Key Vault para armazenar senhas
    # e buscar os secrets via spark.conf ou mssparkutils (no Fabric).
    oracle_url = "jdbc:oracle:thin:@//meu-servidor-oracle.com:1521/MEU_SERVICO"
    oracle_user = "meu_usuario"
    oracle_password = "minha_senha_secreta"
    oracle_table = "SCHEMA.NOME_DA_TABELA"

    print(f"-> Conectando ao Oracle na tabela: {oracle_table}")

    # =========================================================================
    # PASSO 3: Ler os dados do Oracle para um DataFrame PySpark (Copy Data)
    # =========================================================================
    # Usamos o formato "jdbc" para conectar no banco.
    df_oracle = spark.read \
        .format("jdbc") \
        .option("url", oracle_url) \
        .option("dbtable", oracle_table) \
        .option("user", oracle_user) \
        .option("password", oracle_password) \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .option("fetchsize", "10000") \
        .load()
        # Dica: "fetchsize" ajuda na performance lendo lotes de 10.000 linhas
        # por vez, reduzindo as chamadas na rede.
        # Dica 2: Se a tabela for muito grande, considere usar partitionColumn,
        # lowerBound, upperBound e numPartitions para leitura em paralelo.

    # Exibe o esquema lido (as colunas e os tipos de dados)
    # df_oracle.printSchema()

    # =========================================================================
    # PASSO 4: Preparar o caminho de destino na Camada Bronze (Microsoft Fabric)
    # =========================================================================
    # No Fabric, usamos o OneLake. O caminho geralmente começa com "abfss://"
    # e aponta para a pasta Tables do seu Lakehouse.
    # Exemplo: abfss://<Workspace_Name>@onelake.dfs.fabric.microsoft.com/<Lakehouse_Name>.Lakehouse/Tables/<Nome_da_Tabela>
    # Em Notebooks do Fabric, você pode salvar direto como tabela gerenciada: "NOME_DA_TABELA_BRONZE"

    lakehouse_table_path = "Tables/tabela_oracle_bronze"
    # Usando caminho relativo que funciona no Fabric quando o Lakehouse padrão está atachado.

    print("-> Escrevendo os dados na camada Bronze do Fabric em formato Delta...")

    # =========================================================================
    # PASSO 5: Salvar os dados na Camada Bronze (Formato Delta)
    # =========================================================================
    # A camada Bronze é um espelho ("raw" ou "copy data") dos dados de origem,
    # por isso não fazemos transformações de negócios aqui (apenas salvamos o df_oracle).
    # O Microsoft Fabric usa "delta" como formato padrão e nativo.

    df_oracle.write \
        .format("delta") \
        .mode("overwrite") \
        .save(lakehouse_table_path)

    # .mode("overwrite"): Substitui todos os dados (Carga Full).
    # Para cargas incrementais, use .mode("append") e certifique-se de filtrar
    # no Oracle apenas os dados novos (ex: via query no option("dbtable")).

    print("-> Carga finalizada com sucesso!")

    # Para salvar como uma "Tabela Gerenciada" no Lakehouse do Fabric (para ver na UI):
    # df_oracle.write.format("delta").mode("overwrite").saveAsTable("tabela_oracle_bronze")

if __name__ == "__main__":
    main()
