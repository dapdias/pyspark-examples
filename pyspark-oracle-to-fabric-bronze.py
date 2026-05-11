from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

class OracleToFabricIngestor:
    """
    Classe utilitária para facilitar a ingestão de dados de um banco Oracle
    (via Gateway ou Endpoint) para a camada Bronze do Microsoft Fabric.
    """

    def __init__(self, spark: SparkSession):
        """
        Inicializa o ingestor.

        :param spark: Instância ativa da SparkSession.
        """
        self.spark = spark

    def read_from_oracle(
        self,
        jdbc_url: str,
        table_name: str,
        user: str,
        password: str,
        fetchsize: str = "10000"
    ) -> DataFrame:
        """
        Lê dados de uma tabela no Oracle e retorna um DataFrame PySpark.

        :param jdbc_url: URL JDBC de conexão apontando para o Gateway/Endpoint.
                         Exemplo: "jdbc:oracle:thin:@//<IP_DO_GATEWAY>:<PORTA>/<SERVICE_NAME>"
        :param table_name: Nome da tabela no banco (ex: SCHEMA.TABELA).
        :param user: Usuário do banco de dados.
        :param password: Senha do banco de dados.
        :param fetchsize: Tamanho do lote de leitura. Ajuda a melhorar a performance.
        :return: DataFrame contendo os dados extraídos do Oracle.
        """
        print(f"-> Iniciando leitura da tabela {table_name} via Endpoint/Gateway...")

        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", user) \
            .option("password", password) \
            .option("driver", "oracle.jdbc.driver.OracleDriver") \
            .option("fetchsize", fetchsize) \
            .load()

        print("-> Leitura concluída com sucesso.")
        return df

    def write_to_bronze_delta(
        self,
        df: DataFrame,
        destination_path: str,
        mode: str = "overwrite"
    ):
        """
        Escreve um DataFrame no formato Delta, padrão para a camada Bronze do Fabric.

        :param df: O DataFrame PySpark a ser salvo.
        :param destination_path: Caminho no OneLake onde a tabela será salva.
                                 Exemplo relativo: "Tables/minha_tabela_bronze"
                                 Exemplo absoluto: "abfss://Workspace@onelake.dfs.fabric.microsoft.com/Lakehouse.Lakehouse/Tables/minha_tabela"
        :param mode: "overwrite" (Carga Full) ou "append" (Carga Incremental).
        """
        print(f"-> Escrevendo dados no destino: {destination_path} (Modo: {mode})")

        df.write \
            .format("delta") \
            .mode(mode) \
            .save(destination_path)

        print("-> Carga na camada Bronze finalizada com sucesso!")


# =========================================================================
# Exemplo de uso da Classe (Você pode copiar a classe acima para seus projetos)
# =========================================================================
def main():
    # 1. Inicializa o Spark (No Fabric, 'spark' já está disponível nos notebooks)
    spark = SparkSession.builder \
        .appName("OracleToFabricIngestão") \
        .config("spark.jars.packages", "com.oracle.database.jdbc:ojdbc8:21.3.0.0") \
        .getOrCreate()

    # 2. Instancia nossa classe utilitária
    ingestor = OracleToFabricIngestor(spark)

    # 3. Define as variáveis (em produção, pegue essas credenciais do Azure Key Vault!)
    # Como você está usando um Gateway/Endpoint, aponte a URL para o seu Gateway:
    ENDPOINT_URL = "jdbc:oracle:thin:@//ip-do-seu-gateway-ou-endpoint:1521/SEU_SERVICO"
    USER = "usuario_oracle"
    PASSWORD = "senha_secreta"
    TABLE = "SCHEMA.VENDAS"

    # 4. Lê os dados
    df_vendas = ingestor.read_from_oracle(
        jdbc_url=ENDPOINT_URL,
        table_name=TABLE,
        user=USER,
        password=PASSWORD
    )

    # (Opcional) Visualiza os primeiros registros ou esquema
    # df_vendas.printSchema()

    # 5. Salva na camada Bronze no Fabric
    DESTINATION_PATH = "Tables/vendas_bronze"
    ingestor.write_to_bronze_delta(
        df=df_vendas,
        destination_path=DESTINATION_PATH,
        mode="overwrite"  # Use "append" se for carga incremental
    )

if __name__ == "__main__":
    main()
