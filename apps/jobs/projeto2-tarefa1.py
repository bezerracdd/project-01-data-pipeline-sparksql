# Projeto 2 - Banco de Dados, Machine Learning e Pipeline ETL em Cluster Spark Para Detectar Anomalias em Transações Financeiras
# Tarefa 1 - Criando o Primeiro Pipeline Para Listar Anomalias nas Transações Financeiras

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Inicializando a sessão Spark
spark = SparkSession.builder \
    .appName("DSA Projeto 2 - Tarefa 1") \
    .getOrCreate()

# Configurando o nível de log
spark.sparkContext.setLogLevel('ERROR')

# Carregando dados do CSV (caminho absoluto no container)
df_dsa = spark.read.csv(
    '/opt/spark/data/dados1_cap05.csv',
    header=True,
    inferSchema=True
)

# Visualizando os dados
df_dsa.show()

# Definindo um limite realista para anomalias
LIMITE_ANOMALIA = 500

# Filtrando anomalias com base nas unidades vendidas
anomalias = df_dsa.filter(col('unidades_vendidas') > LIMITE_ANOMALIA)

# Exibindo anomalias detectadas
anomalias.show()

# Contagem total de linhas no DataFrame de anomalias
print(f"Total de linhas no DataFrame de anomalias: {anomalias.count()}")

# Salvando o resultado em CSV
anomalias.write \
    .mode('overwrite') \
    .csv('/opt/spark/data/resultado_tarefa1', header=True)

# Encerrando a sessão Spark
spark.stop()

print("\nObrigado DSA. Execução do Job Concluída com Sucesso!\n")
