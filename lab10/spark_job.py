from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

# Ініціалізуємо Spark-сесію
spark = SparkSession.builder \
    .appName("Income Analysis") \
    .getOrCreate()

# Ініціалізуємо дані з якими будемо працювати
data = [
    (1, "Черкаси", "Черкаська", 280000, 15000),
    (2, "Умань", "Черкаська", 85000, 14000),
    (3, "Сміла", "Черкаська", 69000, 13500),
    (4, "Кропивницький", "Кіровоградська", 230000, 14500),
    (5, "Олександрія", "Кіровоградська", 78000, 13000),
    (6, "Світловодськ", "Кіровоградська", 45000, 12500),
    (7, "Вінниця", "Вінницька", 370000, 16000),
    (8, "Жмеринка", "Вінницька", 35000, 12000),
    (9, "Могилів-Подільський", "Вінницька", 31000, 11000)
]

columns = ["id", "city", "region", "population", "average_income"]

# Створюємо DataFrame
df = spark.createDataFrame(data, columns)

# Вивід даних в DataFrame
print("Початкові дані:")
df.show()

# Обчислення загального доходу для кожного регіону
df_region_income = df.groupBy("region") \
    .agg(sum(df.population * df.average_income).alias("total_income"))

# Виводимо загальний дохід для кожного регіону
print("Загальний дохід за регіонами:")
df_region_income.show()

# Завершення Spark-сесії
spark.stop()

