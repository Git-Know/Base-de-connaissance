from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, col

# 🔹 1. Créer la SparkSession
spark = SparkSession.builder.appName("Test").getOrCreate()

# 🔹 2. Lire le fichier CSV
df = spark.read.csv("C:/Users/merye/Base-de-connaissance/data/contributors.csv", header=True, inferSchema=True)

# 🔹 3. Afficher les données brutes
print("📄 Données d'origine :")
df.show()

# 🔹 4. Contributeurs récents uniquement
print("\n🟢 Contributeurs récents (recent_contributor = True) :")
df.filter(col("recent_contributor") == True).show()

# 🔹 5. Auteurs principaux uniquement
print("\n⭐ Auteurs principaux (primary_author = True) :")
df.filter(col("primary_author") == True).show()

# 🔹 6. Nombre total de contributions par module
print("\n📊 Total des contributions par module :")
df.groupBy("module_path") \
  .agg(sum("contributions").alias("total_contributions")) \
  .show()

# 🔹 7. Nombre de contributeurs par module
print("\n👥 Nombre de contributeurs par module :")
df.groupBy("module_path") \
  .agg(count("author_name").alias("contributor_count")) \
  .show()

# 🔹 8. Contributeurs récents par module
print("\n🕒 Contributeurs récents par module :")
df.filter(col("recent_contributor") == True) \
  .groupBy("module_path") \
  .agg(count("author_name").alias("recent_contributor_count")) \
  .show()

# 🔹 9. Trier les contributeurs par nombre de commits (décroissant)
print("\n📈 Classement des contributeurs par commits :")
df.orderBy(col("contributions").desc()).show()

# 🔹 10. Auteurs récents ET principaux
print("\n✅ Auteurs récents ET principaux :")
df.filter((col("primary_author") == True) & (col("recent_contributor") == True)).show()
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, col
import matplotlib.pyplot as plt
import seaborn as sns

# 1. Créer la SparkSession
spark = SparkSession.builder.appName("Test").getOrCreate()

# 2. Lire le fichier CSV
df = spark.read.csv("C:/Users/merye/Base-de-connaissance/data/contributors.csv", header=True, inferSchema=True)

# 3. Afficher les données brutes
print("📄 Données d'origine :")
df.show()

# 4. Contributeurs récents uniquement
print("\n🟢 Contributeurs récents (recent_contributor = True) :")
df.filter(col("recent_contributor") == True).show()

# 5. Auteurs principaux uniquement
print("\n⭐ Auteurs principaux (primary_author = True) :")
df.filter(col("primary_author") == True).show()

# 6. Nombre total de contributions par module
print("\n📊 Total des contributions par module :")
contributions_df = df.groupBy("module_path") \
  .agg(sum("contributions").alias("total_contributions"))
contributions_df.show()

# 7. Afficher un graphique du total des contributions par module
print("\n🎨 Affichage graphique du total des contributions par module...")

# Convertir en Pandas DataFrame
pandas_df = contributions_df.toPandas()

# Créer le graphique
plt.figure(figsize=(8, 5))
sns.barplot(data=pandas_df, x="module_path", y="total_contributions", palette="Blues_d")
plt.title("Total des contributions par module")
plt.xlabel("Module")
plt.ylabel("Nombre de contributions")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
