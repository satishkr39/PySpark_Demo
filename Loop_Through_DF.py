from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('myappp').getOrCreate()

input_data = [(1, "Shivansh", "Data Scientist", "Noida"),
              (2, "Rishabh", "Software Developer", "Banglore"),
              (3, "Swati", "Data Analyst", "Hyderabad"),
              (4, "Amar", "Data Analyst", "Noida"),
              (5, "Arpit", "Android Developer", "Pune"),
              (6, "Ranjeet", "Python Developer", "Gurugram"),
              (7, "Priyanka", "Full Stack Developer", "Banglore")]

schema = ["Id", "Name", "Job Profile", "City"]

df  = spark.createDataFrame(input_data, schema)
print(df.show())

# loop through using collect
df_collect = df.collect()
for row in df_collect:
    print(row["Id"], row["Name"], "  ", row["City"])

# using toLocalIterator
df_toLocalIterator = df.toLocalIterator()
for row in df_toLocalIterator:
    print(df["Id"], df["Name"])


# using iterrows
df_pandas = df.toPandas()
for row in df_pandas.iterrows():
    print(row[0], row[1])