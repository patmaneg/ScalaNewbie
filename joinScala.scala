val cities_df = spark.read.options(Map("inferschema"->"true", "header"->"true")).csv("/home/workspace/resources/lesson1/csv/cities.csv")
cities_df.printSchema()
cities_df.show()


val lines_df = spark.read.options(Map("Inferschema"->"true", "header"->"true")).csv("/home/workspace/resources/lesson1/csv/lines.csv")
lines_df.printSchema()
lines_df.show()

cities_df.join(lines_df, cities_df("id") === lines_df("city_id"), "inner").show()

cities_df.join(lines_df, cities_df("id") === lines_df("city_id"), "inner").explain()

val joined_df = cities_df.join(lines_df, cities_df("id") === lines_df("city_id"), "inner")
joined_df.groupBy("city_id").count().orderBy(asc("count")).show()
joined_df.groupBy("city_id").count().orderBy(desc("count")).explain()
#Limitamos los resultados a 10
joined_df.groupBy("city_id").count().orderBy(desc("count")).limit(10).show()

joined_df.join(top10_df, joined_df("city_id") === top10_df("city_id"), "inner").filter(joined_df("city_id")===4).show()
