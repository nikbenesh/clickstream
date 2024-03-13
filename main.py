from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import pandas as pd
import sys


def row_f(row, routes, prev_row, last_row):
	# print('debug: previous row is', prev_row)
	if (row['event_type'] == 'page' or 'error' in row['event_type']):
		page = row['event_page']
		if ('error' in row['event_type']):
			page = 'error'

		if (prev_row[1] != row['session_id']):
			if (len(routes) > 0):
				print('Session:', prev_row[1], 'Route:', routes[-1])
			routes.append(page)
		else:
			routes[-1] += '-' + page
		
	if (row['timestamp'] == last_row[4] and list(row) == last_row): # if it is the last row
		# write the routes dictionary to a file, since it's impossible to return it otherwise
		pd.DataFrame({'route': [val[5:] if 'err__' in val else val for val in routes]}).to_csv('routes.csv', sep='\t', index=False)
		print('Finished writing routes to csv file')
	prev_row[1] = row['session_id']
	# print(len(routes_dict))

def get_routes(data_file, out_file, spark):
	df = spark.read.csv(data_file, sep='\t', header=True).orderBy(f.col('timestamp'))
	rdd = df.rdd
	rdd = rdd.filter(lambda row: row['event_type'] != 'event')
	last_row = list(rdd.toDF().toPandas().iloc[-1])
	# pd_df = rdd.toDF().toPandas()
	# index = -1
	# while (pd_df.iloc[index]['event_type'] != 'page'):
	# 	index -= 1
	# last_row = pd_df.iloc[index]
	print('Last row:', last_row)
	routes = []
	prev_row = [0, last_row[1]]
	rdd.foreach(lambda row: row_f(row, routes, prev_row, last_row))
	
	routes_df = spark.read.csv('routes.csv', sep='\t', header=True)
	return routes_df


spark = SparkSession.builder.master("local[1]").appName("user routes on site").getOrCreate()
# filename = 'clickstream.csv'
filename = "hdfs://hadoop2-10.yandex.ru:8020/data/lsml/sga/clickstream.csv" # uncomment this line for hive version
routes_df = get_routes(filename, 'routes.csv', spark)

result_table = routes_df.filter(f.col('route') != f.lit('')).groupBy('route').count().orderBy(f.col('count').desc()).limit(30)

result_table.toPandas().to_csv('output.csv', sep='\t', index=False)
print('Finished writing the result table to the output file (output.csv)')
