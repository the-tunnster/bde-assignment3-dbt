import psycopg2
from psycopg2.extras import execute_values
import pandas as pd

def get_conn():
	conn = psycopg2.connect(
		database="postgres",
		user="postgres",
		password="root_password",
		host="127.0.0.1",
		port="5432"
	)
	return conn

def import_raw_census_data_g01():
	file_path = "./data/census/" + "2016Census_G01_NSW_LGA.csv"
    
	conn = get_conn()

	dataFrame = pd.read_csv(file_path)

	if len(dataFrame) > 0:
		sql = """
			INSERT INTO RAW.census_data_g01
				VALUES %s
		"""

		values = dataFrame.to_dict('split')
		values = values['data']

		result = execute_values(conn.cursor(), sql, values, page_size=len(dataFrame))
		conn.commit()
		conn.close()
	else:
		conn.close()

	return None

def import_raw_census_data_g02():
	file_path = "./data/census/" + "2016Census_G02_NSW_LGA.csv"
    
	conn = get_conn()

	dataFrame = pd.read_csv(file_path)

	if len(dataFrame) > 0:
		sql = """
			INSERT INTO RAW.census_data_g02
				VALUES %s
		"""

		values = dataFrame.to_dict('split')
		values = values['data']

		result = execute_values(conn.cursor(), sql, values, page_size=len(dataFrame))
		conn.commit()
		conn.close()
	else:
		conn.close()

	return None

def import_raw_listings():
	file_path = "./data/listings/"
	listings = ["01_2021.csv",
				"02_2021.csv",
				"03_2021.csv",
	 			"04_2021.csv",
				"05_2020.csv",
				"06_2020.csv",
				"07_2020.csv",
				"08_2020.csv",
				"09_2020.csv",
				"10_2020.csv",
				"11_2020.csv",
				"12_2020.csv"]

	conn = get_conn()

	for l in listings:
		dataFrame = pd.read_csv(file_path + l)

		if len(dataFrame) > 0:
			sql = """
				INSERT INTO RAW.listings
					VALUES %s
			"""

			values = dataFrame.to_dict('split')
			values = values['data']

			result = execute_values(conn.cursor(), sql, values, page_size=len(dataFrame))
			conn.commit()
		else:
			None
	
	return None

def import_raw_lga_code_name():
	file_path = "./data/lga/" + "NSW_LGA_CODE.csv"
    
	conn = get_conn()

	dataFrame = pd.read_csv(file_path)

	if len(dataFrame) > 0:
		sql = """
			INSERT INTO RAW.lga_code_name
				VALUES %s
		"""

		values = dataFrame.to_dict('split')
		values = values['data']

		result = execute_values(conn.cursor(), sql, values, page_size=len(dataFrame))
		conn.commit()
		conn.close()
	else:
		None

	return None

def import_raw_lga_name_suburb():
	file_path = "./data/lga/" + "NSW_LGA_SUBURB.csv"
    
	conn = get_conn()

	dataFrame = pd.read_csv(file_path)

	if len(dataFrame) > 0:
		sql = """
			INSERT INTO RAW.lga_name_suburb
				VALUES %s
		"""

		values = dataFrame[["LGA_NAME", "SUBURB_NAME"]].to_dict('split')
		values = values['data']

		result = execute_values(conn.cursor(), sql, values, page_size=len(dataFrame))
		conn.commit()
		conn.close()
	else:
		conn.close()

	return None

import_raw_listings()
