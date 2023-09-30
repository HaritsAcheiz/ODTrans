import pandas as pd
from dataclasses import dataclass
import os
from dotenv import load_dotenv
import sys
import psycopg2
import csv

load_dotenv()

@dataclass
class ODTrans:

	def connect(self, host, database, username, password):
		try:
			print('Connecting...')
			conn = psycopg2.connect(host=host, database=database, user=username, password=password)
		except (Exception, psycopg2.DatabaseError) as error:
			print(error)
			sys.exit(1)

		print('All good, Connection successful!')

		return conn


	def extract(self, conn, query):
		print('Extracting...')
		try:
			cursor = conn.cursor()
			cursor.execute(query)
		except (Exception, psycopg2.DatabaseError) as error:
			print(error)
		results = cursor.fetchall()

		with open("data/output.csv", "w", newline="") as f:
    			writer = csv.writer(f)

			# write the column names
    			writer.writerow([col[0] for col in cursor.description])

	    		# write the query results
    			writer.writerows(results)
		
		cursor.close()
		conn.close()

		print('Download Completed!')


	def transform(self):
		pass
	
	def load(self):
		pass

	def run(self):
		pass

if __name__ == '__main__':
	host = os.getenv('HOST')
	database = os.getenv('DATABASE')
	username = os.getenv('DBUSERNAME')
	password = os.getenv('DBPASSWORD')
	
	odt = ODTrans()
	conn = odt.connect(host=host, database=database, username=username, password=password)
	
	gate_in_query = '''
	SELECT * FROM ctm.t_trx_gate AS trx WHERE trx.gate_in_boo = True LIMIT 10
	'''

	gate_out_query = '''
	SELECT * FROM ctm.t_trx_gate AS trx WHERE trx.gate_in_boo = False LIMIT 10
	'''	

	tob_in_query = '''
	SELECT * FROM ctm.t_trx_gate_test_integration AS trx WHERE trx.gate_in_boo = True LIMIT 10
	'''

	tob_out_query = '''
	SELECT * FROM ctm.t_trx_gate_test_integration AS trx WHERE trx.gate_in_boo = True LIMIT 10
	'''

	odt.extract(conn=conn, query=gate_in_query)
