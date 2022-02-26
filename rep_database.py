from typing import List
import urllib.request
import json
import base64
import sqlite3
import os
from datetime import datetime
import greves
import archive2

#com autenticacao
def getResponse(tabela: str, ano: int = 0) -> List[object]:
	username = os.environ["DGERT_LOGIN_USER"]
	password = os.environ["DGERT_LOGIN_PASSWORD"]
	request = urllib.request.Request("https://www.dgert.gov.pt/application-dgert-projeto-rep-php/" + tabela + ".php?ano=" + str(ano))
	string = '%s:%s' % (username, password)
	base64string = base64.standard_b64encode(string.encode('utf-8'))
	request.add_header("Authorization", "Basic %s" % base64string.decode('utf-8'))   
	response = urllib.request.urlopen(request)
	data = response.read()
	result = data.decode("utf8")
	response.close()
	obj_or_empty_list = json.loads(result)
	if not obj_or_empty_list:
		return list()
	else:
		return list(obj_or_empty_list.values())

def table_structure(tabela: str):
	types = {}
	for ano in range(1975, datetime.now().year + 1):
		data = getResponse(tabela, ano)
		for row in data:
			for key in row.keys():
				if key not in types:
					types[key] = type(row[key])
				else:
					if types[key] != type(row[key]):
						print(f"{key} {types[key]} {type(row[key])}")
	return types

def create_database_tables(cursor):
	with open('rep-database.sql', 'r') as sql_file:
		cursor.executescript(sql_file.read())


def insert_from_table(cursor: sqlite3.Cursor, table: str, ano: int = 0):
	data = getResponse(table, ano)
	errors = 0
	for row in data:
		keys = ",".join(row.keys())
		valuesTemplate = ",".join(["?"] * len(row.keys()))
		values = list(o.strip() if type(o) is str else o for o in row.values())
		try:
			cursor.execute(f"""INSERT INTO {table}({keys}) VALUES({valuesTemplate});""", values)
		except Exception as e:
			if "FOREIGN KEY constraint failed" in str(e):
				pass
			print(f"{table} {row} {str(e)}")
			errors += 1
	return errors

def main():
	if 'rep-database.db' in os.listdir('.'):
		os.remove('rep-database.db')
	connection = sqlite3.connect('rep-database.db')
	cursor = connection.cursor()
	create_database_tables(cursor)
	errors = insert_from_table(cursor, "entidades", 0)
	print(f"Errors inserting entidades: {errors}")
	for table in ["processos", "eleicoesCorposGerentes", "alteracoesEstatutos", "ircts","outorgantes"]:
		errors = 0
		for ano in range(1975, datetime.now().year + 1):
			errors += insert_from_table(cursor, table, ano)
		print(f"Errors inserting {table}: {errors}")
	connection.commit()
	greves.main()
	archive2.main()

if __name__ == '__main__':
	main()
