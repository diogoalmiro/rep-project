from datetime import date, datetime
from tokenize import Number
import numpy as np
import pandas as pd
import re
import sqlite3

DATABASE_NAME = "rep-database.db"
connection = sqlite3.connect(DATABASE_NAME)

xl = pd.ExcelFile('greves.xlsx') # faster than reading the whole excel with pd.read_excel('greves.xlsx', sheet_name=None)

names = (a for a in xl.sheet_names if re.match('^\d{4}$', a)) # filter the sheet names to only the years

cols_to_db = {
    'Código Entidade Ativa': '[Código Entidade Ativa]',
    'Código Entidade Sindical': '[Código Entidade Ativa]',
    'Código entidade Ativa': '[Código Entidade Ativa]',
    'Data entrada': '[Data Entrada]',
    'DATA ENTRADA': '[Data Entrada]',
    'DataEntradaPreAviso': '[Data Entrada]',
    'Entidade Patronal': '[Entidade Patronal]',
    'Entidade Sindical': '[Entidade Sindical]',
    'CAE': '[CAE]',
    'SETOR': '[Setor]',
    'Setor': '[Setor]',
    'Início': '[Início]',
    'InicioGreve': '[Início]',
    'Fim': '[Fim]',
    'FimGreve': '[Fim]',
    'DURAÇÃO': '[Duração]',
    'DIAS ': '[Dias]',
    'DIAS': '[Dias]',
    'Dias': '[Dias]',
    'Observacoes': '[Observações]',
    'OBSERVAÇÕES': '[Observações]',
    'Nº GREVE': None,
    'Nº Entrada': None,
    'Data APG': None,
    'N.º da greve': None,
    'Total Avisos Prévios de Greve': None,
    'Sigla': None,
    'Entidade ativa': None,
    'TÉC.': None,
    'Designação': None,
    'Âmbito': None,
    'Observações': None,
    'TS/TT': None,
    'TI': None,
    'SM': None,
    'REUNIÃO': None,
    'RESULTADO': None,
    'DC': None,
    'CES': None,
    'Tabela entidades ativas': None,
    'DC/CES': None,
    'CES-Hora final Reunião': None,
    'CES-Hora de Envio': None,
    'DC-Dt Envio Gov': None,
    'SINDICATO': None,
    'Entidades Ativas': None,
    'Entidades ativas': None,
    'EMPREGADOR': None,
    'NEC. SOCIAS IMPRETERÍVEIS': None,
    'Nº PROCESSO': None,
    'CONCILIADOR': None,
    'Entidade Ativa': None,
    'NIF': None,
    'Sindicato': None,
    'Empresa': None,
    'Nº do processo': None,
    'Conciliador': None
}

types = {
    'Data entrada': datetime,
    'DATA ENTRADA': datetime,
    'DataEntradaPreAviso': datetime,
    'Entidade Patronal': str,
    'Entidade Sindical': str,
    'Código Entidade Ativa': str,
    'SETOR': str,
    'Setor': str,
    'Início': datetime,
    'Fim': datetime,
    'InicioGreve': datetime,
    'FimGreve': datetime,
    'CAE': str,
    'Código Entidade Sindical': str,
    'Código entidade Ativa': str,
    'DURAÇÃO': str,
    'OBSERVAÇÕES': str,
    'Observacoes': str,
    'DIAS ': str,
    'DIAS': str,
    'Dias': str
}

keys = list(dict.fromkeys([ value for (key, value) in cols_to_db.items() if value is not None]))
connection.execute("DROP TABLE IF EXISTS Avisos_Greve_New")
connection.execute("""CREATE TABLE IF NOT EXISTS Avisos_Greve_New(%s)""" % ",".join(["%s TEXT DEFAULT ''" % key for key in keys]))


# Id_Entidade_Sindical Ano_Inicio Mes_Inicio Entidade_Sindical Entidade_Patronal Ano_Fim Mes_Fim Duracao
for name in names:
    sh: pd.DataFrame = xl.parse(name) # read the sheet
    curr_keys = [key for key in sh.columns if cols_to_db[key] is not None]
    insertstmt = """INSERT INTO Avisos_Greve_New(%s) VALUES (%s)""" % (",".join([cols_to_db[key] for key in curr_keys]), ",".join(["?" for _ in curr_keys]))
    cursor = connection.executemany(insertstmt, [
        sh.loc[i, curr_keys].fillna("").astype(str).values
        for i in sh.index])
    cursor.close()
    connection.commit()

    print("Inserted %s from sheet %s." % (cursor.rowcount, name))
    
    #print( sh.columns ) 
    #for row in sh.iterrows():
    #    insert_row = [value for ]
    #    for name in sh.columns.values:

            #print(" === ROW BELLOW === ")
        #print(row.index)
        #print(" === ROW ABOVE === ")
        #connection.execute(insertstmt)
    #    pass


"""    for _, row in sh.iterrows():
        sqlite_obj = {}
        sqlite_obj['Id_Entidades'] = row.get('Código Entidade Ativa') or row.get('Código Entidade Sindical') or row.get('Código entidade Ativa')



        print(row.get("Entidade Patronal"))
    print(*sh.columns.values, sep=", " )"""