from datetime import datetime
from math import isnan, nan
import sys
import pandas as pd
import re
import sqlite3
import re

def main():
    DATABASE_NAME = "rep-database.db"
    connection = sqlite3.connect(DATABASE_NAME)

    xl = pd.ExcelFile('greves.xlsx') # faster than reading the whole excel with pd.read_excel('greves.xlsx', sheet_name=None)

    names = (a for a in xl.sheet_names if re.match('^\d{4}$', a)) # filter the sheet names to only the years

    insertstmn="""INSERT INTO Avisos_Greve(
        inicio_ano,
        inicio_mes,
	    fim_ano,
        fim_mes,
        dadosExcel) VALUES (:ano_inicio, :mes_inicio, :ano_fim, :mes_fim, :dadosExcel)"""

    insertentidade = """INSERT INTO Avisos_Greve_Entidades(_id_greve, codEntG, codEntE, numAlt) VALUES (:id_aviso_greve, :codEntG, :codEntE, :numAlt)"""
    
    for name in names:
        print("Processing %s" % name, file=sys.stderr)
        sh: pd.DataFrame = xl.parse(name) # read the sheet
        ano = int(name)

        # columns to lowercase
        sh.columns = map(str.strip, map(str.lower, sh.columns))
        count = 0
        for _, row in sh.iterrows():
            inicio_ano = ano
            inicio_mes = get_inicial_month_value(row)
            if not inicio_mes:
                continue # ignore rows without
            fim_ano = get_end_year_value(row) or ano
            fim_mes = get_end_month_value(row)
            
            cursor = connection.execute(insertstmn, {"ano_inicio":inicio_ano, "mes_inicio":inicio_mes, "ano_fim":fim_ano, "mes_fim":fim_mes, "dadosExcel": row.to_json()})

            all_entities_ids = set(all_ids_iter(row, connection))
            for id in all_entities_ids:
                codEntG, codEntE, numAlt, *_ = id.split(".")+[None,None,None]
                assert codEntG is not None, f"codEntG is None id: {id}" 
                if codEntG is None:
                    continue
                if numAlt is None:
                    entidade = connection.execute("""SELECT codEntG, codEntE, numAlt FROM Organizacoes WHERE codEntG = :codEntG AND codEntE = :codEntE""", (codEntG,codEntE)).fetchone()
                else:
                    entidade = connection.execute("""SELECT codEntG, codEntE, numAlt FROM Entidades WHERE codEntG = :codEntG  AND codEntE = :codEntE  AND numAlt = :numAlt""", (codEntG,codEntE,numAlt)).fetchone()
                if entidade:
                    connection.execute(insertentidade, {"id_aviso_greve":cursor.lastrowid, "codEntG":entidade[0], "codEntE":entidade[1], "numAlt":entidade[2]})
            count+=1
            connection.commit()
        print("Inserted %d rows" % count, file=sys.stderr)

def get_ID_by_name_or_acronimo(name, connection):
    cursor = connection.cursor()
    cursor.execute("SELECT codEntG || \".\" || codEntE || \".\" || numAlt FROM Entidades WHERE nomeEntidade LIKE ? OR sigla LIKE ?", (name,name,))
    return cursor.fetchone()

def all_formated_ids(row: pd.Series):
    for id in re.finditer("\d+\.\d+\.\d+", str(row.get("código entidade ativa",""))):
        yield id.group(0)
    for id in re.finditer("\d+\.\d+\.\d+", str(row.get("código entidade sindical",""))):
        yield id.group(0)

def all_ids_from_columns(row: pd.Series, connection):
    for colname in ["tabela entidades ativas", "entidade patronal", "entidade sindical", "entidades ativas", "sindicato"]:
        for nome in str(row.get(colname,"")).split("/"):
            for subnome in nome.split("-"):
                if subnome.strip() != "":
                    id = get_ID_by_name_or_acronimo(subnome.strip(), connection)
                    if id:
                        yield id[0]
        


def all_ids_iter(row: pd.Series, connection):
    yielded = False
    for id in all_formated_ids(row):
        yielded = True
        yield id
    if not yielded:
        print("Fallback Search for IDs on row %s" % row[1], file=sys.stderr)
        for id in all_ids_from_columns(row, connection):
            yield id

def get_value(row: pd.Series, columns: list):
    for col in columns:
        if col in row and not pd.isnull(row[col]):
            return row[col]
    return None

def get_end_year_value(row: pd.Series):
    found_value = get_value(row, ["fim", "fimgreve", "dias", "data entrada", "dataentradapreaviso"])
    if isinstance(found_value, type(None)):
        return None
    if isinstance(found_value,datetime):
        return found_value.year
    if isinstance(found_value, int):
        # happens in 2019
        found_value = get_value(row, ["data entrada"])
        return found_value.year
    if isinstance(found_value, str):
        regres = re.search("\d{4}", found_value)
        if regres:
            return int(regres.group(0))
        return None
    #print( "Unexpected type for inicial_month_value: %s" % type(found_value), file=sys.stderr)
    return None

def get_inicial_month_value(row: pd.Series):
    found_value = get_value(row, ["início", "iniciogreve","dias","data entrada","dataentradapreaviso"])
    if isinstance(found_value, type(None)):
        return None
    if isinstance(found_value,datetime):
        return found_value.month
    if isinstance(found_value, int):
        # happens in 2019
        found_value = get_value(row, ["data entrada"])
        return found_value.month
    if isinstance(found_value, str):
        regres = re.search(r"\b((jan)|(fev)|(mar)|(abr)|(mai)|(jun)|(jul)|(ago)|(set)|(out)|(nov)|(dez))", found_value, re.I)
        if regres:
            months = {'jan':1,'fev':2,'mar':3,'abr':4,'mai':5,'jun':6,'jul':7,'ago':8,'set':9,'out':10,'nov':11,'dez':12}
            return months[regres.group(0).lower()]
        regres = re.search(r"\d{1,2}[/-](\d{1,2})(([/-]\d{4})?)", found_value)
        if regres:
            return int(regres.group(1))
        if re.search("feriado", found_value, re.I):
            return 1
        return get_value(row, ["data entrada"]).month
    print( "Unexpected type for inicial_month_value: %s" % type(found_value), file=sys.stderr)
    return None

def get_end_month_value(row: pd.Series):
    found_value = get_value(row, ["fim", "fimgreve", "dias", "data entrada", "dataentradapreaviso"])
    if isinstance(found_value, type(None)):
        return None
    if isinstance(found_value,datetime):
        return found_value.month
    if isinstance(found_value, int):
        # happens in 2019
        found_value = get_value(row, ["data entrada"])
        return found_value.month
    if isinstance(found_value, str):
        regres = re.findall(r"\b((jan)|(fev)|(mar)|(abr)|(mai)|(jun)|(jul)|(ago)|(set)|(out)|(nov)|(dez))", found_value, re.I)
        if regres:
            months = {'jan':1,'fev':2,'mar':3,'abr':4,'mai':5,'jun':6,'jul':7,'ago':8,'set':9,'out':10,'nov':11,'dez':12}
            if( len(regres) == 1 ):
                return int(months[regres[0][0].lower()])
            else:
                return int(months[regres[1][0].lower()])
        if re.search("(ti)|(ideter)|(indeter)|(pagamento)|(feriado)", found_value, re.I):
            return 12
        regres = re.findall(r"(\d{1,2}[/-](\d{1,2})([/-]\d{4})?)", found_value)
        if regres:
            if( len(regres) == 1):
                return int(regres[0][1])
            else:
                return int(regres[1][1])
        return get_value(row, ["data entrada"]).month
    return None
    
if __name__ == "__main__":
    main()

        
        

