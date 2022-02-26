from pathlib import Path
from unittest import result
from jinja2 import Environment, FileSystemLoader
import sqlite3
import datetime
from functools import partial
import json

def query_all(cursor, sqlquery, *args):
    cursor.execute(sqlquery, args)
    description = cursor.description
    tmp = cursor.fetchall()
    result_set = [dict(zip([col[0] for col in description], row)) for row in tmp]
    for i, row in enumerate(tmp):
        result_set[i].update(dict(enumerate(row)))
    return result_set

def query_one(cursor, sqlquery, *args):
    cursor.execute(sqlquery, args)
    description = cursor.description
    tmp = cursor.fetchone()
    if tmp is None:
        return None
    result = dict(zip([col[0] for col in description], tmp))
    result.update(dict(enumerate(tmp)))
    return result

def to_dict(str):
    return json.loads(str)

def main():
    data_path = Path("static/data/")
    env = Environment(loader=FileSystemLoader("templates"))
    
    connection = sqlite3.connect('rep-database.db')
    cursor = connection.cursor()
    
    env.globals.update(fetchall=partial(query_all, cursor))
    env.globals.update(fetchone=partial(query_one, cursor))
    env.filters.update(to_dict=to_dict)
    
    for table in ["Entidades", "Processos", "EleicoesCorposGerentes", "AlteracoesEstatutos", "PK_IRCTs","Outorgantes","Avisos_Greve"]:

        data_table_path = data_path/table
        data_table_path.mkdir(parents=True, exist_ok=True)
        
        columns = query_all(cursor, f"SELECT name, pk FROM pragma_table_info('{table}') ORDER BY pk")
        pk = [row['name'] for row in columns if row['pk'] > 0]
        
        results = query_all(cursor, f"SELECT * FROM {table}")

        for row in results:
            name = ".".join(str(row[name]) for name in pk)
            env.get_template(f"{table}.html").stream(row=row, cursor=cursor, name=name, table=table, columns=columns).dump((data_table_path / f"{name}.html").open('w'))
            row['filename'] = name
        
        aggs = env.list_templates(filter_func=lambda name: name.startswith(f"{table}-"))
        for agg in aggs:
            out = agg[len(f"{table}-"):] # remove prefix added in 3.9
            env.get_template(agg).stream(cursor=cursor, table=table, rows=results, columns=columns).dump((data_table_path / out).open('w'))
    


if __name__ == "__main__":
    main()



def btetourl(strdate, numero):
    """ Deprecated. """
    date = datetime.date.fromisoformat(strdate)
    if not date:
        return ""
    if numero > 48:
        return strdate
    url = f"http://bte.gep.msess.gov.pt/completos/{date.year}/bte{numero}_{date.year}.pdf"
    return f"<a href='{url}' target='_blank'>{strdate}</a>"