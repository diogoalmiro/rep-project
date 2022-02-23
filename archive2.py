from pathlib import Path
from unittest import result
from jinja2 import Environment, FileSystemLoader
import sqlite3
import datetime
from functools import partial

def query(cursor, sqlquery, *args):
    cursor.execute(sqlquery, args)
    description = cursor.description
    results = cursor.fetchall()
    return [dict(zip([col[0] for col in description], row)) for row in results]

def main():
    data_path = Path("static/data/")
    env = Environment(loader=FileSystemLoader("templates"))
    
    connection = sqlite3.connect('rep-database.db')
    cursor = connection.cursor()
    fetchall = partial(query, cursor)
    env.globals.update(fetchall=fetchall)
    
    for table in ["Entidades", "Processos", "EleicoesCorposGerentes", "AlteracoesEstatutos", "PK_IRCTs","Outorgantes"]:

        data_table_path = data_path/table
        data_table_path.mkdir(parents=True, exist_ok=True)
        
        columns = fetchall(f"PRAGMA table_info({table})")
        pk = [row['name'] for row in columns if row['pk'] > 0]
        
        results = fetchall(f"SELECT * FROM {table}")

        for row in results:
            name = ".".join([str(row[col]) for col in pk])
            env.get_template(f"{table}.html").stream(row=row, cursor=cursor, name=name, table=table, columns=columns).dump((data_table_path / f"{name}.html").open('w'))
            row['filename'] = name
        
        aggs = env.list_templates(filter_func=lambda name: name.startswith(f"{table}-"))
        for agg in aggs:
            out = agg.removeprefix(f"{table}-")
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