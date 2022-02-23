from pathlib import Path
from jinja2 import Environment, FileSystemLoader
import sqlite3
import datetime

def btetourl(strdate, numero):
    """ Deprecated. """
    date = datetime.date.fromisoformat(strdate)
    if not date:
        return ""
    if numero > 48:
        return strdate
    url = f"http://bte.gep.msess.gov.pt/completos/{date.year}/bte{numero}_{date.year}.pdf"
    return f"<a href='{url}' target='_blank'>{strdate}</a>"

def main():
    data_path = Path("static/data/")
    env = Environment(loader=FileSystemLoader("templates"))
    
    connection = sqlite3.connect('rep-database.db')
    cursor = connection.cursor()
    
    for table in ["Entidades", "Processos", "EleicoesCorposGerentes", "AlteracoesEstatutos", "PK_IRCTs","Outorgantes"]:
        data_table_path = data_path/table
        data_table_path.mkdir(parents=True, exist_ok=True)
        cursor.execute(f"PRAGMA table_info({table})")
        columns = [column for column in cursor.fetchall()]
        pk = [row[1] for row in columns if row[5] > 0]
        names = [row[1] for row in columns]
        agg_rows = []
        for row in cursor.execute(f"SELECT * FROM {table}").fetchall():
            row = {names[i]: val for i, val in enumerate(row)}
            name = ".".join([str(row[col]) for col in pk])
            agg_rows.append(dict(row, filename=name))
            env.get_template(f"{table}.html").stream(row=row, cursor=cursor, name=name, table=table, columns=columns).dump((data_table_path / f"{name}.html").open('w'))
        
        aggs = env.list_templates(filter_func=lambda name: name.startswith(f"{table}-"))
        for agg in aggs:
            out = agg.removeprefix(f"{table}-")
            env.get_template(agg).stream(cursor=cursor, table=table, rows=agg_rows, columns=columns).dump((data_table_path / out).open('w'))
    


if __name__ == "__main__":
    main()