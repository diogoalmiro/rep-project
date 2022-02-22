from pathlib import Path
from jinja2 import Environment, FileSystemLoader
import sqlite3
import datetime

def btetourl(strdate, numero):
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
    env.filters['btetourl'] = btetourl
    
    connection = sqlite3.connect('rep-database.db')
    cursor = connection.cursor()
    
    for table in ["Entidades", "Processos", "EleicoesCorposGerentes", "AlteracoesEstatutos", "PK_IRCTs","Outorgantes"]:
        data_table_path = data_path/table
        data_table_path.mkdir(parents=True, exist_ok=True)
        cursor.execute(f"PRAGMA table_info({table})")
        columns = [column for column in cursor.fetchall()]
        pk = [row[5] for row in columns if row[5] > 0]
        names = [row[1] for row in columns]
        for row in cursor.execute(f"SELECT * FROM {table}").fetchall():
            name = ".".join([str(row[col-1]) for col in pk])
            row = {names[i]: val for i, val in enumerate(row)}
            env.get_template(f"{table}.html").stream(row=row, cursor=cursor, name=name, table=table).dump((data_table_path / f"{name}.html").open('w'))


if __name__ == "__main__":
    main()