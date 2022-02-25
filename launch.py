from email.policy import default
import os
import sqlite3
import subprocess
import sys
import threading
from datetime import date, datetime
from io import BytesIO
import click

import pandas as pd
from flask import Flask, jsonify, render_template, request, send_file, send_from_directory, redirect

import rep_database

DATABASE_NAME = "rep-database.db"

if "DGERT_LOGIN_USER" not in os.environ:
    print("ERROR: enviroment variable DGERT_LOGIN_USER not set")
    sys.exit(1)

dgert_user = os.environ["DGERT_LOGIN_USER"]

if "DGERT_LOGIN_PASSWORD" not in os.environ:
    print("ERROR: enviroment variable DGERT_LOGIN_PASSWORD not set")
    sys.exit(1)

dgert_password = os.environ["DGERT_LOGIN_PASSWORD"]

if "SQLITE_WEB_PASSWORD" not in os.environ:
    print("Hint: set the password for sqlite_web with the environment variable: SQLITE_WEB_PASSWORD", file=sys.stderr)

# Start sqlite_web server on a new thread at port 8090
def run_sqlite_web(host="0.0.0.0", port=8090):
    subprocess.run(["sqlite_web",DATABASE_NAME,"--port",port,"--no-browser", "--host",host, "--password", "--read-only"])


updating = False
def regular_update():
    global updating
    print("Updating database...")
    try:
        updating = True
        rep_database.repDatabase()
        updating = False
        # Sleep for one day
        threading.Timer(86400, regular_update).start()
    except Exception as e:
        print("Error updating database: ", e)
        # Sleep for 15min
        threading.Timer(900, regular_update).start()

# Create a new flask web server
app = Flask(__name__)
app.config["APPLICATION_ROOT"] = "/dot/"


@app.route("/")
def index():
    return redirect("/index.html")

ROWS_PER_PAGE = 50

@app.route("/dashboard-search/")
def server_side_search():
    search = {}
    search["table"] = request.args.get('table_org', "")
    search["term"] = request.args.get('organization', "")
    search["distrito"]  = request.args.get('distrito', "")
    search["setor"] = request.args.get('setor', "")
    search["inicio"] = request.args.get("ano-inicio", type=int)
    search["fim"] = request.args.get("ano-fim",type=int)
    search["estado"] = request.args.get("estado", "")
    search["page"] = request.args.get("page", type=int)

    sqlformat = {}
    if search["table"] == "Unions":
        sqlformat["table"] = [1,2,3,4] 
    elif search["table"] == "Employees":
        sqlformat["table"] = [5,6,7,8]
    else:
        sqlformat["table"] = [1,2,3,4,5,6,7,8]
    sqlformat["table"] = f"{','.join(str(x) for x in sqlformat['table'])}"
    sqlformat["term"] = "%{}%".format(search["term"].upper())
    sqlformat["distrito"]  = "%{}%".format(search["distrito"])
    sqlformat["setor"] = "%{}%".format(search["setor"])
    sqlformat["inicio"] = "{}-01-01".format(search["inicio"] or "0000")
    sqlformat["fim"] = "{}-12-31".format(search["fim"] or date.today().year)
    sqlformat["estado"] = "%{}%".format(search["estado"])
    sqlformat["offset"] = (search["page"] or 0) * ROWS_PER_PAGE
    sqlformat["rows_per_page"] = ROWS_PER_PAGE

    results = []

    connection = sqlite3.connect(DATABASE_NAME)
    connection.create_function("act2str", 1, lambda x: "Activa" if x else "Extinta")
    cursor = connection.execute(f"""
        SELECT Tipo, nomeEntidade, ifnull(sigla, ""), distritoDescricao, estadoEntidade, codEntG, codEntE, numAlt
        FROM Organizacoes
        WHERE 
            (nomeEntidade LIKE :term OR ifnull(sigla,"") LIKE :term OR (codEntG || "." || codEntE) LIKE :term OR (codEntG || "." || codEntE || "." || numAlt) LIKE :term)
        AND ifnull(distritoDescricao,"") LIKE :distrito
        AND dataBteConstituicao >= :inicio
        AND ifnull(dataBteExtincao, "0000-01-01") <= :fim
        AND codEntG in ({sqlformat["table"]})
        AND estadoEntidade LIKE :estado
        LIMIT :rows_per_page OFFSET :offset
    """, sqlformat)
    for row in cursor.fetchall():
        results.append(list(row))

    return render_template("dashboard-search.html", results=results, search=search, last_page=len(results) < ROWS_PER_PAGE, date=date)

@app.route("/export/")
def export():
    search = {}
    search["table"] = request.args.get('table_org', "")
    search["term"] = request.args.get('organization', "")
    search["distrito"]  = request.args.get('distrito', "")
    search["setor"] = request.args.get('setor', "")
    search["inicio"] = request.args.get("ano-inicio", type=int)
    search["fim"] = request.args.get("ano-fim",type=int)

    sqlformat = {}
    sqlformat["table"] = search["table"]
    sqlformat["term"] = "%{}%".format(search["term"].upper())
    sqlformat["distrito"]  = "%{}%".format(search["distrito"])
    sqlformat["setor"] = "%{}%".format(search["setor"])
    sqlformat["inicio"] = "{}-01-01".format(search["inicio"] or "0000")
    sqlformat["fim"] = "{}-12-31".format(search["fim"] or date.today().year)

    connection = sqlite3.connect(DATABASE_NAME)
    connection.create_function("act2str", 1, lambda x: "Activa" if x else "Extinta")
    
    strIO = BytesIO()
    excel_writer = pd.ExcelWriter(strIO, engine="xlsxwriter")

    IDS = []

    col_names = ["Código Identificador da Organização" , "Tipo de Organização" , "Denominação da Organização", "Acrónimo", "Concelho da Sede", "Distrito da Sede", "Data da Primeira Atividade Registada", "Data da Última Atividade Registada", "Ativa ou Extinta"]
    rows = connection.execute("""
        SELECT ID, Tipo, Nome, ifnull(Acronimo,""), Concelho_Sede, ifnull(Distrito_Sede,""), Data_Primeira_Actividade, Data_Ultima_Actividade, act2str(Activa)
        FROM Org_Sindical
        WHERE (Nome LIKE :term OR ifnull(Acronimo,"") LIKE :term OR ID LIKE :term)
        AND ifnull(Distrito_Sede,"") LIKE :distrito
        AND ifnull(Sector, "") LIKE :setor
        AND ifnull(Data_Primeira_Actividade, "0000-01-01") >= :inicio
        AND ifnull(Data_Ultima_Actividade, "0000-01-01") <= :fim
        AND :table NOT LIKE "Employees"
    """, sqlformat).fetchall()
    pd.DataFrame(list(rows), columns=col_names).to_excel(excel_writer, sheet_name="Organizações Sindicais", index=False)
    IDS.extend(list(map(lambda x: x[0], rows)))

    rows = connection.execute("""
        SELECT ID, Tipo, Nome, ifnull(Acronimo,""), Concelho_Sede, ifnull(Distrito_Sede,""), Data_Primeira_Actividade, Data_Ultima_Actividade, act2str(Activa)
        FROM Org_Patronal
        WHERE (Nome LIKE :term OR ifnull(Acronimo,"") LIKE :term OR ID LIKE :term)
        AND ifnull(Distrito_Sede,"") LIKE :distrito
        AND ifnull(Sector, "") LIKE :setor
        AND ifnull(Data_Primeira_Actividade, "0000-01-01") >= :inicio
        AND ifnull(Data_Ultima_Actividade, "0000-01-01") <= :fim
        AND :table NOT LIKE "Unions"
    """, sqlformat).fetchall()
    pd.DataFrame(list(rows), columns=col_names).to_excel(excel_writer, sheet_name="Organizações de Empregadores", index=False)
    IDS.extend(list(map(lambda x: x[0], rows)))
    
    col_names = [ "Código Identificador da Organização", "Denominação da Organização", "Identificador do Acto de Negociação", "Nome Acto", "Tipo Acto", "Natureza", "Ano", "Numero", "Série", "URL pata BTE", "Âmbito Geográfico" ]
    rows = connection.execute("""
            SELECT DISTINCT ID_Organizacao_Sindical, Org_Sindical.Nome, Actos_Negociacao_Colectiva.ID, Nome_Acto, Tipo_Acto, Natureza, Actos_Negociacao_Colectiva.Ano, Actos_Negociacao_Colectiva.Numero, Actos_Negociacao_Colectiva.Serie, Actos_Negociacao_Colectiva.URL, Actos_Negociacao_Colectiva.Ambito_Geografico
                       FROM Actos_Negociacao_Colectiva
               NATURAL JOIN Outorgantes_Actos, Org_Sindical
                      WHERE Org_Sindical.ID=ID_Organizacao_Sindical
                        AND ID_Organizacao_Sindical IS NOT NULL 
            UNION
            SELECT DISTINCT ID_Organizacao_Patronal, Org_Patronal.Nome, Actos_Negociacao_Colectiva.ID, Nome_Acto, Tipo_Acto, Natureza, Actos_Negociacao_Colectiva.Ano, Actos_Negociacao_Colectiva.Numero, Actos_Negociacao_Colectiva.Serie, Actos_Negociacao_Colectiva.URL, Actos_Negociacao_Colectiva.Ambito_Geografico
                       FROM Actos_Negociacao_Colectiva
               NATURAL JOIN Outorgantes_Actos, Org_Patronal
                      WHERE Org_Patronal.ID=ID_Organizacao_Patronal
                        AND ID_Organizacao_Patronal IS NOT NULL 
    """, sqlformat).fetchall()
    pd.DataFrame(filter(lambda x: x[0] in IDS, list(rows)), columns=col_names).to_excel(excel_writer, sheet_name="Negociação coletiva", index=False)

    col_names = ["_id_greve", "Código Identificador da Organização", "Denominação da Organização", "Ano de Início", "Mês de Início", "Ano de Fim", "Mês de Fim", "CAE"]
    rows = connection.execute("""
            SELECT Avisos_Greve_New.ID_Aviso_Greve, Org_Sindical.ID, Org_Sindical.Nome, Ano_Inicio, Mes_Inicio, Ano_Fim, Mes_Fim, CAE
              FROM Avisos_Greve_New
              JOIN Avisos_Greve_Participante_Sindical
              JOIN Org_Sindical
             WHERE Avisos_Greve_Participante_Sindical.Id_Entidade_Sindical = Org_Sindical.ID
               AND Avisos_Greve_Participante_Sindical.Id_Aviso_Greve = Avisos_Greve_New.ID_Aviso_Greve
             UNION
            SELECT Avisos_Greve_New.ID_Aviso_Greve, Org_Patronal.ID, Org_Patronal.Nome, Ano_Inicio, Mes_Inicio, Ano_Fim, Mes_Fim, CAE
              FROM Avisos_Greve_New
              JOIN Avisos_Greve_Participante_Patronal
              JOIN Org_Patronal
             WHERE Avisos_Greve_Participante_Patronal.Id_Entidade_Patronal = Org_Patronal.ID
               AND Avisos_Greve_Participante_Patronal.Id_Aviso_Greve = Avisos_Greve_New.ID_Aviso_Greve
          ORDER BY Avisos_Greve_New.ID_Aviso_Greve
    """, sqlformat).fetchall()
    pd.DataFrame(filter(lambda x: x[1] in IDS, list(rows)), columns=col_names).to_excel(excel_writer, sheet_name="Pré avisos de greve", index=False)

    col_names = ["Código Identificador da Organização", "Denominação da Organização", "Ano", "Número", "Série", "URL para BTE"]
    rows = connection.execute("""
            SELECT ID_Organizacao_Sindical, Org_Sindical.Nome, Ano, Numero, Serie, URL
              FROM Mencoes_BTE_Org_Sindical, Org_Sindical
             WHERE Mencoes_BTE_Org_Sindical.ID_Organizacao_Sindical = Org_Sindical.ID 
               AND Mudanca_Estatuto = TRUE
             UNION
            SELECT ID_Organizacao_Patronal, Org_Patronal.Nome, Ano, Numero, Serie, URL
              FROM Mencoes_BTE_Org_Patronal, Org_Patronal
             WHERE Mencoes_BTE_Org_Patronal.ID_Organizacao_Patronal = Org_Patronal.ID 
               AND Mudanca_Estatuto = TRUE
    """, sqlformat).fetchall()
    pd.DataFrame(filter(lambda x: x[0] in IDS, list(rows)), columns=col_names).to_excel(excel_writer, sheet_name="Estatutos", index=False)

    col_names = ["Código Identificador da Organização", "Denominação da Organização", "Ano", "Número", "Série", "URL para BTE"]
    rows = connection.execute("""
            SELECT ID_Organizacao_Sindical, Org_Sindical.Nome, Ano, Numero, Serie, URL
              FROM Mencoes_BTE_Org_Sindical, Org_Sindical
             WHERE Mencoes_BTE_Org_Sindical.ID_Organizacao_Sindical = Org_Sindical.ID 
               AND Eleicoes = TRUE
             UNION
            SELECT ID_Organizacao_Patronal, Org_Patronal.Nome, Ano, Numero, Serie, URL
              FROM Mencoes_BTE_Org_Patronal, Org_Patronal
             WHERE Mencoes_BTE_Org_Patronal.ID_Organizacao_Patronal = Org_Patronal.ID 
               AND Eleicoes = TRUE
    """, sqlformat).fetchall()
    pd.DataFrame(filter(lambda x: x[0] in IDS, list(rows)), columns=col_names).to_excel(excel_writer, sheet_name="Eleições", index=False)
    excel_writer.save()
    strIO.seek(0)
    return send_file(strIO,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            download_name='data-export_%s.xlsx' % datetime.now().strftime("%Y-%m-%d-%H_%M_%S"),
            as_attachment=True)

@app.route("/orgs_by_year", methods=['GET'])
def orgs_by_year():
    connection = sqlite3.connect(DATABASE_NAME)
    selectstm = """SELECT Tipo, dataBteConstituicao, dataBteExtincao FROM Organizacoes"""
    rows = connection.execute(selectstm).fetchall()
    results = []
    for ano in range(1996, date.today().year + 1):
    
        curr_obj = {
            'ano': ano,
            'CONFEDERAÇÃO SINDICAL': 0,
            'FEDERAÇÃO SINDICAL': 0,
            'SINDICATO': 0,
            'UNIÃO SINDICAL': 0,
            'CONFEDERAÇÃO DE EMPREGADORES': 0,
            'FEDERAÇÃO DE EMPREGADORES': 0,
            'ASSOCIAÇÃO DE EMPREGADORES': 0,
            'UNIÃO DE EMPREGADORES': 0
        }
    
        for entidade in rows:
            start = date.fromisoformat(entidade[1]).year
            if entidade[2] is not None:
                end = date.fromisoformat(entidade[2]).year
            else:
                end = date.today().year+1
            if start <= ano < end:
                curr_obj[entidade[0]] += 1
        results.append(curr_obj)
    return jsonify(results)

@app.route("/<path:path>", methods=['GET'])
def static_file(path):
    return send_from_directory('static', path)

@click.command()
@click.option('--host', help='webapp server host.', show_default=True, default="127.0.0.1")
@click.option('--port', help='webapp server port.', show_default=True, default="8080")
@click.option('--sqlite-host', help='sqlite_web server host.', show_default=True, default="127.0.0.1")
@click.option('--sqlite-port', help='sqlite_web server port.', show_default=True, default="8090")
def main(host="127.0.0.1", port="8080", sqlite_host="127.0.0.1", sqlite_port="8090"):
    if not os.path.isfile(DATABASE_NAME):
        # Create the database
        print("Creating database for the first time. This operation may take a while...")
        try:
            rep_database.repDatabase()
            print("Database created successfully.")
        except Exception as e:
            print("Error creating database:\n ", e)
            os.remove(DATABASE_NAME)
            sys.exit(1)
    
    threading.Thread(target=run_sqlite_web, args=(sqlite_host, sqlite_port)).start()
    # update database in background every day
    threading.Timer(86400, regular_update).start()
    app.run(host=host, port=port)


if __name__ == "__main__":
    main()

