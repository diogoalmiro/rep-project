import os
import sqlite3
import subprocess
import sys
import threading
from datetime import date, datetime
from io import BytesIO
import click
import unicodedata

import pandas as pd
from flask import Flask, jsonify, render_template, request, send_file, send_from_directory, redirect

import rep_database

import time

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

ROWS_PER_PAGE = 50

# https://stackoverflow.com/questions/517923/what-is-the-best-way-to-remove-accents-normalize-in-a-python-unicode-string#comment62045428_518232
def normalize_term(str):
    return unicodedata.normalize('NFD',str).encode('ASCII','ignore').decode().upper()

@app.route("/dashboard-search/")
def server_side_search():
    search = {}
    search["table"] = request.args.get('table_org', "")
    search["term"] = request.args.get('organization', "")
    search["distrito"]  = request.args.get('distrito', "")
    search["setor"] = request.args.get('setor', "")
    search["inicio"] = request.args.get("ano-inicio", type=int)
    search["fim"] = request.args.get("ano-fim",type=int)
    search["page"] = request.args.get("page", type=int)

    sqlformat = {}
    sqlformat["table"] = search["table"]
    sqlformat["term"] = "%{}%".format(search["term"].upper())
    sqlformat["norm_term"] = "%{}%".format(normalize_term(search["term"]))
    sqlformat["distrito"]  = "%{}%".format(search["distrito"])
    sqlformat["setor"] = "%{}%".format(search["setor"])
    sqlformat["inicio"] = "{}-01-01".format(search["inicio"] or "0000")
    sqlformat["fim"] = "{}-12-31".format(search["fim"] or date.today().year)
    sqlformat["offset"] = (search["page"] or 0) * ROWS_PER_PAGE
    sqlformat["rows_per_page"] = ROWS_PER_PAGE

    results = []

    connection = sqlite3.connect(DATABASE_NAME)
    connection.create_function("act2str", 1, lambda x: "Activa" if x else "Extinta")
    connection.create_function("normalize", 1, normalize_term)
    cursor = connection.execute("""
        SELECT Tipo, Nome, ifnull(Acronimo, ""), Distrito_Sede, act2str(Activa)
        FROM Org_Patronal
        WHERE (Nome LIKE :term OR ifnull(Acronimo,"") LIKE :term OR ID LIKE :term OR normalize(Nome) LIKE :norm_term)
        AND ifnull(Distrito_Sede,"") LIKE :distrito
        AND ifnull(Sector,"") LIKE :setor
        AND ifnull(Data_Primeira_Actividade, "0000-01-01") >= :inicio
        AND ifnull(Data_Ultima_Actividade,"0000-01-01") <= :fim
        AND :table NOT LIKE "Unions"
        UNION
        SELECT Tipo, Nome, ifnull(Acronimo, ""), Distrito_Sede, act2str(Activa)
        FROM Org_Sindical
        WHERE (Nome LIKE :term OR ifnull(Acronimo,"") LIKE :term OR ID LIKE :term OR normalize(Nome) LIKE :norm_term)
        AND ifnull(Distrito_Sede,"") LIKE :distrito
        AND ifnull(Sector,"") LIKE :setor
        AND ifnull(Data_Primeira_Actividade, "0000-01-01") >= :inicio
        AND ifnull(Data_Ultima_Actividade,"0000-01-01") <= :fim
        AND :table NOT LIKE "Employees"
        LIMIT :rows_per_page OFFSET :offset
    """, sqlformat)
    for row in cursor.fetchall():
        results.append(list(row))

    return render_template("dashboard-search.html", results=results, search=search, last_page=len(results) < ROWS_PER_PAGE, date=date)

def id_value(id: str):
    g, e, a = id.split(".")
    return int(g) * 1000000 + int(e) * 1000 + int(a)

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
    sqlformat["norm_term"] = "%{}%".format(normalize_term(search["term"]))
    sqlformat["distrito"]  = "%{}%".format(search["distrito"])
    sqlformat["setor"] = "%{}%".format(search["setor"])
    sqlformat["inicio"] = "{}-01-01".format(search["inicio"] or "0000")
    sqlformat["fim"] = "{}-12-31".format(search["fim"] or date.today().year)

    connection = sqlite3.connect(DATABASE_NAME)
    connection.create_function("act2str", 1, lambda x: "Activa" if x else "Extinta")
    connection.create_function("normalize", 1, normalize_term)
    
    strIO = BytesIO()
    excel_writer = pd.ExcelWriter(strIO, engine="xlsxwriter")

    IDS = set()
    start = time.monotonic()
    ents = connection.execute("""
        SELECT ID
        FROM Org_Patronal
        WHERE (Nome LIKE :term OR ifnull(Acronimo,"") LIKE :term OR ID LIKE :term OR normalize(Nome) LIKE :norm_term)
        AND ifnull(Distrito_Sede,"") LIKE :distrito
        AND ifnull(Sector,"") LIKE :setor
        AND ifnull(Data_Primeira_Actividade, "0000-01-01") >= :inicio
        AND ifnull(Data_Ultima_Actividade,"0000-01-01") <= :fim
        AND :table NOT LIKE "Unions"
        UNION
        SELECT ID
        FROM Org_Sindical
        WHERE (Nome LIKE :term OR ifnull(Acronimo,"") LIKE :term OR ID LIKE :term OR normalize(Nome) LIKE :norm_term)
        AND ifnull(Distrito_Sede,"") LIKE :distrito
        AND ifnull(Sector,"") LIKE :setor
        AND ifnull(Data_Primeira_Actividade, "0000-01-01") >= :inicio
        AND ifnull(Data_Ultima_Actividade,"0000-01-01") <= :fim
        AND :table NOT LIKE "Employees"
    """, sqlformat).fetchall()
    for (id,) in ents:
        codEntG, codEntE, numAlt = id.split(".")
        strID = f"{codEntG}.{codEntE}._%"
        for (sid,) in connection.execute("""SELECT ID FROM Org_Patronal WHERE ID LIKE :id """, {"id": strID}).fetchall():
            IDS.add(sid)
        for (sid,) in connection.execute("""SELECT ID FROM Org_Sindical WHERE ID LIKE :id """, {"id": strID}).fetchall():
            IDS.add(sid)
    print(time.monotonic() - start, "Fetched IDs")
    pd_sind = pd.DataFrame(columns=["Código Identificador da Organização", "Tipo de Organização" , "Denominação da Organização", "Acrónimo", "Concelho da Sede", "Distrito da Sede", "Data da Primeira Atividade Registada", "Data da Última Atividade Registada", "Ativa ou Extinta"])
    pd_patr = pd.DataFrame(columns=["Código Identificador da Organização", "Tipo de Organização" , "Denominação da Organização", "Acrónimo", "Concelho da Sede", "Distrito da Sede", "Data da Primeira Atividade Registada", "Data da Última Atividade Registada", "Ativa ou Extinta"])
    pd_negc = pd.DataFrame(columns=["Código Identificador da Organização", "Denominação da Organização", "Identificador do Acto de Negociação", "Nome Acto", "Tipo Acto", "Natureza", "Ano", "Numero", "Série", "URL pata BTE", "Âmbito Geográfico" ])
    pd_padg = pd.DataFrame(columns=["_id_greve", "Código Identificador da Organização", "Denominação da Organização", "Ano de Início", "Mês de Início", "Ano de Fim", "Mês de Fim", "CAE"])
    pd_esta = pd.DataFrame(columns=["Código Identificador da Organização", "Denominação da Organização", "Ano", "Número", "Série", "URL para BTE"])
    pd_elei = pd.DataFrame(columns=["Código Identificador da Organização", "Denominação da Organização", "Ano", "Número", "Série", "URL para BTE"])
    time_fetch_sind_info = 0
    time_fetch_patr_info = 0
    time_fetch_negc_info = 0
    time_fetch_padg_info = 0
    time_fetch_esta_info = 0
    time_fetch_elei_info = 0

    start = time.monotonic()
    sind = connection.execute("""SELECT ID, Tipo, Nome, ifnull(Acronimo,""), Concelho_Sede, ifnull(Distrito_Sede,""), Data_Primeira_Actividade, Data_Ultima_Actividade, act2str(Activa) FROM Org_Sindical""").fetchall()
    pd_sind.from_records(filter(lambda x: x[0] in IDS, sind), columns=pd_sind.columns).to_excel(excel_writer, sheet_name="Organizações sindicais", index=False)
    time_fetch_sind_info += time.monotonic() - start
    start = time.monotonic()
    patr = connection.execute("""SELECT ID, Tipo, Nome, ifnull(Acronimo,""), Concelho_Sede, ifnull(Distrito_Sede,""), Data_Primeira_Actividade, Data_Ultima_Actividade, act2str(Activa) FROM Org_Patronal""").fetchall()
    pd_patr.from_records(filter(lambda x: x[0] in IDS, patr), columns=pd_patr.columns).to_excel(excel_writer, sheet_name="Organizações de empregadores", index=False)
    time_fetch_patr_info += time.monotonic() - start
    start = time.monotonic()
    negc = connection.execute("""
        SELECT DISTINCT ID_Organizacao_Sindical as ID, Org_Sindical.Nome, Actos_Negociacao_Colectiva.ID, Nome_Acto, Tipo_Acto, Natureza, Actos_Negociacao_Colectiva.Ano, Actos_Negociacao_Colectiva.Numero, Actos_Negociacao_Colectiva.Serie, Actos_Negociacao_Colectiva.URL, Actos_Negociacao_Colectiva.Ambito_Geografico
                       FROM Actos_Negociacao_Colectiva
               NATURAL JOIN Outorgantes_Actos, Org_Sindical
                      WHERE Org_Sindical.ID=ID_Organizacao_Sindical
                        AND ID_Organizacao_Sindical IS NOT NULL 
            UNION
            SELECT DISTINCT ID_Organizacao_Patronal as ID, Org_Patronal.Nome, Actos_Negociacao_Colectiva.ID, Nome_Acto, Tipo_Acto, Natureza, Actos_Negociacao_Colectiva.Ano, Actos_Negociacao_Colectiva.Numero, Actos_Negociacao_Colectiva.Serie, Actos_Negociacao_Colectiva.URL, Actos_Negociacao_Colectiva.Ambito_Geografico
                       FROM Actos_Negociacao_Colectiva
               NATURAL JOIN Outorgantes_Actos, Org_Patronal
                      WHERE Org_Patronal.ID=ID_Organizacao_Patronal
                        AND ID_Organizacao_Patronal IS NOT NULL
                        ORDER BY ID ASC, Actos_Negociacao_Colectiva.Ano ASC, Actos_Negociacao_Colectiva.Numero ASC""").fetchall()
    pd_negc.from_records(filter(lambda x: x[0] in IDS, negc), columns=pd_negc.columns).to_excel(excel_writer, sheet_name="Negociação coletiva", index=False)
    time_fetch_negc_info += time.monotonic() - start
    start = time.monotonic()
    padg = connection.execute("""
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
        """).fetchall()
    pd_padg.from_records(filter(lambda x: x[1] in IDS, padg), columns=pd_padg.columns).to_excel(excel_writer, sheet_name="Pré avisos de greve", index=False)
    time_fetch_padg_info += time.monotonic() - start
    start = time.monotonic()
    esta = connection.execute("""
             SELECT ID_Organizacao_Sindical as ID, Org_Sindical.Nome, Ano, Numero, Serie, URL
              FROM Mencoes_BTE_Org_Sindical, Org_Sindical
             WHERE Mencoes_BTE_Org_Sindical.ID_Organizacao_Sindical = Org_Sindical.ID 
               AND Mudanca_Estatuto = TRUE
             UNION
            SELECT ID_Organizacao_Patronal as ID, Org_Patronal.Nome, Ano, Numero, Serie, URL
              FROM Mencoes_BTE_Org_Patronal, Org_Patronal
             WHERE Mencoes_BTE_Org_Patronal.ID_Organizacao_Patronal = Org_Patronal.ID 
               AND Mudanca_Estatuto = TRUE
          ORDER BY ID ASC, Ano ASC, Numero ASC
        """).fetchall()
    pd_esta.from_records(filter(lambda x: x[0] in IDS, esta), columns=pd_esta.columns).to_excel(excel_writer, sheet_name="Estatutos", index=False)
    time_fetch_esta_info += time.monotonic() - start
    start = time.monotonic()
    elei = connection.execute("""
            SELECT ID_Organizacao_Sindical as ID, Org_Sindical.Nome, Ano, Numero, Serie, URL
              FROM Mencoes_BTE_Org_Sindical, Org_Sindical
             WHERE Mencoes_BTE_Org_Sindical.ID_Organizacao_Sindical = Org_Sindical.ID 
               AND Eleicoes = TRUE
             UNION
            SELECT ID_Organizacao_Patronal as ID, Org_Patronal.Nome, Ano, Numero, Serie, URL
              FROM Mencoes_BTE_Org_Patronal, Org_Patronal
             WHERE Mencoes_BTE_Org_Patronal.ID_Organizacao_Patronal = Org_Patronal.ID 
               AND Eleicoes = TRUE
            ORDER BY ID ASC, Ano ASC, Numero ASC
        """).fetchall()
    pd_elei.from_records(filter(lambda x: x[0] in IDS, elei), columns=pd_elei.columns).to_excel(excel_writer, sheet_name="Eleições", index=False)
    time_fetch_elei_info += time.monotonic() - start
    
    print("Fetched sindical info in {:.2f}s".format(time_fetch_sind_info))
    print("Fetched patronal info in {:.2f}s".format(time_fetch_patr_info))
    print("Fetched negociacao colectiva info in {:.2f}s".format(time_fetch_negc_info))
    print("Fetched avisos greve info in {:.2f}s".format(time_fetch_padg_info))
    print("Fetched estatuto info in {:.2f}s".format(time_fetch_esta_info))
    print("Fetched eleicoes info in {:.2f}s".format(time_fetch_elei_info))

    excel_writer.sheets["Organizações sindicais"].set_column(0, pd_sind.shape[1]-1, 15)
    excel_writer.sheets["Organizações sindicais"].autofilter(0, 0, pd_sind.shape[0], pd_sind.shape[1]-1)
    
    excel_writer.sheets["Organizações de empregadores"].set_column(0, pd_patr.shape[1]-1, 15)
    excel_writer.sheets["Organizações de empregadores"].autofilter(0, 0, pd_patr.shape[0], pd_patr.shape[1]-1)
    
    excel_writer.sheets["Negociação coletiva"].set_column(0, pd_negc.shape[1]-1, 15)
    excel_writer.sheets["Negociação coletiva"].autofilter(0, 0, pd_negc.shape[0], pd_negc.shape[1]-1)

    excel_writer.sheets["Pré avisos de greve"].set_column(0, pd_padg.shape[1]-1, 15)
    excel_writer.sheets["Pré avisos de greve"].autofilter(0, 0, pd_padg.shape[0], pd_padg.shape[1]-1)
    
    excel_writer.sheets["Estatutos"].set_column(0, pd_esta.shape[1]-1, 15)
    excel_writer.sheets["Estatutos"].autofilter(0, 0, pd_esta.shape[0], pd_esta.shape[1]-1)

    excel_writer.sheets["Eleições"].set_column(0, pd_elei.shape[1]-1, 15)
    excel_writer.sheets["Eleições"].autofilter(0, 0, pd_elei.shape[0], pd_elei.shape[1]-1)

    excel_writer.save()
    strIO.seek(0)
    return send_file(strIO,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            download_name='data-export_%s.xlsx' % datetime.now().strftime("%Y-%m-%d-%H_%M_%S"),
            as_attachment=True)

@app.route("/orgs_by_year", methods=['GET'])
def org_by_year():
    results = []
    seen = set()
    tmp_entidades = rep_database.json.loads(rep_database.getResponse("entidades", 0)).values()
    entidades = []
    for entidade in tmp_entidades:
        id = f"{entidade['codEntG']}.{entidade['codEntE']}"
        if id not in seen:
            entidades.append(entidade)
            seen.add(id)
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
        for entidade in entidades:
            start = date.fromisoformat(entidade['dataBteConstituicao']).year
            if 'dataBteExtincao' in entidade:
                end = date.fromisoformat(entidade['dataBteExtincao']).year
            else:
                end = date.today().year+1
            if start <= ano < end:
                if entidade['codEntG'] == 1:
                    curr_obj['SINDICATO'] += 1
                elif entidade['codEntG'] == 2:
                    curr_obj['FEDERAÇÃO SINDICAL'] += 1
                elif entidade['codEntG'] == 3:
                    curr_obj['UNIÃO SINDICAL'] += 1
                elif entidade['codEntG'] == 4:
                    curr_obj['CONFEDERAÇÃO SINDICAL'] += 1
                if entidade['codEntG'] == 5:
                    curr_obj['ASSOCIAÇÃO DE EMPREGADORES'] += 1
                elif entidade['codEntG'] == 6:
                    curr_obj['FEDERAÇÃO DE EMPREGADORES'] += 1
                elif entidade['codEntG'] == 7:
                    curr_obj['UNIÃO DE EMPREGADORES'] += 1
                elif entidade['codEntG'] == 8:
                    curr_obj['CONFEDERAÇÃO DE EMPREGADORES'] += 1
        results.append(curr_obj)
    return jsonify(results)

@app.route("/<path:path>", methods=['GET'])
@app.route("/" , methods=['GET'])
def static_file(path='/'):
    if path[-1:] == '/':
        path += 'index.html'
    if path[0] == '/':
        path = path[1:]
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

