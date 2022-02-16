from distutils.file_util import write_file
import sqlite3
from flask import render_template
from pathlib import Path

from jinja2 import Template

data_path = Path("static/data/")
entidades_path = (data_path/"entidades")
templates_path = Path("templates")
template_entidade = Template((templates_path/"entidade.html").open().read())

def subtables(id, colnames, sqlstr, conn):
    return [{colnames[i]: col for i, col in enumerate(row)} for row in conn.execute(sqlstr, {'id': id}).fetchall()]

def archive_entity(entidade, tipo_entidade, conn):
    file = entidades_path/f"{entidade['id']}.html"

    atos_negociacao = subtables(entidade['id'], [ "Identificador do Acto de Negociação", "Nome Acto", "Tipo Acto", "Natureza", "CAE", "Ano", "Âmbito Geográfico", "Numero", "Série", "URL"], """
            SELECT DISTINCT Actos_Negociacao_Colectiva.ID, Nome_Acto, Tipo_Acto, Natureza, CAE, Actos_Negociacao_Colectiva.Ano, Actos_Negociacao_Colectiva.Ambito_Geografico, Numero, Serie, URL
                       FROM Actos_Negociacao_Colectiva
               NATURAL JOIN Outorgantes_Actos, Org_Sindical
                      WHERE Org_Sindical.ID=ID_Organizacao_Sindical
                        AND ID_Organizacao_Sindical LIKE :id
            UNION
            SELECT DISTINCT Actos_Negociacao_Colectiva.ID, Nome_Acto, Tipo_Acto, Natureza, CAE, Actos_Negociacao_Colectiva.Ano, Actos_Negociacao_Colectiva.Ambito_Geografico, Numero, Serie, URL
                       FROM Actos_Negociacao_Colectiva
               NATURAL JOIN Outorgantes_Actos, Org_Patronal
                      WHERE Org_Patronal.ID=ID_Organizacao_Patronal
                        AND ID_Organizacao_Patronal LIKE :id 
    """, conn)

    avisos_greve = subtables(entidade['id'], ["_id_greve", "Ano de Início", "Mês de Início", "Ano de Fim", "Mês de Fim", "CAE"], """
        SELECT Avisos_Greve_New.ID_Aviso_Greve, Ano_Inicio, Mes_Inicio, Ano_Fim, Mes_Fim, CAE
              FROM Avisos_Greve_New
              JOIN Avisos_Greve_Participante_Sindical
              WHERE Avisos_Greve_Participante_Sindical.Id_Aviso_Greve = Avisos_Greve_New.ID_Aviso_Greve AND Avisos_Greve_Participante_Sindical.Id_Entidade_Sindical LIKE :id
            UNION
        SELECT Avisos_Greve_New.ID_Aviso_Greve, Ano_Inicio, Mes_Inicio, Ano_Fim, Mes_Fim, CAE
              FROM Avisos_Greve_New
              JOIN Avisos_Greve_Participante_Patronal
             WHERE Avisos_Greve_Participante_Patronal.Id_Aviso_Greve = Avisos_Greve_New.ID_Aviso_Greve AND Avisos_Greve_Participante_Patronal.Id_Entidade_Patronal LIKE :id
          ORDER BY Avisos_Greve_New.ID_Aviso_Greve
    """, conn)

    mudacas_estatuto = subtables(entidade['id'], ["Ano", "Número", "Série", "URL"], """
            SELECT Ano, Numero, Serie, URL
              FROM Mencoes_BTE_Org_Sindical
             WHERE Mencoes_BTE_Org_Sindical.ID_Organizacao_Sindical LIKE :id
               AND Mudanca_Estatuto = TRUE
             UNION
            SELECT Ano, Numero, Serie, URL
              FROM Mencoes_BTE_Org_Patronal
             WHERE Mencoes_BTE_Org_Patronal.ID_Organizacao_Patronal Like :id 
               AND Mudanca_Estatuto = TRUE
    """, conn)

    eleicoes = subtables(entidade['id'], ["Ano", "Número", "Série","URL"], """
            SELECT Ano, Numero, Serie, URL
              FROM Mencoes_BTE_Org_Sindical
             WHERE Mencoes_BTE_Org_Sindical.ID_Organizacao_Sindical Like :id
               AND Eleicoes = TRUE
             UNION
            SELECT Ano, Numero, Serie, URL
              FROM Mencoes_BTE_Org_Patronal
             WHERE Mencoes_BTE_Org_Patronal.ID_Organizacao_Patronal Like :id
               AND Eleicoes = TRUE
    """, conn)

    render = template_entidade.render(tipo_entidade=tipo_entidade, entidade=entidade, atos_negociacao=atos_negociacao, avisos_greve=avisos_greve, mudacas_estatuto=mudacas_estatuto, eleicoes=eleicoes)
    file.write_text(render)

def main():
    conn = sqlite3.connect('rep-database.db')
    conn.create_function("act2str", 1, lambda x: "Activa" if x else "Extinta")

    entidades_path.mkdir(parents=True, exist_ok=True)
    distritos = {}
    sorted_nomes = []
    colname = ['id', 'nome', 'distrito_sede', 'activa', 'acronimo', 'data_inicio', 'data_fim', 'website']
    for org_sindical in conn.execute('SELECT ID, NOME, DISTRITO_SEDE, act2str(ACTIVA) as ACTIVA, Acronimo, Data_Primeira_Actividade, Data_Ultima_Actividade, website FROM Org_Sindical').fetchall():
        if org_sindical[2] not in distritos:
            distritos[org_sindical[2]] = []
        distritos[org_sindical[2]].append({'id':org_sindical[0], 'nome':org_sindical[1]})
        sorted_nomes.append({'id':org_sindical[0], 'nome':org_sindical[1]})
        archive_entity({col: org_sindical[i] or "" for i, col in enumerate(colname)},'sindical', conn)
    for org_patronal in conn.execute('SELECT ID, NOME, DISTRITO_SEDE, act2str(ACTIVA) as ACTIVA, Acronimo, Data_Primeira_Actividade, Data_Ultima_Actividade, website FROM Org_Patronal').fetchall():
        if org_patronal[2] not in distritos:
            distritos[org_patronal[2]] = []
        distritos[org_patronal[2]].append({'id':org_patronal[0], 'nome':org_patronal[1]})
        sorted_nomes.append({'id':org_patronal[0], 'nome':org_patronal[1]})
        archive_entity({col: org_patronal[i] or "" for i, col in enumerate(colname)},'patronal', conn)
    
    file = entidades_path/"bydistrito.html"
    template_distrito = Template((templates_path/"bydistrito.html").open().read())
    render = template_distrito.render(distritos=distritos)
    file.write_text(render)

    sorted_nomes.sort(key=lambda x: x['nome'])
    for x in sorted_nomes:
        if x["id"] == "4.9.2":
            print(x)
    file = entidades_path/"bynome.html"
    template_distrito = Template((templates_path/"bynome.html").open().read())
    render = template_distrito.render(sorted_nomes=sorted_nomes)
    file.write_text(render)
        

if __name__ == '__main__':
    main()