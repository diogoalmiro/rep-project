from distutils.file_util import write_file
from json import load
import sqlite3
from flask import render_template
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, Template

data_path = Path("static/data/")
entidades_path = (data_path/"entidades")
env = Environment(loader=FileSystemLoader("templates"))
template_entidade = env.get_template("entidade.html")

def subtables(id, colnames, sqlstr, conn):
    return [{colnames[i]: col for i, col in enumerate(row)} for row in conn.execute(sqlstr, {'id': id}).fetchall()]

def archive_entity(entidade, tipo_entidade, conn):
    file = entidades_path/f"{entidade['id']}.html"

    atos_negociacao = subtables(entidade['id'], [ "Identificador do Acto de Negociação", "Nome Acto", "Tipo Acto", "Natureza", "Ano", "Âmbito Geográfico", "Numero", "Série", "URL"], """
            SELECT DISTINCT Actos_Negociacao_Colectiva.ID, Nome_Acto, Tipo_Acto, Natureza, Actos_Negociacao_Colectiva.Ano, Actos_Negociacao_Colectiva.Ambito_Geografico, Numero, Serie, URL
                       FROM Actos_Negociacao_Colectiva
               NATURAL JOIN Outorgantes_Actos, Org_Sindical
                      WHERE Org_Sindical.ID=ID_Organizacao_Sindical
                        AND ID_Organizacao_Sindical LIKE :id
            UNION
            SELECT DISTINCT Actos_Negociacao_Colectiva.ID, Nome_Acto, Tipo_Acto, Natureza, Actos_Negociacao_Colectiva.Ano, Actos_Negociacao_Colectiva.Ambito_Geografico, Numero, Serie, URL
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

    template_entidade.stream(tipo_entidade=tipo_entidade, entidade=entidade, atos_negociacao=atos_negociacao, avisos_greve=avisos_greve, mudacas_estatuto=mudacas_estatuto, eleicoes=eleicoes).dump(file.open('w'))

def main():
    conn = sqlite3.connect('rep-database.db')
    entidades_path.mkdir(parents=True, exist_ok=True)
    distritos = {}
    sorted_nomes = []
    for entidade in conn.execute('SELECT * FROM Organizacoes'):
        tipo_entidade = int(entidade['id'][0]) < 5 ? 'Sindical' : 'Patronal'
        if entidade['distritoDescricao'] not in distritos:
            distritos[entidade['distritoDescricao']] = []
        distritos[entidade['distritoDescricao']].append(entidade)
        sorted_nomes.append(entidade)
        archive_entity(entidade, tipo_entidade, conn)
    
    file = entidades_path/"bydistrito.html"
    template_distrito = env.get_template("bydistrito.html")
    template_distrito.stream(distritos=distritos).dump(file.open('w'))

    sorted_nomes.sort(key=lambda x: x['nome'])
    file = entidades_path/"bynome.html"
    template_nomes = env.get_template("bynome.html")
    template_nomes.stream(sorted_nomes=sorted_nomes).dump(file.open('w'))
        

if __name__ == '__main__':
    main()