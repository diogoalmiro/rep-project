import sqlite3
import pandas as pd

class RepDataset:
    def __init__(self, filename): # x
        self.filename = filename

    @property
    def database(self): # x
        return sqlite3.connect(self.filename)

    def query(self, *args): # x
        return self.database.execute(*args)

    #tipos de organizações sindicais
    @property
    def barchart1_labels(self): # x
        cursor = self.query("SELECT ANO, COUNT(DISTINCT SUBSTR(ID,0,INSTR(ID,'.')+1) || SUBSTR(SUBSTR(ID,INSTR(ID,'.')+1,LENGTH(ID)) ,0,INSTR(SUBSTR(ID,INSTR(ID,'.')+1,LENGTH(ID)),'.'))) AS NUM_ORG FROM Org_Sindical, ( SELECT DISTINCT CAST(strftime('%Y',date(Data_Primeira_Actividade)) AS DECIMAL) AS Ano FROM Org_Sindical WHERE Data_Primeira_Actividade IS NOT NULL AND Ano >= 1977 UNION SELECT DISTINCT CAST(strftime('%Y',date(Data_Ultima_Actividade)) AS DECIMAL) AS Ano FROM Org_Sindical WHERE Data_Primeira_Actividade IS NOT NULL) AS ANOS WHERE CAST(strftime('%Y',date(Data_Primeira_Actividade)) AS DECIMAL) <= ANO AND (Activa = 1 OR CAST(strftime('%Y',date(Data_Ultima_Actividade)) AS DECIMAL) >= ANO) GROUP BY ANO HAVING ANO >= 1996")
        return [row[0] for row in cursor.fetchall()]

    #confederacoes sindicais
    @property
    def barchart1_data(self): # x
        cursor = self.query("SELECT ANO, TIPO, COUNT(DISTINCT SUBSTR(ID,0,INSTR(ID,'.')+1) || SUBSTR(SUBSTR(ID,INSTR(ID,'.')+1,LENGTH(ID)) ,0,INSTR(SUBSTR(ID,INSTR(ID,'.')+1,LENGTH(ID)),'.'))) AS NUM_ORG FROM Org_Sindical, ( SELECT DISTINCT CAST(strftime('%Y',date(Data_Primeira_Actividade)) AS DECIMAL) AS Ano FROM Org_Sindical WHERE Data_Primeira_Actividade IS NOT NULL AND Ano >= 1977 UNION SELECT DISTINCT CAST(strftime('%Y',date(Data_Ultima_Actividade)) AS DECIMAL) AS Ano FROM Org_Sindical WHERE Data_Primeira_Actividade IS NOT NULL) AS ANOS WHERE CAST(strftime('%Y',date(Data_Primeira_Actividade)) AS DECIMAL) <= ANO AND (Activa = 1 OR CAST(strftime('%Y',date(Data_Ultima_Actividade)) AS DECIMAL) >= ANO) GROUP BY ANO, TIPO HAVING ANO >= 1996")
        return [row[2] for row in cursor.fetchall() if row[1].startswith('CONF')]

    #federacoes sindicais
    @property
    def barchart1_data2(self): # x
        cursor = self.query("SELECT ANO, TIPO, COUNT(DISTINCT SUBSTR(ID,0,INSTR(ID,'.')+1) || SUBSTR(SUBSTR(ID,INSTR(ID,'.')+1,LENGTH(ID)) ,0,INSTR(SUBSTR(ID,INSTR(ID,'.')+1,LENGTH(ID)),'.'))) AS NUM_ORG FROM Org_Sindical, ( SELECT DISTINCT CAST(strftime('%Y',date(Data_Primeira_Actividade)) AS DECIMAL) AS Ano FROM Org_Sindical WHERE Data_Primeira_Actividade IS NOT NULL AND Ano >= 1977 UNION SELECT DISTINCT CAST(strftime('%Y',date(Data_Ultima_Actividade)) AS DECIMAL) AS Ano FROM Org_Sindical WHERE Data_Primeira_Actividade IS NOT NULL) AS ANOS WHERE CAST(strftime('%Y',date(Data_Primeira_Actividade)) AS DECIMAL) <= ANO AND (Activa = 1 OR CAST(strftime('%Y',date(Data_Ultima_Actividade)) AS DECIMAL) >= ANO) GROUP BY ANO, TIPO HAVING ANO >= 1996")
        return [row[2] for row in cursor.fetchall() if row[1].startswith('FED')]

    #sindicatos
    @property
    def barchart1_data3(self): # x
        cursor = self.query("SELECT ANO, TIPO, COUNT(DISTINCT SUBSTR(ID,0,INSTR(ID,'.')+1) || SUBSTR(SUBSTR(ID,INSTR(ID,'.')+1,LENGTH(ID)) ,0,INSTR(SUBSTR(ID,INSTR(ID,'.')+1,LENGTH(ID)),'.'))) AS NUM_ORG FROM Org_Sindical, ( SELECT DISTINCT CAST(strftime('%Y',date(Data_Primeira_Actividade)) AS DECIMAL) AS Ano FROM Org_Sindical WHERE Data_Primeira_Actividade IS NOT NULL AND Ano >= 1977 UNION SELECT DISTINCT CAST(strftime('%Y',date(Data_Ultima_Actividade)) AS DECIMAL) AS Ano FROM Org_Sindical WHERE Data_Primeira_Actividade IS NOT NULL) AS ANOS WHERE CAST(strftime('%Y',date(Data_Primeira_Actividade)) AS DECIMAL) <= ANO AND (Activa = 1 OR CAST(strftime('%Y',date(Data_Ultima_Actividade)) AS DECIMAL) >= ANO) GROUP BY ANO, TIPO HAVING ANO >= 1996")
        return [row[2] for row in cursor.fetchall() if row[1].startswith('SIND')]

    #unioes sindicais
    @property
    def barchart1_data4(self): # x
        cursor = self.query("SELECT ANO, TIPO, COUNT(DISTINCT SUBSTR(ID,0,INSTR(ID,'.')+1) || SUBSTR(SUBSTR(ID,INSTR(ID,'.')+1,LENGTH(ID)) ,0,INSTR(SUBSTR(ID,INSTR(ID,'.')+1,LENGTH(ID)),'.'))) AS NUM_ORG FROM Org_Sindical, ( SELECT DISTINCT CAST(strftime('%Y',date(Data_Primeira_Actividade)) AS DECIMAL) AS Ano FROM Org_Sindical WHERE Data_Primeira_Actividade IS NOT NULL AND Ano >= 1977 UNION SELECT DISTINCT CAST(strftime('%Y',date(Data_Ultima_Actividade)) AS DECIMAL) AS Ano FROM Org_Sindical WHERE Data_Primeira_Actividade IS NOT NULL) AS ANOS WHERE CAST(strftime('%Y',date(Data_Primeira_Actividade)) AS DECIMAL) <= ANO AND (Activa = 1 OR CAST(strftime('%Y',date(Data_Ultima_Actividade)) AS DECIMAL) >= ANO) GROUP BY ANO, TIPO HAVING ANO >= 1996")
        return [row[2] for row in cursor.fetchall() if row[1].startswith('UNI')]

    #tipos de organizações patronais
    @property
    def barchart2_labels(self): # x
        cursor = self.query("SELECT ANO, COUNT(DISTINCT SUBSTR(ID,0,INSTR(ID,'.')+1) || SUBSTR(SUBSTR(ID,INSTR(ID,'.')+1,LENGTH(ID)) ,0,INSTR(SUBSTR(ID,INSTR(ID,'.')+1,LENGTH(ID)),'.'))) AS NUM_ORG FROM Org_Patronal, ( SELECT DISTINCT CAST(strftime('%Y',date(Data_Primeira_Actividade)) AS DECIMAL) AS Ano FROM Org_Patronal WHERE Data_Primeira_Actividade IS NOT NULL AND Ano >= 1977 UNION SELECT DISTINCT CAST(strftime('%Y',date(Data_Ultima_Actividade)) AS DECIMAL) AS Ano FROM Org_Patronal WHERE Data_Primeira_Actividade IS NOT NULL) AS ANOS WHERE CAST(strftime('%Y',date(Data_Primeira_Actividade)) AS DECIMAL) <= ANO AND (Activa = 1 OR CAST(strftime('%Y',date(Data_Ultima_Actividade)) AS DECIMAL) >= ANO) GROUP BY ANO HAVING ANO >= 1996")
        return [row[0] for row in cursor.fetchall()]

    #confederacoes de empregadores
    @property
    def barchart2_data(self): # x
        cursor = self.query("SELECT ANO, TIPO, COUNT(DISTINCT SUBSTR(ID,0,INSTR(ID,'.')+1) || SUBSTR(SUBSTR(ID,INSTR(ID,'.')+1,LENGTH(ID)) ,0,INSTR(SUBSTR(ID,INSTR(ID,'.')+1,LENGTH(ID)),'.'))) AS NUM_ORG FROM Org_Patronal, ( SELECT DISTINCT CAST(strftime('%Y',date(Data_Primeira_Actividade)) AS DECIMAL) AS Ano FROM Org_Patronal WHERE Data_Primeira_Actividade IS NOT NULL AND Ano >= 1977 UNION SELECT DISTINCT CAST(strftime('%Y',date(Data_Ultima_Actividade)) AS DECIMAL) AS Ano FROM Org_Patronal WHERE Data_Primeira_Actividade IS NOT NULL) AS ANOS WHERE CAST(strftime('%Y',date(Data_Primeira_Actividade)) AS DECIMAL) <= ANO AND (Activa = 1 OR CAST(strftime('%Y',date(Data_Ultima_Actividade)) AS DECIMAL) >= ANO) GROUP BY ANO, TIPO HAVING ANO >= 1996")
        return [row[2] for row in cursor.fetchall() if row[1].startswith('CONF')]

    #federacoes de empregadores
    @property
    def barchart2_data2(self): #x
        cursor = self.query("SELECT ANO, TIPO, COUNT(DISTINCT SUBSTR(ID,0,INSTR(ID,'.')+1) || SUBSTR(SUBSTR(ID,INSTR(ID,'.')+1,LENGTH(ID)) ,0,INSTR(SUBSTR(ID,INSTR(ID,'.')+1,LENGTH(ID)),'.'))) AS NUM_ORG FROM Org_Patronal, ( SELECT DISTINCT CAST(strftime('%Y',date(Data_Primeira_Actividade)) AS DECIMAL) AS Ano FROM Org_Patronal WHERE Data_Primeira_Actividade IS NOT NULL AND Ano >= 1977 UNION SELECT DISTINCT CAST(strftime('%Y',date(Data_Ultima_Actividade)) AS DECIMAL) AS Ano FROM Org_Patronal WHERE Data_Primeira_Actividade IS NOT NULL) AS ANOS WHERE CAST(strftime('%Y',date(Data_Primeira_Actividade)) AS DECIMAL) <= ANO AND (Activa = 1 OR CAST(strftime('%Y',date(Data_Ultima_Actividade)) AS DECIMAL) >= ANO) GROUP BY ANO, TIPO HAVING ANO >= 1996")
        return [row[2] for row in cursor.fetchall() if row[1].startswith('FED')]

    #associacoes de empregadores
    @property
    def barchart2_data3(self): #x
        cursor = self.query("SELECT ANO, TIPO, COUNT(DISTINCT SUBSTR(ID,0,INSTR(ID,'.')+1) || SUBSTR(SUBSTR(ID,INSTR(ID,'.')+1,LENGTH(ID)) ,0,INSTR(SUBSTR(ID,INSTR(ID,'.')+1,LENGTH(ID)),'.'))) AS NUM_ORG FROM Org_Patronal, ( SELECT DISTINCT CAST(strftime('%Y',date(Data_Primeira_Actividade)) AS DECIMAL) AS Ano FROM Org_Patronal WHERE Data_Primeira_Actividade IS NOT NULL AND Ano >= 1977 UNION SELECT DISTINCT CAST(strftime('%Y',date(Data_Ultima_Actividade)) AS DECIMAL) AS Ano FROM Org_Patronal WHERE Data_Primeira_Actividade IS NOT NULL) AS ANOS WHERE CAST(strftime('%Y',date(Data_Primeira_Actividade)) AS DECIMAL) <= ANO AND (Activa = 1 OR CAST(strftime('%Y',date(Data_Ultima_Actividade)) AS DECIMAL) >= ANO) GROUP BY ANO, TIPO HAVING ANO >= 1996")
        return [row[2] for row in cursor.fetchall() if row[1].startswith('ASS')]

    #unioes de empregadores
    @property
    def barchart2_data4(self): #x
        cursor = self.query("SELECT ANO, TIPO, COUNT(DISTINCT SUBSTR(ID,0,INSTR(ID,'.')+1) || SUBSTR(SUBSTR(ID,INSTR(ID,'.')+1,LENGTH(ID)) ,0,INSTR(SUBSTR(ID,INSTR(ID,'.')+1,LENGTH(ID)),'.'))) AS NUM_ORG FROM Org_Patronal, ( SELECT DISTINCT CAST(strftime('%Y',date(Data_Primeira_Actividade)) AS DECIMAL) AS Ano FROM Org_Patronal WHERE Data_Primeira_Actividade IS NOT NULL AND Ano >= 1977 UNION SELECT DISTINCT CAST(strftime('%Y',date(Data_Ultima_Actividade)) AS DECIMAL) AS Ano FROM Org_Patronal WHERE Data_Primeira_Actividade IS NOT NULL) AS ANOS WHERE CAST(strftime('%Y',date(Data_Primeira_Actividade)) AS DECIMAL) <= ANO AND (Activa = 1 OR CAST(strftime('%Y',date(Data_Ultima_Actividade)) AS DECIMAL) >= ANO) GROUP BY ANO, TIPO HAVING ANO >= 1996")
        return [row[2] for row in cursor.fetchall() if row[1].startswith('UNI')]    
    
    #choroplethMapDistritos
    @property
    def map_labels(self): # TODO: NOT USED AT ALL
        cursor = self.query("SELECT Distrito_Sede, COUNT(Distinct ID) as NUM_ORG FROM Org_Sindical WHERE Activa=1 GROUP BY Distrito_Sede")
        lista = []
        for row in cursor.fetchall():
            if(row[0]!="" and row[0]!="REG. AUT. AÇORES" and row[0]!="REG. AUT. MADEIRA"):
                lista.append(row[0])
        
        return lista

    #choroplethMap
    @property
    def map_data(self): # x
        cursor = self.query("SELECT Distrito_Sede, COUNT(Distinct ID) as NUM_ORG FROM Org_Sindical WHERE Activa=1 GROUP BY Distrito_Sede")
        lista = []
        for row in cursor.fetchall():
            if(row[0]!="" and row[0]!="REG. AUT. AÇORES" and row[0]!="REG. AUT. MADEIRA"):
                lista.append(row[1])
        
        return lista
    
    @property
    def map_data_new(self): # x
        cursor = self.query("SELECT Distrito_Sede, COUNT(ID) as NUM_ORG FROM Org_Sindical WHERE Activa=1 AND Distrito_Sede != '' GROUP BY Distrito_Sede")
        return {row[0]: row[1] for row in cursor.fetchall()}

    #avisosGreveAnos
    @property
    def barchart3_labels(self): # x
        cursor = self.query("SELECT Ano_Inicio as Ano, COUNT(*) as NUM_GREVES FROM Avisos_Greve_New WHERE ANO >= 1996 GROUP BY Ano_Inicio")
        return [row[0] for row in cursor.fetchall()]

    
    #avisosGreve
    @property
    def barchart3_data(self): # x
        cursor = self.query("SELECT Ano_Inicio as Ano, COUNT(*) as NUM_GREVES FROM Avisos_Greve_New WHERE ANO >= 1996 GROUP BY Ano_Inicio")
        return [row[1] for row in cursor.fetchall()]
    
    def barchart3_labels_data():
        cursor = self.query("SELECT Ano_Inicio as Ano, COUNT(*) as NUM_GREVES FROM Avisos_Greve_New WHERE ANO >= 1996 GROUP BY Ano_Inicio")
        return [list(row) for row in cursor.fetchall()]

    
    #orgSindicaisAtivasPorSector
    @property
    def barchart5_labels(self): # x
        cursor = self.query("SELECT Sectores_Profissionais.Sector, COUNT(DISTINCT Org_Sindical.ID) AS Num_Org FROM Sectores_Profissionais LEFT JOIN Org_Sindical ON Org_Sindical.Sector = Sectores_Profissionais.Sector GROUP BY Sectores_Profissionais.Sector ORDER BY Sectores_Profissionais.Sector")
        return [row[0] for row in cursor.fetchall()]

    @property
    def barchart5_labels_red(self): # x
        cursor = self.query("SELECT Sectores_Profissionais.Sector, Sectores_Profissionais.Nome_Abrev, COUNT(DISTINCT Org_Sindical.ID) AS Num_Org FROM Sectores_Profissionais LEFT JOIN Org_Sindical ON Org_Sindical.Sector = Sectores_Profissionais.Sector GROUP BY Sectores_Profissionais.Sector, Sectores_Profissionais.Nome_Abrev ORDER BY Sectores_Profissionais.Sector")
        return [row[1] for row in cursor.fetchall()]

    #sectores
    @property
    def barchart5_data(self): # TODO: NOT USED AT ALL
        cursor = self.query("SELECT Sectores_Profissionais.Sector, COUNT(DISTINCT Org_Sindical.ID) AS Num_Org FROM Sectores_Profissionais LEFT JOIN Org_Sindical ON Org_Sindical.Sector = Sectores_Profissionais.Sector AND Activa = 1 GROUP BY Sectores_Profissionais.Sector ORDER BY Sectores_Profissionais.Sector")
        return [row[1] for row in cursor.fetchall()]


    #ircts por ano
    @property
    def barchart4_labels(self): # x
        cursor = self.query("SELECT Ano, COUNT(*) AS Num_IRCT FROM Actos_Negociacao_Colectiva WHERE ANO >= 1996 GROUP BY Ano")
        return [row[0] for row in cursor.fetchall()]

    @property
    def barchart4_data(self): # x
        cursor = self.query("SELECT Ano, COUNT(*) AS Num_IRCT FROM Actos_Negociacao_Colectiva WHERE ANO >= 1996 GROUP BY Ano")
        return [row[1] for row in cursor.fetchall()]

    
    
    
    
    
    @property
    def orgsindical1_data(self): # x
        lista = []
        cursor = self.query("SELECT Tipo, Nome, Acronimo, Distrito_Sede, Sector, Activa FROM Org_Sindical")
        for row in cursor.fetchall():
            lista.append(list(row))
        for i in range(len(lista)):
            for j in range(len(lista[i])):
                if lista[i][j] is None:
                    lista[i][j] = ""
                elif lista[i][j] == 1:
                    lista[i][j] = "Ativa"
                elif lista[i][j] == 0:
                    lista[i][j] = "Extinta"

        return lista

    def orgsindical_data(self): # TODO: NOT USED AT ALL
        lista = self.orgsindical1_data
        new_dict = {}
        
        for c, value in enumerate(lista):
            new_dict[str(c)] = value

        return new_dict

    def orgpatronal_data(self): # TODO: NOT USED AT ALL
        lista = []
        new_dict = {}
        cursor = self.query("SELECT Tipo, Nome, Acronimo, Distrito_Sede, Activa FROM Org_Patronal WHERE Tipo IS NOT NULL")
        
        for row in cursor.fetchall():
            lista.append(list(row))

        for i in range(len(lista)):
            for j in range(len(lista[i])):
                if lista[i][j] is None:
                    lista[i][j] = ""
                elif lista[i][j] == 1:
                    lista[i][j] = "Ativa"
                elif lista[i][j] == 0:
                    lista[i][j] = "Extinta"

        for c, value in enumerate(lista):
            new_dict[str(c)] = value

        return new_dict

    def updateChart(self, table, org): # TODO: NOT USED AT ALL
        lista_final = []
        lista_orgs_tipo_ano = self.orgs_tipo_ano(table, org)
        for lista_org_tipo_ano in lista_orgs_tipo_ano: lista_final.append(lista_org_tipo_ano)
        org_distritos = self.orgs_por_distrito(table, org)
        lista_greves = self.avisos_greve_ano(table, org)
        orgs_por_sector = self.orgs_por_sector(table, org)
        lista_final.append(org_distritos)
        lista_final.append(lista_greves)
        lista_final.append(orgs_por_sector)
        return lista_final

    def orgs_tipo_ano(self, table, org): # TODO: NOT USED AT ALL
        lista_orgs = []
        orgs_tipo_ano_conf = {} #confederacao
        orgs_tipo_ano_fed = {} #federacao
        orgs_tipo_ano_org = {} #organizacao / associacao
        orgs_tipo_ano_uni = {} #uniao
        linhas = 0        
        anos = self.barchart_labels
        for ano in anos:
            orgs_tipo_ano_conf[ano] = 0
            orgs_tipo_ano_fed[ano] = 0
            orgs_tipo_ano_org[ano] = 0
            orgs_tipo_ano_uni[ano] = 0


        if (table=="Unions" or not table) and org:
            cursor = self.query("""SELECT ANO, TIPO, COUNT(DISTINCT SUBSTR(ID,0,INSTR(ID,'.')+1) || SUBSTR(SUBSTR(ID,INSTR(ID,'.')+1,LENGTH(ID)) ,0,INSTR(SUBSTR(ID,INSTR(ID,'.')+1,LENGTH(ID)),'.'))) AS NUM_ORG FROM Org_Sindical, 
            ( SELECT DISTINCT CAST(strftime('%Y',date(Data_Primeira_Actividade)) AS DECIMAL) AS Ano FROM Org_Sindical 
            WHERE Data_Primeira_Actividade IS NOT NULL AND Ano >= 1977 UNION SELECT DISTINCT CAST(strftime('%Y',date(Data_Ultima_Actividade)) AS DECIMAL) AS Ano FROM Org_Sindical 
            WHERE Data_Primeira_Actividade IS NOT NULL) AS ANOS WHERE CAST(strftime('%Y',date(Data_Primeira_Actividade)) AS DECIMAL) <= ANO AND (Activa = 1 OR CAST(strftime('%Y',date(Data_Ultima_Actividade)) AS DECIMAL) >= ANO) AND 
            ID IN (SELECT ID FROM Org_Sindical WHERE Nome LIKE ? OR Acronimo LIKE ? AND Activa = 1 ) GROUP BY ANO, TIPO HAVING ANO >= 1996""",('%' + org + '%', '%' + org + '%')) 
        
        elif table=="Unions" or not table:
            cursor = self.query("""SELECT ANO, TIPO, COUNT(DISTINCT SUBSTR(ID,0,INSTR(ID,'.')+1) || SUBSTR(SUBSTR(ID,INSTR(ID,'.')+1,LENGTH(ID)) ,0,INSTR(SUBSTR(ID,INSTR(ID,'.')+1,LENGTH(ID)),'.'))) AS NUM_ORG FROM Org_Sindical, 
            ( SELECT DISTINCT CAST(strftime('%Y',date(Data_Primeira_Actividade)) AS DECIMAL) AS Ano FROM Org_Sindical
             WHERE Data_Primeira_Actividade IS NOT NULL AND Ano >= 1977 UNION SELECT DISTINCT CAST(strftime('%Y',date(Data_Ultima_Actividade)) AS DECIMAL) AS Ano
             FROM Org_Sindical WHERE Data_Primeira_Actividade IS NOT NULL) AS ANOS WHERE CAST(strftime('%Y',date(Data_Primeira_Actividade)) AS DECIMAL) <= ANO AND 
             (Activa = 1 OR CAST(strftime('%Y',date(Data_Ultima_Actividade)) AS DECIMAL) >= ANO) GROUP BY ANO, TIPO HAVING ANO >= 1996""")
        
        elif table=="Employees" and org:
            cursor = self.query("""SELECT ANO, TIPO, COUNT(DISTINCT SUBSTR(ID,0,INSTR(ID,'.')+1) || SUBSTR(SUBSTR(ID,INSTR(ID,'.')+1,LENGTH(ID)) ,0,INSTR(SUBSTR(ID,INSTR(ID,'.')+1,LENGTH(ID)),'.'))) AS NUM_ORG FROM Org_Patronal, 
            ( SELECT DISTINCT CAST(strftime('%Y',date(Data_Primeira_Actividade)) AS DECIMAL) AS Ano FROM Org_Patronal 
            WHERE Data_Primeira_Actividade IS NOT NULL AND Ano >= 1977 UNION SELECT DISTINCT CAST(strftime('%Y',date(Data_Ultima_Actividade)) AS DECIMAL) AS Ano FROM Org_Patronal 
            WHERE Data_Primeira_Actividade IS NOT NULL) AS ANOS WHERE CAST(strftime('%Y',date(Data_Primeira_Actividade)) AS DECIMAL) <= ANO AND (Activa = 1 OR CAST(strftime('%Y',date(Data_Ultima_Actividade)) AS DECIMAL) >= ANO) AND 
            ID IN (SELECT ID FROM Org_Patronal WHERE Nome LIKE ? OR Acronimo LIKE ? AND Activa = 1) AND TIPO IS NOT NULL GROUP BY ANO, TIPO HAVING ANO >= 1996""",('%' + org + '%', '%' + org + '%'))
        
        elif table=="Employees" and not org:
            cursor = self.query("""SELECT ANO, TIPO, COUNT(DISTINCT SUBSTR(ID,0,INSTR(ID,'.')+1) || SUBSTR(SUBSTR(ID,INSTR(ID,'.')+1,LENGTH(ID)) ,0,INSTR(SUBSTR(ID,INSTR(ID,'.')+1,LENGTH(ID)),'.'))) AS NUM_ORG FROM Org_Patronal, 
            ( SELECT DISTINCT CAST(strftime('%Y',date(Data_Primeira_Actividade)) AS DECIMAL) AS Ano FROM Org_Patronal
             WHERE Data_Primeira_Actividade IS NOT NULL AND Ano >= 1977 UNION SELECT DISTINCT CAST(strftime('%Y',date(Data_Ultima_Actividade)) AS DECIMAL) AS Ano
             FROM Org_Patronal WHERE Data_Primeira_Actividade IS NOT NULL) AS ANOS WHERE CAST(strftime('%Y',date(Data_Primeira_Actividade)) AS DECIMAL) <= ANO AND 
             (Activa = 1 OR CAST(strftime('%Y',date(Data_Ultima_Actividade)) AS DECIMAL) >= ANO) AND TIPO IS NOT NULL GROUP BY ANO, TIPO HAVING ANO >= 1996""")

        
        for row in cursor.fetchall():
            linhas = 1   
            if row[1].startswith("CONF"):
                orgs_tipo_ano_conf[row[0]] = row[2]
            
            elif row[1].startswith("FED"):
                orgs_tipo_ano_fed[row[0]] = row[2]
            
            elif row[1].startswith("SIND") or row[1].startswith("ASSO"):
                orgs_tipo_ano_org[row[0]] = row[2]
            
            elif row[1].startswith("UNI"):
                orgs_tipo_ano_uni[row[0]] = row[2]

        if linhas == 1:
            lista_orgs.append(list(orgs_tipo_ano_conf.values()))
            lista_orgs.append(list(orgs_tipo_ano_fed.values()))
            lista_orgs.append(list(orgs_tipo_ano_org.values()))
            lista_orgs.append(list(orgs_tipo_ano_uni.values()))
        else:
            lista_orgs = [[] for _ in range(4)]

        return lista_orgs


    def orgs_por_distrito(self, table, org): # TODO: NOT USED AT ALL
        distritos = self.map_labels
        linhas = 0
        org_distritos = {}

        for distrito in distritos:
            org_distritos[distrito] = 0


        if ((table=="Unions" or table=="") and org!=""):
            cursor = self.query("SELECT Distrito_Sede, COUNT(Distinct ID) as NUM_ORG FROM Org_Sindical WHERE ID IN (SELECT ID FROM Org_Sindical WHERE Nome LIKE ? OR Acronimo LIKE ?) AND Activa=1 GROUP BY Distrito_Sede",('%' + org + '%', '%' + org + '%'))
        
        elif ((table=="Unions" or table=="") and org==""):
            cursor = self.query("SELECT Distrito_Sede, COUNT(Distinct ID) as NUM_ORG FROM Org_Sindical WHERE Activa=1 GROUP BY Distrito_Sede")
        
        elif table=="Employees" and org!="":
            cursor = self.query("SELECT Distrito_Sede, COUNT(Distinct ID) as NUM_ORG FROM Org_Patronal WHERE ID IN (SELECT ID FROM Org_Patronal WHERE Nome LIKE ? OR Acronimo LIKE ?) AND Activa=1 GROUP BY Distrito_Sede",('%' + org + '%', '%' + org + '%'))

        elif table=="Employees" and org=="":
            cursor = self.query("SELECT Distrito_Sede, COUNT(Distinct ID) as NUM_ORG FROM Org_Patronal WHERE Activa=1 GROUP BY Distrito_Sede")

        for row in cursor.fetchall():
            linhas = 1
            if(row[0]!="" and row[0]!="REG. AUT. AÇORES" and row[0]!="REG. AUT. MADEIRA"):
                org_distritos[row[0]] = row[1]


        if linhas == 1:
            return list(org_distritos.values())
        else:
            return []


    def avisos_greve_ano(self, table, org): # TODO: NOT USED AT ALL
        anos_greve = self.barchart2_labels
        avisos_greve = {}
        linhas = 0
        
        for ano in anos_greve:
            avisos_greve[ano] = 0

        if ((table == "Unions" or table=="") and org!=""):
            cursor = self.query("SELECT Ano_Inicio as Ano, COUNT(*) as NUM_GREVES FROM Avisos_Greve WHERE Entidade_Sindical IN (SELECT Nome FROM Org_Sindical WHERE Nome LIKE ? OR Acronimo LIKE ? AND Activa = 1) AND ANO >= 1996 GROUP BY Ano_Inicio",('%' + org + '%', '%' + org + '%'))
        
        elif ((table=="Unions" or table=="") and org==""):
            cursor = self.query("SELECT Ano_Inicio as Ano, COUNT(*) as NUM_GREVES FROM Avisos_Greve WHERE ANO >= 1996 GROUP BY Ano_Inicio")
        
        elif table=="Employees" and org!="":
            cursor = self.query("SELECT Ano_Inicio as Ano, COUNT(*) as NUM_GREVES FROM Avisos_Greve WHERE Entidade_Patronal IN (SELECT Nome FROM Org_Patronal WHERE Nome LIKE ? OR Acronimo LIKE ? AND Activa = 1) AND ANO >= 1996 GROUP BY Ano_Inicio",('%' + org + '%', '%' + org + '%'))

        elif table=="Employees" and org=="":
            cursor = self.query("SELECT Ano_Inicio as Ano, COUNT(*) as NUM_GREVES FROM Avisos_Greve WHERE ANO >= 1996 GROUP BY Ano_Inicio")

        
        for row in cursor.fetchall():
            linhas = 1
            avisos_greve[row[0]] = row[1]


        if linhas == 1:
            return list(avisos_greve.values())
        else:
            return []

    def orgs_por_sector(self, table, org): # TODO: NOT USED AT ALL
        sectores = self.barchart3_labels
        orgs_por_sector = {}
        linhas = 0

        for sector in sectores:
            orgs_por_sector[sector] = 0

        if ((table == "Unions" or table=="") and org!=""):
            cursor = self.query("""SELECT Sectores_Profissionais.Sector, COUNT(DISTINCT Org_Sindical.ID) AS Num_Org 
                FROM Sectores_Profissionais LEFT JOIN Org_Sindical ON Org_Sindical.Sector = Sectores_Profissionais.Sector WHERE Org_Sindical.ID IN 
             (SELECT ID FROM Org_Sindical WHERE Nome LIKE ? OR Acronimo LIKE ? AND Activa = 1) 
             GROUP BY Sectores_Profissionais.Sector ORDER BY Sectores_Profissionais.Sector""",('%' + org + '%', '%' + org + '%'))

        elif ((table == "Unions" or table=="") and org==""):
            cursor = self.query("""SELECT Sectores_Profissionais.Sector, COUNT(DISTINCT Org_Sindical.ID) AS Num_Org 
                FROM Sectores_Profissionais LEFT JOIN Org_Sindical ON Org_Sindical.Sector = Sectores_Profissionais.Sector 
                AND Activa = 1 GROUP BY Sectores_Profissionais.Sector ORDER BY Sectores_Profissionais.Sector""")
        
        elif table=="Employees" and org!="":
            cursor = self.query("""SELECT Sectores_Profissionais.Sector, COUNT(DISTINCT Org_Patronal.ID) AS Num_Org 
                FROM Sectores_Profissionais LEFT JOIN Org_Patronal ON Org_Patronal.Sector = Sectores_Profissionais.Sector WHERE Org_Patronal.ID IN 
                (SELECT ID FROM Org_Patronal WHERE Nome LIKE ? OR Acronimo LIKE ? AND Activa = 1)
                 GROUP BY Sectores_Profissionais.Sector ORDER BY Sectores_Profissionais.Sector""",('%' + org + '%', '%' + org + '%'))

        elif table=="Employees" and org=="":
            cursor = self.query("""SELECT Sectores_Profissionais.Sector, COUNT(DISTINCT Org_Patronal.ID) AS Num_Org 
                FROM Sectores_Profissionais LEFT JOIN Org_Patronal ON Org_Patronal.Sector = Sectores_Profissionais.Sector AND Activa = 1
                GROUP BY Sectores_Profissionais.Sector ORDER BY Sectores_Profissionais.Sector""")

        for row in cursor.fetchall():
            linhas = 1
            orgs_por_sector[row[0]] = row[1]

        if linhas == 1:
            return list(orgs_por_sector.values())
        else:
            return []
    

    def export_to_excel(self, table, org, setor, distrito):        
        data1 = [ ]
        data1_names = ["Código Identificador da Organização" , "Tipo de Organização" , "Nome da Organização", "Acrónimo", "Concelho da Sede", "Distrito da Sede", "Data da Primeira Atividade Registada", "Data da Última Atividade Registada", "Ativa ou Extinta"]
        cursor1 = self.query("SELECT ID, Tipo, Nome, Acronimo, Concelho_Sede, Distrito_Sede, Data_Primeira_Actividade, Data_Ultima_Actividade, Activa FROM Org_Sindical")
        for row in cursor1.fetchall(): data1.append(list(row))
        for i in range(len(data1)):
            for j in range(len(data1[i])):
                if j == 13:
                    if data1[i][j] is None: data1[i][j] = ""
                    elif data1[i][j] == 1: data1[i][j] = "Ativa"
                    elif data1[i][j] == 0: data1[i][j] = "Extinta"

        data2 = [ ]
        data2_names = [ "Código Identificador da Organização" , "Tipo de Organização" , "Nome da Organização", "Acrónimo", "Concelho da Sede", "Distrito da Sede", "Data da Primeira Atividade Registada", "Data da Última Atividade Registada", "Ativa ou Extinta" ]
        cursor2 = self.query("SELECT ID, Tipo, Nome, Acronimo, Concelho_Sede, Distrito_Sede, Data_Primeira_Actividade, Data_Ultima_Actividade, Activa FROM Org_Patronal")
        for row in cursor2.fetchall(): data2.append(list(row))
        for i in range(len(data2)):
            for j in range(len(data2[i])):
                if j == 12:
                    if data2[i][j] is None: data2[i][j] = ""
                    elif data2[i][j] == 1: data2[i][j] = "Ativa"
                    elif data2[i][j] == 0: data2[i][j] = "Extinta"


        data3 = [ ]
        data3_names = [ "Código Identificador da Organização", "Nome da Organização", "Identificador do Acto de Negociação", "Nome Acto", "Tipo Acto", "Natureza", "CAE", "Ano", "Numero", "Série", "URL pata BTE", "Âmbito Geográfico" ]
        cursor3 = self.query("""
            SELECT DISTINCT ID_Organizacao_Sindical, Org_Sindical.Nome, Actos_Negociacao_Colectiva.ID, Nome_Acto, Tipo_Acto, Natureza, CAE, Actos_Negociacao_Colectiva.Ano, Actos_Negociacao_Colectiva.Numero, Actos_Negociacao_Colectiva.Serie, Actos_Negociacao_Colectiva.URL, Actos_Negociacao_Colectiva.Ambito_Geografico
                       FROM Actos_Negociacao_Colectiva
               NATURAL JOIN Outorgantes_Actos, Org_Sindical
                      WHERE Org_Sindical.ID=ID_Organizacao_Sindical
                        AND ID_Organizacao_Sindical IS NOT NULL 
            UNION
            SELECT DISTINCT ID_Organizacao_Patronal, Org_Patronal.Nome, Actos_Negociacao_Colectiva.ID, Nome_Acto, Tipo_Acto, Natureza, CAE, Actos_Negociacao_Colectiva.Ano, Actos_Negociacao_Colectiva.Numero, Actos_Negociacao_Colectiva.Serie, Actos_Negociacao_Colectiva.URL, Actos_Negociacao_Colectiva.Ambito_Geografico
                       FROM Actos_Negociacao_Colectiva
               NATURAL JOIN Outorgantes_Actos, Org_Patronal
                      WHERE Org_Patronal.ID=ID_Organizacao_Patronal
                        AND ID_Organizacao_Patronal IS NOT NULL""")
        for row in cursor3.fetchall(): data3.append(list(row))
                
        data4 = [ ]
        data4_names = [ "Código Identificador da Organização", "Nome da Organização", "Ano de Início", "Mês de Início", "Ano de Fim", "Mês de Fim" ]
        cursor4 = self.query("SELECT Avisos_Greve.Id_Entidade_Sindical, Org_Sindical.Nome, Ano_Inicio, Mes_Inicio, Ano_Fim, Mes_Fim FROM Avisos_Greve, Org_Sindical WHERE Avisos_Greve.Id_Entidade_Sindical = Org_Sindical.ID")
        for row in cursor4.fetchall(): data4.append(list(row))

        data5 = [ ]
        data5_names = [ "Código Identificador da Organização", "Nome da Organização", "Ano", "Número", "Série", "URL para BTE" ]
        cursor5 = self.query("SELECT ID_Organizacao_Sindical, Org_Sindical.Nome, Ano, Numero, Serie, URL FROM Mencoes_BTE_Org_Sindical, Org_Sindical WHERE Mencoes_BTE_Org_Sindical.ID_Organizacao_Sindical = Org_Sindical.ID AND Mudanca_Estatuto = TRUE")
        for row in cursor5.fetchall(): data5.append(list(row))

        data6 = [ ]
        data6_names = [ "Código Identificador da Organização", "Nome da Organização", "Ano", "Número", "Série", "URL para BTE" ]
        cursor6 = self.query("SELECT ID_Organizacao_Sindical, Org_Sindical.Nome, Ano, Numero, Serie, URL FROM Mencoes_BTE_Org_Sindical, Org_Sindical WHERE Mencoes_BTE_Org_Sindical.ID_Organizacao_Sindical = Org_Sindical.ID AND Eleicoes = TRUE")
        for row in cursor6.fetchall(): data6.append(list(row))
            
        return pd.DataFrame(data1, columns=data1_names) , pd.DataFrame(data2, columns=data2_names) , pd.DataFrame(data3, columns=data3_names) , pd.DataFrame(data4, columns=data4_names) , pd.DataFrame(data5, columns=data5_names) , pd.DataFrame(data6, columns=data6_names)
        
        registos = []
        colunas = []
        linhas = []
        
        if ((table == "Unions" or table=="") and org!=""):
            table = 'Org_Sindical'
            if setor != "":
                cursor = self.query("SELECT * FROM Org_Sindical WHERE (Nome LIKE ? OR Acronimo LIKE ?) AND Sector LIKE ?", ('%' + org + '%', '%' + org + '%', '%' + setor + '%'))
            elif distrito != "":
                cursor = self.query("SELECT * FROM Org_Sindical WHERE (Nome LIKE ? OR Acronimo LIKE ?) AND Distrito_Sede LIKE ?", ('%' + org + '%', '%' + org + '%', '%' + distrito + '%'))
            else:
                cursor = self.query("SELECT * FROM Org_Sindical WHERE Nome LIKE ? OR Acronimo LIKE ?", ('%' + org + '%', '%' + org + '%'))
        
        elif ((table == "Unions" or table=="") and org==""):
            table = 'Org_Sindical'
            if setor != "":
                cursor = self.query("SELECT * FROM Org_Sindical WHERE Sector LIKE ?", ('%' + setor + '%'))
            elif distrito != "":
                cursor = self.query("SELECT * FROM Org_Sindical WHERE Distrito_Sede LIKE ?", ('%' + distrito + '%'))
            else:
                cursor = self.query("SELECT * FROM Org_Sindical")

        elif table=="Employees" and org!="":
            table = 'Org_Patronal'
            if setor != "":
                cursor = self.query("SELECT * FROM Org_Patronal WHERE (Nome LIKE ? OR Acronimo LIKE ?) AND Sector LIKE ?", ('%' + org + '%', '%' + org + '%', '%' + setor + '%'))
            elif distrito != "":
                cursor = self.query("SELECT * FROM Org_Patronal WHERE (Nome LIKE ? OR Acronimo LIKE ?) AND Distrito_Sede LIKE ?", ('%' + org + '%', '%' + org + '%', '%' + distrito + '%'))
            else:
                cursor = self.query("SELECT * FROM Org_Patronal WHERE Nome LIKE ? OR Acronimo LIKE ?", ('%' + org + '%', '%' + org + '%'))

        elif table=="Employees" and org=="": 
            table = 'Org_Patronal'
            if setor != "":
                cursor = self.query("SELECT * FROM Org_Patronal WHERE Sector LIKE ?", ('%' + setor + '%'))
            elif distrito != "":
                cursor = self.query("SELECT * FROM Org_Patronal WHERE Distrito_Sede LIKE ?", ('%' + distrito + '%'))
            else:
                cursor = self.query("SELECT * FROM Org_Patronal")

        for tuplo in self.get_columns(table):
            cols = list(tuplo)
            if cols[0] == "Activa":
                cols[0] = "Estado"
            elif cols[0] == "Sector":
                cols[0] = "Setor"
            elif cols[0] == "Acronimo":
                cols[0] = "Acrónimo"

            colunas.append(cols[0])

        print(colunas[18])

        for row in cursor.fetchall():
            linhas.append(list(row))

        # 18 é a coluna do Estado
        for i in range(len(linhas)):
            for j in range(len(linhas[i])):
                if j == 18:
                    if linhas[i][j] is None:
                        linhas[i][j] = ""
                    elif linhas[i][j] == 1:
                        linhas[i][j] = "Ativa"
                    elif linhas[i][j] == 0:
                        linhas[i][j] = "Extinta"
        
        return pd.DataFrame(linhas, columns=colunas)