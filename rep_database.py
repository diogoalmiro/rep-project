import urllib.request
import json
import csv
import base64
import sqlite3
import os
import re
from datetime import datetime
import greves

#com autenticacao
def getResponse(tabela, ano):
	username = os.environ["DGERT_LOGIN_USER"]
	password = os.environ["DGERT_LOGIN_PASSWORD"]
	request = urllib.request.Request("https://www.dgert.gov.pt/application-dgert-projeto-rep-php/" + tabela + ".php?ano=" + str(ano))
	string = '%s:%s' % (username, password)
	base64string = base64.standard_b64encode(string.encode('utf-8'))
	request.add_header("Authorization", "Basic %s" % base64string.decode('utf-8'))   
	response = urllib.request.urlopen(request)
	data = response.read()
	result = data.decode("utf8")
	response.close()
	return result

def create_database_tables(cursor):
	with open('rep-database.sql', 'r') as sql_file:
		cursor.executescript(sql_file.read())

def getTable_AlteracoesEstatutos(ano,cursor):
	result = getResponse("alteracoesEstatutos", ano)
	final = json.loads(result)
	
	for key in final:	
		linha = final[key]

		format_str = """INSERT INTO TEMP_ALTERACOES_ESTATUTOS1 (TIPO,ESPECIE,SUB_ESPECIE,NUMERO,ANO,CONTROLO,SERVICO,CODENTG,CODENTE,NUMALT,NUMBTE,DATABTE,SERIEBTE,AMBITO_GEOGRAFICO)
    	VALUES ("{tipo}", "{especie}", "{subEspecie}", "{numero}", "{ano}", "{controlo}","{servico}","{codEntG}","{codEntE}","{numAlt}","{numBTE}","{dataBTE}","{serieBTE}","{ambitoGeografico}");"""
		
		if "ambitoGeografico" not in linha:
			ambGeo = ""
		else:
			ambGeo = linha["ambitoGeografico"]
		if "servico" not in linha:
                        servico = ""
		else:
			servico = linha["servico"]
		
		sql_command = format_str.format(tipo=linha["tipo"],especie=linha["especie"],subEspecie=linha["subEspecie"],numero=linha["numero"],ano=linha["ano"],controlo=linha["controlo"],
			servico=servico,codEntG=linha["codEntG"],codEntE=linha["codEntE"],numAlt=linha["numAlt"],numBTE=linha["numBTE"],dataBTE=linha["dataBTE"],
			serieBTE=linha["serieBTE"],ambitoGeografico=ambGeo)

		cursor.execute(sql_command)


def alteracoesEstatutos(cursor):
	for ano in range(1975,datetime.today().year+1):
		getTable_AlteracoesEstatutos(ano,cursor)


def getTable_EleicoesCorposGerentes(ano,cursor):
	result = getResponse("eleicoesCorposGerentes", ano)
	final = json.loads(result)	

	for key in final:
		linha = final[key]

		format_str = """INSERT INTO TEMP_ELEICAO_CORPOS_GERENTES1 (CODENTG,CODENTE,NUMALT,NUMERO_ELEICAO,DATA_ELEICAO,INSCRITOS,VOTANTES,MESES_MANDATO,DATABTE,NUMBTE,SERIEBTE,NUMMAXEFECT,NUMMINEFECT,NUMMAXSUPL,NUMMINSUPL,NUM_H_EFECT,NUM_H_SUPL,NUM_M_EFECT,NUM_M_SUPL,TIPO,ESPECIE,SUB_ESPECIE,NUMERO,ANO,CONTROLO,SERVICO)
    	VALUES ("{codEntG}", "{codEntE}", "{numAlt}", "{numeroEleicao}", "{dataEleicao}", "{inscritos}","{votantes}","{mesesMandato}","{dataBTE}","{numBTE}","{serieBTE}","{numMaxEfect}","{numMinEfect}","{numMaxSupl}","{numMinSupl}","{numHEfect}","{numHSupl}","{numMEfect}","{numMSupl}","{tipo}", "{especie}", "{subEspecie}", "{numero}", "{ano}", "{controlo}","{servico}");"""

		if "servico" not in linha:
			servico = ""
		else:
			servico = linha["servico"]

		if "inscritos" not in linha:
			inscritos = ""
		else:
			inscritos = linha["inscritos"]

		if "votates" not in linha:
			votantes = ""
		else:
			inscritos = linha["votantes"]
			
		sql_command = format_str.format(codEntG=linha["codEntG"],codEntE=linha["codEntE"],numAlt=linha["numAlt"],numeroEleicao=linha["numeroEleicao"],dataEleicao=linha["dataEleicao"],inscritos=inscritos,
			votantes=votantes,mesesMandato=linha["mesesMandato"],dataBTE=linha["dataBTE"],numBTE=linha["numBTE"],serieBTE=linha["serieBTE"],numMaxEfect=linha["numMaxEfect"],
			numMinEfect=linha["numMinEfect"],numMaxSupl=linha["numMaxSupl"],numMinSupl=linha["numMinSupl"],numHEfect=linha["numHEfect"],numHSupl=linha["numHSupl"],numMEfect=linha["numMEfect"],
			numMSupl=linha["numMSupl"],tipo=linha["tipo"],especie=linha["especie"],subEspecie=linha["subEspecie"],numero=linha["numero"],ano=linha["ano"],controlo=linha["controlo"],servico=servico)
		
		cursor.execute(sql_command)


def eleicoesCorposGerentes(cursor):
	for ano in range(1975,datetime.today().year+1):
		getTable_EleicoesCorposGerentes(ano,cursor)


def getTable_Entidades(ano,cursor):
	result = getResponse("entidades", ano)
	final = json.loads(result)

	for key in final:
		linha = final[key]

		format_str = """INSERT INTO TEMP_ENTIDADES1 (CODENTG,CODENTE,NUMALT,SIGLA,NOME_ENTIDADE,CODIGOPOSTAL_ENTIDADE,ID_DISTRITO,DISTRITO_DESCRICAO,ESTADO_ENTIDADE, MORADA_ENTIDADE, LOCAL_MORADA_ENTIDADE, AREA_POSTAL_ENTIDADE, TELEFONE_ENTIDADE, FAX_ENTIDADE)
    	VALUES ("{codEntG}", "{codEntE}", "{numAlt}", "{sigla}", "{nomeEntidade}", "{codigoPostalEntidade}","{idDistrito}","{distritoDescricao}","{estadoEntidade}","{moradaEntidade}","{localMoradaEntidade}","{areaPostalEntidade}","{telefoneEntidade}","{faxEntidade}");"""
		
		nomeEntidade = linha["nomeEntidade"].replace('"','')
		moradaEntidade = linha["moradaEntidade"].replace('"','')

		if "sigla" not in linha:
			sigla = ""
		else:
			sigla = linha["sigla"]

		if "telefoneEntidade" not in linha:
			telefoneEntidade = ""
		else:
			telefoneEntidade = linha["telefoneEntidade"]

		if "faxEntidade" not in linha:
			faxEntidade = ""
		else:
			faxEntidade = linha["faxEntidade"]

		if "distritoDescricao" not in linha:
			distritoDescricao = ""
		else:
			distritoDescricao = linha["distritoDescricao"]

		sql_command = format_str.format(codEntG=linha["codEntG"],codEntE=linha["codEntE"],numAlt=linha["numAlt"],sigla=sigla,nomeEntidade=nomeEntidade,codigoPostalEntidade=linha["codigoPostalEntidade"],idDistrito=linha["idDistrito"],distritoDescricao=distritoDescricao,
			estadoEntidade=linha["estadoEntidade"],moradaEntidade=moradaEntidade,localMoradaEntidade=linha["localMoradaEntidade"],areaPostalEntidade=linha["areaPostalEntidade"],telefoneEntidade=telefoneEntidade,faxEntidade=faxEntidade)

		cursor.execute(sql_command)

def entidades(cursor):
	#ano = 0 devolve todos os registos, ?ano tbm devolve todos
	getTable_Entidades(0,cursor)



#VER DEPOIS AS COLUNAS RETORNADAS DO PHP
def getTable_Ircts(ano,cursor):
	result = getResponse("ircts", ano)
	final = json.loads(result)

	for key in final:
		
		linha = final[key]

		format_str = """INSERT INTO TEMP_IRCT (NUMERO,NUMERO_SEQUENCIAL,ANO,TIPO_CONVENCAO_CODIGO,TIPO_CONVENCAO_DESCR,TIPO_CONVENCAO_DESCR_LONG,TIPO_CONVENCAO_ORDEM,NATUREZA_CODIGO,NATUREZA_DESCRICAO,NOMECC,AMBITO_GEOGRAFICO_IRCT,AMBITO_GEOGRAFICO_CODE_IRCT,NUMBTE,DATABTE,SERIEBTE,AMBGEG,DATA_EFEITOS,AREA,DIST,CONC,PROV,CODCAE, REVCAE)
    	VALUES ("{numero}", "{numeroSequencial}", "{ano}", "{tipoConvencaoCodigo}", "{tipoConvencaoDescr}", "{tipoConvencaoDescrLong}","{tipoConvencaoOrdem}","{naturezaCodigo}","{naturezaDescricao}","{nomeCC}","{ambitoGeograficoIRCT}","{ambitoGeograficoCodeIRCT}","{numBTE}","{dataBTE}","{serieBTE}","{ambGeg}","{dataEfeitos}","{area}","{dist}","{conc}","{prov}","{codCAE}","{revCAE}");"""

		nomeCC = linha["nomeCC"].replace('"','')

		if "dataEfeitos" not in linha:
			dataEfeitos = ""
		else:
			dataEfeitos = linha["dataEfeitos"]


		if "codCAE" not in linha:
			codCAE = ""
		else:
			codCAE = linha["codCAE"]

		if "naturezaDescricao" not in linha:
			naturezaDescricao = ""
		else:
			naturezaDescricao = linha["naturezaDescricao"]

		if "ambGeg" not in linha:
			ambGeg = ""
		else:
			ambGeg = linha["ambGeg"]
		
		sql_command = format_str.format(numero=linha["numero"],numeroSequencial=linha["numeroSequencial"],ano=linha["ano"],tipoConvencaoCodigo=linha["tipoConvencaoCodigo"],tipoConvencaoDescr=linha["tipoConvencaoDescr"],tipoConvencaoDescrLong=linha["tipoConvencaoDescrLong"],
			tipoConvencaoOrdem=linha["tipoConvencaoOrdem"],naturezaCodigo=linha["naturezaCodigo"],naturezaDescricao=naturezaDescricao,nomeCC=nomeCC,ambitoGeograficoIRCT=linha["ambitoGeograficoIRCT"],ambitoGeograficoCodeIRCT=linha["ambitoGeograficoCodeIRCT"],
			numBTE=linha["numBTE"],dataBTE=linha["dataBTE"],serieBTE=linha["serieBTE"],ambGeg=ambGeg,dataEfeitos=dataEfeitos,
			area=linha["area"],dist=linha["dist"],conc=linha["conc"],prov=linha["prov"],codCAE=codCAE,revCAE=linha["revCAE"])

		cursor.execute(sql_command)



def ircts(cursor):
	for ano in range(1975,datetime.today().year+1):
		getTable_Ircts(ano,cursor)


def getTable_Outorgantes(ano,cursor):
	result = getResponse("outorgantes", ano)
	final = json.loads(result)

	for key in final:

		linha = final[key]

		format_str = """INSERT INTO TEMP_OUTORGANTES (NUM,NUMSEQ,ANO,TIPOCONV,CODENTG,CODENTE,NUMALT,SIGLA_ENT_E,NOME_ENT_E)
    	VALUES ("{numero}", "{numSeq}", "{ano}", "{tipoConv}", "{CodEntG}", "{CodEntE}","{numAlt}","{siglaEntE}","{nomeEntE}");"""

		nomeEntE = linha["nomeEntE"].replace('"','')

		if "siglaEntE" not in linha:
			siglaEntE = ""
		else:
			siglaEntE = linha["siglaEntE"]
		
		sql_command = format_str.format(numero=linha["numero"],numSeq=linha["numSeq"],ano=linha["ano"],tipoConv=linha["tipoConv"],CodEntG=linha["CodEntG"],CodEntE=linha["CondEntE"],
			numAlt=linha["numAlt"],siglaEntE=siglaEntE,nomeEntE=nomeEntE)

		cursor.execute(sql_command)

def outorgantes(cursor):
	for ano in range(1975,datetime.today().year+1):
		getTable_Outorgantes(ano,cursor)


def getTable_Processos(ano,cursor):
	result = getResponse("processos", ano)
	final = json.loads(result)	

	for key in final:
		linha = final[key]

		format_str = """INSERT INTO TEMP_PROCESSOS1 (TIPO,ESPECIE,SUB_ESPECIE,NUMERO,ANO,CONTROLO,SERVICO,COD_ASSUNTO,ASSUNTO,DESIGNACAO,TITULO,DATA_ABERTURA_PROCESSO)
    	VALUES ("{tipo}", "{especie}", "{subEspecie}", "{numero}", "{ano}", "{controlo}","{servico}","{codAssunto}","{assunto}","{designacao}","{titulo}","{dataAberturaProcesso}");"""

		titulo = linha["titulo"].replace('"','')

		if "dataAberturaProcesso" not in linha:
			dataAP = ""
		else:
			dataAP = linha["dataAberturaProcesso"]

		sql_command = format_str.format(tipo=linha["tipo"],especie=linha["especie"],subEspecie=linha["subEspecie"],numero=linha["numero"],ano=linha["ano"],controlo=linha["controlo"],
			servico=linha["servico"],codAssunto=linha["codAssunto"],assunto=linha["assunto"],designacao=linha["designacao"],titulo=titulo,dataAberturaProcesso=dataAP)

		cursor.execute(sql_command)


def processos(cursor):
	for ano in range(1975,datetime.today().year+1):
		getTable_Processos(ano,cursor)


def municipios_codigosPostais(cursor):
	cursor.execute("""CREATE TABLE TEMP_MUNICIPIOS( 
		DD VARCHAR(2),
		CC VARCHAR(2),
		DESIG VARCHAR(1000)
	);""")

	cursor.execute("""CREATE TABLE TEMP_CP( 
		DD VARCHAR(2),
		CC VARCHAR(2),
		LLLL VARCHAR(3),
		LOCALIDADE VARCHAR(100),
		ART_COD VARCHAR(100),
		ART_TIPO VARCHAR(100),
		PRI_PREP VARCHAR(100),
		ART_TITULO VARCHAR(100),
		SEG_PREP VARCHAR(100),
		ART_DESIG VARCHAR(100),
		ART_LOCAL VARCHAR(100),
		TRO??O VARCHAR(100),
		PORTA VARCHAR(100),
		CLIENTE VARCHAR(100),
		CP4 VARCHAR(4),
		CP3 VARCHAR(3),
		CPALF VARCHAR(100)
	);""")


	f = open("./postal-codes/concelhos.txt", "r",encoding="utf8")
	f.readline()
	to_db = []
	for line in f:
  		values = line.split(";")
  		to_db.append((values[0],values[1],values[2]))

	f.close()

	cursor.executemany("INSERT INTO TEMP_MUNICIPIOS(DD,CC,DESIG) VALUES(?,?,?);", to_db)

	f = open("./postal-codes/todos_cp.txt", "r",encoding="utf8")
	f.readline()
	to_db = []
	for line in f:
  		values = line.split(";")
  		to_db.append((values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16]))
  	
	f.close()

	cursor.executemany("INSERT INTO TEMP_CP(DD,CC,LLLL,LOCALIDADE,ART_COD,ART_TIPO,PRI_PREP,ART_TITULO,SEG_PREP,ART_DESIG,ART_LOCAL,TRO??O,PORTA,CLIENTE,CP4,CP3,CPALF) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);", to_db)

def websites(cursor):

	cursor.execute("""CREATE TABLE TEMP_WEBSITES_SINDICAIS( 
		NOME_ORG VARCHAR(1000),
		WEBSITE VARCHAR(1000)
	);""")

	cursor.execute("""CREATE TABLE TEMP_WEBSITES_PATRONAIS( 
		NOME_ORG VARCHAR(1000),
		WEBSITE VARCHAR(1000)
	);""")


	f = open("./institution-websites/websites_sindicais.txt", "r",encoding="utf8")
	f.readline()
	to_db = []
	for line in f:
		values = line.split(";")
		nome_org = values[0]
		website = values[1]
		if website == " ":
			website = ""

		to_db.append((nome_org, website))

	f.close()

	cursor.executemany("INSERT INTO TEMP_WEBSITES_SINDICAIS(NOME_ORG,WEBSITE) VALUES(?,?);", to_db)

	f = open("./institution-websites/websites_patronais.txt", "r",encoding="utf8")
	f.readline()
	to_db = []
	for line in f:
		values = line.split(";")
		nome_org = values[0]
		website = values[1]
		if website == " ":
			website = ""

		to_db.append((nome_org, website))
		
	f.close()

	cursor.executemany("INSERT INTO TEMP_WEBSITES_PATRONAIS(NOME_ORG,WEBSITE) VALUES(?,?);", to_db)

def tratarDataEleicao(dataEleicao):
	
	meses_numero = {"JANEIRO": "01", "FEVEREIRO": "02", "MAR??O": "03", "ABRIL": "04", "MAIO": "05", "JUNHO": "06", "JULHO": "07",
	 "AGOSTO": "08", "SETEMBRO": "09", "OUTUBRO": "10", "NOVEMBRO": "11", "DEZEMBRO": "12"}

	data = re.findall("[0-9].*20[0-2][0-9]", dataEleicao)

	if len(data) == 0:
		data_final = None
		return data_final
	
	dia_mes_ano = data[0].replace(": ","")
	dia_mes_ano = dia_mes_ano.replace(" - ","")
	dia_mes_ano = dia_mes_ano.replace(" -","")
	dia_mes_ano = dia_mes_ano.replace("- ","")
	dia_mes_ano = dia_mes_ano.replace("DE ","")
	dia_mes_ano = dia_mes_ano.replace("E ","")
	dia_mes_ano = dia_mes_ano.replace(",", "")
	dia_mes_ano = dia_mes_ano.replace(" A ", " ")
	
	dia_mes_ano_tratado = dia_mes_ano.split(" ")
	
	#obter primeiro dia da eleicao
	dia = dia_mes_ano_tratado[-3]

	if len(dia) == 1:
		dia = "0" + dia
	
	#obter mes da eleicao
	mes = dia_mes_ano_tratado[-2]

	#obter ano da eleicao
	ano = dia_mes_ano_tratado[-1]
	
	numero_do_mes = ""

	if mes in meses_numero:
		numero_do_mes = meses_numero[mes]

	data_final = ano + "-" + numero_do_mes + "-" + dia

	return data_final

def tratarGeneroMasculino():

	with open("./pt-person-names/nomesMasculino.csv", "r", encoding="ISO-8859-1") as f:
		# csv.DictReader uses first line in file for column headings by default
		dr = csv.DictReader(f, delimiter=',')
		nomes_masculinos = []
		for i in dr:
			values = list(i.values())
			nome_masculino = values[1].upper()
			nomes_masculinos.append(nome_masculino)
	
	return nomes_masculinos


def tratarGeneroFeminino():

	with open("./pt-person-names/nomesFeminino.csv", "r", encoding="ISO-8859-1") as f:
		# csv.DictReader uses first line in file for column headings by default
		dr = csv.DictReader(f, delimiter=',')
		nomes_femininos = []
		for i in dr:
			values = list(i.values())
			nome_feminino = values[1].upper()
			nomes_femininos.append(nome_feminino)
	
	return nomes_femininos

def tratarNomeDirigente(nome_dirigente):

	if "BILHETE" in nome_dirigente:
		nome_dirigente = nome_dirigente[ : nome_dirigente.find("BILHETE")]
			
	nome_dirigente = nome_dirigente.replace("\n","")

	if nome_dirigente.startswith(' '):
		nome_dirigente = nome_dirigente.replace(" ", "", 1)

	nome_dirigente = nome_dirigente.replace(" ??? ","- ")
	nome_dirigente = nome_dirigente.replace("DR.?? ", "")
	nome_dirigente = nome_dirigente.replace("DR?? ", "")
	nome_dirigente = nome_dirigente.replace("DR. ", "")
	nome_dirigente = nome_dirigente.replace("DR ", "")
	nome_dirigente = nome_dirigente.replace("SR ", "")
	nome_dirigente = nome_dirigente.replace("PROF.?? ", "")
	nome_dirigente = nome_dirigente.replace("PROF. ", "")
	nome_dirigente = nome_dirigente.replace("PROFESSORA ", "")
	nome_dirigente = nome_dirigente.replace("PROFESSOR ", "")
	nome_dirigente = nome_dirigente.replace("DOUTORA ", "")
	nome_dirigente = nome_dirigente.replace("DOUTOR ", "")
	nome_dirigente = nome_dirigente.replace("EMBAIXADOR ", "")
	nome_dirigente = nome_dirigente.replace("M.??", "MARIA")
	nome_dirigente = nome_dirigente.replace("M??", "MARIA")
	nome_dirigente = nome_dirigente.replace("M. ??", "MARIA")
	nome_dirigente = nome_dirigente.replace("ENGENHEIRO ", "")
	nome_dirigente = nome_dirigente.replace(" (", "")
	nome_dirigente = nome_dirigente.replace("(", "")
	nome_dirigente = nome_dirigente.replace("SUPLENTES ", "")
	nome_dirigente = nome_dirigente.replace("MESTRE ", "")
	nome_dirigente = nome_dirigente.replace("JOSE ","JOS?? ")
	nome_dirigente = nome_dirigente.replace("CESAR ","CES??R ")

	return nome_dirigente

def obterGeneroDirigente(nome_dirigente, nomes_masculinos, nomes_femininos):

	com_numeros = re.findall("^[0-9]", nome_dirigente) 

	#obter numero proprio
	if len(com_numeros) != 0:
		numero_nomes = nome_dirigente.split(" ", 1)
		numero = numero_nomes[0]
		nomes = numero_nomes[1]
		nome_proprio = nomes.split(" ")[0]
	else:
		nome_dirigente = nome_dirigente[ : nome_dirigente.find("-")]
		nome_proprio = nome_dirigente.split(" ")[0]

	#obter o genero do dirigente
	if nome_proprio in nomes_masculinos:
		genero = "MASCULINO"
	
	elif nome_proprio in nomes_femininos:
		genero = "FEMININO"

	else:
		genero = None

	if genero is None:
		if nome_proprio.endswith("A") or nome_proprio.endswith("??"):
			genero = "FEMININO"
		
		elif nome_proprio.endswith("O"):
			genero = "MASCULINO"

	return genero 



def listasDirigentes(cursor):

	to_db = []
	nomes_masculinos = tratarGeneroMasculino()
	nomes_femininos = tratarGeneroFeminino()

	ficheiros = os.listdir(r"./BTE-download-scripts/mencoes/")

	for file in ficheiros:
		f = open("./BTE-download-scripts/mencoes/" + file, "r", encoding="utf8")
		file = file.replace(".txt", "")
		
		nome_num_ano = file.split("_")
		nome_org = nome_num_ano[0]
		num_boletim = nome_num_ano[1]
		ano_boletim = int(nome_num_ano[2])
		id_org = None
		id_nome = None
		cargo = None
		data_inicio = None
		data_fim = None
		data_eleicao = None
		
		if nome_org == "SIND DOS PROF DE LACTIC??NIOS, ALIMENTA????O, ESCRIT??RIOS, COM??RCIO, SERVI??OS, TRANSP. RODOVI??RIOS, METALOMEC??NICA, METALURGIA, CONSTR. CIVIL E MADEIRAS":
			nome_org = "SINDICATO DOS PROFISSIONAIS DE LACTICINIOS, ALIMENTA????O, AGRICULTURA, ESCRITORIOS, COMERCIO, SERVI??OS, TRANSPORTES RODOVIARIOS,METALOMECANICA, METALURGIA, CONSTRU??AO CIVIL E MADEIRAS"
		
		#obter id associado ao nome do sindicato
		cursor.execute("SELECT DISTINCT ID, Nome FROM Org_Sindical WHERE Nome LIKE ?", (f'%{nome_org}%',))
		id_nome = cursor.fetchone()
		if id_nome is None:
			print(file)
		else:
			id_org = id_nome[0]

		
		linha_1 = f.readline()

		#obter datas de inicio e fim de mandato
		datas = re.findall("20[0-2][0-9]-20[0-2][0-9]", linha_1)
		if len(datas) != 0:
			data_inicio_fim = datas[0].split("-")
			data_inicio = int(data_inicio_fim[0])
			data_fim = int(data_inicio_fim[1])
		else:
			anos_mandato = re.findall("MANDATO.*ANO", linha_1)
			if len(anos_mandato) != 0:
				ano_inicio = re.findall("20[0-2][0-9]", linha_1)

				if len(ano_inicio) != 0:
					data_inicio = int(ano_inicio[0])
				else:
					data_inicio = ano_boletim

				for anos in anos_mandato:
					if "QUATRO" in anos or " 4 " in anos:
						data_fim = data_inicio + 4
					elif "TR??S" in anos or " 3 " in anos:
						data_fim = data_inicio + 3
					elif "DOIS" in anos or " 2 " in anos:
						data_fim = data_inicio + 2
					elif "UM" in anos or " 1 " in anos:
						data_fim = data_inicio + 1
			

		#obter data da eleicao
		datas_eleicoes = re.findall("ELEI.*20[0-2][0-9]", linha_1)
		data_ano = re.findall("20[0-2][0-9]", linha_1)
		
		if len(datas_eleicoes) != 0:
			if len(data_ano) != 0:
				match = data_ano[0]
				for data in datas_eleicoes:
					data_eleicao = data[ : data.find(match) + len(match)]
					data_eleicao = tratarDataEleicao(data_eleicao)
					

		#obter os cargos e respetivos dirigentes

		for linha in f:
			nome_dirigente = ""
			genero_sexo = ""

			if "BOLETIM" in linha:
				
				linha = linha[ linha.find(str(ano_boletim)) + len(str(ano_boletim)) : ]
				linha = linha[ : linha.find(",")]
				nome_dirigente = linha
				
				if ":" in linha:
					valores = linha.split(":")
					cargo = valores[0]
					nome_dirigente = valores[1]

			elif ":" in linha:

				valores = linha.split(":")
				if len(valores) > 2:
					cargo = valores[-2]
					nome_dirigente = valores[-1]
				else:
					cargo = valores[0]
					nome_dirigente = valores[1]

			else:
				nome_dirigente = linha

			
			#obter nome dirigente tratado
			nome_dirigente = tratarNomeDirigente(nome_dirigente)

			#obter o genero do dirigente
			genero_sexo = obterGeneroDirigente(nome_dirigente, nomes_masculinos, nomes_femininos)

			if cargo.startswith(' '):
				cargo = cargo.replace(" ","",1)

			to_db.append((id_org, nome_dirigente, genero_sexo, cargo, data_eleicao, data_inicio, data_fim))

		f.close()

	cursor.executemany("INSERT OR IGNORE INTO Direccao_Org_Sindical(ID_Organizacao_Sindical, Nome_Pessoa, Genero_Sexo, Cargo, Data_Eleicao, Data_Inicio, Data_Fim) VALUES(?,?,?,?,?,?,?);", to_db)

	cursor.execute("""INSERT INTO Membros_Org_Sindical SELECT
		ID_Organizacao_Sindical,
		Data_Eleicao,
		Data_Inicio,
		Data_Fim,
		COUNT(*) AS Numero_Membros
		FROM Direccao_Org_Sindical GROUP BY ID_Organizacao_Sindical, Data_Eleicao, Data_Inicio, Data_Fim;""")



def repDatabase():

	connection = None
	try:
	    connection = sqlite3.connect("./rep-database.db")
	except Error as e:
	    print(e)

	cursor = connection.cursor()

	create_database_tables(cursor)


	cursor.execute("DROP VIEW IF EXISTS TEMP_DATAS_ENTIDADES;")
	cursor.execute("DROP TABLE IF EXISTS TEMP_ALTERACOES_ESTATUTOS;")
	cursor.execute("DROP TABLE IF EXISTS TEMP_ALTERACOES_ESTATUTOS1;")
	cursor.execute("DROP TABLE IF EXISTS TEMP_ELEICAO_CORPOS_GERENTES;")
	cursor.execute("DROP TABLE IF EXISTS TEMP_ELEICAO_CORPOS_GERENTES1;")
	cursor.execute("DROP TABLE IF EXISTS TEMP_ENTIDADES1;")
	cursor.execute("DROP TABLE IF EXISTS TEMP_ENTIDADES;")
	cursor.execute("DROP TABLE IF EXISTS TEMP_PROCESSOS1;")
	cursor.execute("DROP TABLE IF EXISTS TEMP_PROCESSOS;")
	cursor.execute("DROP TABLE IF EXISTS TEMP_IRCT;")
	cursor.execute("DROP TABLE IF EXISTS TEMP_OUTORGANTES;")
	cursor.execute("DROP TABLE IF EXISTS TEMP_MESES_NUMERO;")
	cursor.execute("DROP TABLE IF EXISTS TEMP_WEBSITES_SINDICAIS;")
	cursor.execute("DROP TABLE IF EXISTS TEMP_WEBSITES_PATRONAIS;")

	#create tables from importation
	cursor.execute("""CREATE TABLE TEMP_ALTERACOES_ESTATUTOS1 ( 
		TIPO INT, 
		ESPECIE INT, 
		SUB_ESPECIE INT, 
		NUMERO INT, 
		ANO INT,
		CONTROLO INT,
		SERVICO VARCHAR(6),
		CODENTG INT,
		CODENTE INT,
		NUMALT INT,
		NUMBTE INT,
		DATABTE DATE,
		SERIEBTE INT,
		AMBITO_GEOGRAFICO VARCHAR(100)
	);""")


	cursor.execute("""CREATE TABLE TEMP_ELEICAO_CORPOS_GERENTES1 (
		CODENTG INT,
		CODENTE INT,
		NUMALT INT,
		NUMERO_ELEICAO INT,
		DATA_ELEICAO DATE,
		INSCRITOS INT,
		VOTANTES INT,
		MESES_MANDATO INT,
		DATABTE DATE,
		NUMBTE INT,
		SERIEBTE INT,
		NUMMAXEFECT INT,
		NUMMINEFECT INT,
		NUMMAXSUPL INT,
		NUMMINSUPL INT,
		NUM_H_EFECT INT,
		NUM_H_SUPL INT,
		NUM_M_EFECT INT,
		NUM_M_SUPL INT,
		TIPO INT, 
		ESPECIE INT, 
		SUB_ESPECIE INT, 
		NUMERO INT, 
		ANO INT,
		CONTROLO INT,
		SERVICO VARCHAR(6)
	);""")

	cursor.execute("""CREATE TABLE TEMP_ENTIDADES1 (
		CODENTG INT,
		CODENTE INT,
		NUMALT INT,
		SIGLA VARCHAR(100),
		NOME_ENTIDADE VARCHAR(100),
		CODIGOPOSTAL_ENTIDADE VARCHAR(8),
		ID_DISTRITO INT,
		DISTRITO_DESCRICAO VARCHAR(100),
		ESTADO_ENTIDADE VARCHAR(100),
		MORADA_ENTIDADE VARCHAR(100),
		LOCAL_MORADA_ENTIDADE VARCHAR(100),
		AREA_POSTAL_ENTIDADE VARCHAR(100),
		TELEFONE_ENTIDADE VARCHAR(9),
		FAX_ENTIDADE VARCHAR(9)
	);""")

	cursor.execute("""CREATE TABLE TEMP_IRCT (
		NUMERO INT,
		NUMERO_SEQUENCIAL INT,
		ANO INT,
		TIPO_CONVENCAO_CODIGO INT,
		TIPO_CONVENCAO_DESCR VARCHAR(10),
		TIPO_CONVENCAO_DESCR_LONG VARCHAR(100),
		TIPO_CONVENCAO_ORDEM INT,
		NATUREZA_CODIGO INT,
		NATUREZA_DESCRICAO VARCHAR(100),
		NOMECC VARCHAR(100),
		AMBITO_GEOGRAFICO_IRCT VARCHAR(100),
		AMBITO_GEOGRAFICO_CODE_IRCT INT,
		NUMBTE INT,
		DATABTE DATE,
		SERIEBTE INT,
		AMBGEG VARCHAR(100),
		DATA_EFEITOS DATE,
		AREA INT,
		DIST INT,
		CONC INT,
		PROV INT,
		CODCAE VARCHAR(100),
		REVCAE VARCHAR(5)
	);""")


	cursor.execute("""CREATE TABLE TEMP_OUTORGANTES (
		NUM INT,
		NUMSEQ INT,
		ANO INT,
		TIPOCONV INT,
		CODENTG INT,
		CODENTE INT,
		NUMALT INT,
		SIGLA_ENT_E VARCHAR(100),
		NOME_ENT_E VARCHAR(100)
	);""")


	cursor.execute("""CREATE TABLE TEMP_PROCESSOS1 (
		TIPO INT, 
		ESPECIE INT, 
		SUB_ESPECIE INT, 
		NUMERO INT, 
		ANO INT,
		CONTROLO INT,
		SERVICO VARCHAR(6),
		COD_ASSUNTO INT,
		ASSUNTO VARCHAR(100),
		DESIGNACAO VARCHAR(100),
		TITULO VARCHAR(100),
		DATA_ABERTURA_PROCESSO DATE
	);""")


	#insert into tables from importation
	alteracoesEstatutos(cursor)
	eleicoesCorposGerentes(cursor)
	entidades(cursor)
	ircts(cursor)
	outorgantes(cursor)
	processos(cursor)
	websites(cursor)


	#some auxiliary tables to unite attributes
	cursor.execute("""CREATE TABLE TEMP_ALTERACOES_ESTATUTOS(
		PROCESSO VARCHAR(100),
		ID_ENTIDADE VARCHAR(10),
		NUMBTE INT,
		DATABTE DATE,
		SERIEBTE INT,
		AMBITO_GEOGRAFICO VARCHAR(100)
	);""")

	cursor.execute("""INSERT INTO TEMP_ALTERACOES_ESTATUTOS
		SELECT TIPO || '.' || ESPECIE || '.' || SUB_ESPECIE || '.' || NUMERO || '.' || ANO || '.' || CONTROLO || '-' || SERVICO,
			CODENTG || '.' || CODENTE || '.' || NUMALT,
			NUMBTE,
			DATABTE,
			SERIEBTE,
			AMBITO_GEOGRAFICO
		FROM TEMP_ALTERACOES_ESTATUTOS1;""")

	cursor.execute("""CREATE TABLE TEMP_ENTIDADES(
		ID_ENTIDADE VARCHAR(10) NOT NULL PRIMARY KEY,
		SIGLA VARCHAR(10),
		NOME_ENTIDADE VARCHAR(100),
		CODIGOPOSTAL_ENTIDADE VARCHAR(8),
		ID_DISTRITO VARCHAR(100),
		DISTRITO_DESCRICAO VARCHAR(100),
		ESTADO_ENTIDADE VARCHAR(100),
		MORADA_ENTIDADE VARCHAR(100),
		LOCAL_MORADA_ENTIDADE VARCHAR(100),
		AREA_POSTAL_ENTIDADE VARCHAR(100),
		TELEFONE_ENTIDADE VARCHAR(9),
		FAX_ENTIDADE VARCHAR(9)
	);""")

	cursor.execute("""INSERT INTO TEMP_ENTIDADES
		SELECT CODENTG || '.' || CODENTE || '.' || NUMALT,
			SIGLA,
			NOME_ENTIDADE,
			CODIGOPOSTAL_ENTIDADE,
			ID_DISTRITO,
			DISTRITO_DESCRICAO,
			ESTADO_ENTIDADE,
			MORADA_ENTIDADE,
			LOCAL_MORADA_ENTIDADE,
			AREA_POSTAL_ENTIDADE,
			TELEFONE_ENTIDADE,
			FAX_ENTIDADE
		FROM TEMP_ENTIDADES1;""")


	cursor.execute("""CREATE TABLE TEMP_ELEICAO_CORPOS_GERENTES(
		ID_ENTIDADE VARCHAR(10),
		NUMERO_ELEICAO INT,
		DATA_ELEICAO DATE,
		INSCRITOS INT,
		VOTANTES INT,
		MESES_MANDATO INT,
		DATABTE DATE,
		NUMBTE INT,
		SERIEBTE INT,
		NUMMAXEFECT INT,
		NUMMINEFECT INT,
		NUMMAXSUPL INT,
		NUMMINSUPL INT,
		NUM_H_EFECT INT,
		NUM_H_SUPL INT,
		NUM_M_EFECT INT,
		NUM_M_SUPL INT,
		PROCESSO VARCHAR(100)
	);""")


	cursor.execute("""INSERT INTO TEMP_ELEICAO_CORPOS_GERENTES
		SELECT  CODENTG || '.' || CODENTE || '.' || NUMALT,
			NUMERO_ELEICAO,
			DATA_ELEICAO,
			INSCRITOS,
			VOTANTES,
			MESES_MANDATO,
			DATABTE,
			NUMBTE,
			SERIEBTE,
			NUMMAXEFECT,
			NUMMINEFECT,
			NUMMAXSUPL,
			NUMMINSUPL,
			NUM_H_EFECT,
			NUM_H_SUPL,
			NUM_M_EFECT,
			NUM_M_SUPL,
			TIPO || '.' || ESPECIE || '.' || SUB_ESPECIE || '.' || NUMERO || '.' || ANO || '.' || CONTROLO || '-' || SERVICO
		FROM TEMP_ELEICAO_CORPOS_GERENTES1;""")


	cursor.execute("""CREATE TABLE TEMP_PROCESSOS(
		PROCESSO VARCHAR(100),
		COD_ASSUNTO INT,
		ASSUNTO VARCHAR(100),
		DESIGNACAO VARCHAR(100),
		TITULO VARCHAR(100),
		DATA_ABERTURA_PROCESSO DATE
	);""")

	cursor.execute("""INSERT INTO TEMP_PROCESSOS
		SELECT TIPO || '.' || ESPECIE || '.' || SUB_ESPECIE || '.' || NUMERO || '.' || ANO || '.' || CONTROLO || '-' || SERVICO,
			COD_ASSUNTO,
			ASSUNTO,
			DESIGNACAO,
			TITULO,
			DATA_ABERTURA_PROCESSO
		FROM TEMP_PROCESSOS1;""")



	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = trim(replace(replace(replace(NOME_ENTIDADE, X'0A', ' '),'  ',' '),'  ',' '));")
	cursor.execute("UPDATE TEMP_ENTIDADES SET SIGLA = trim(replace(replace(replace(SIGLA, X'0A', ' '),'  ',' '),'  ',' '));")

	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE, 'CGTP-IN', 'CGTPIN') WHERE instr(NOME_ENTIDADE, 'CGTP-IN') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(SIGLA, 'CGTP-IN', 'CGTPIN') WHERE instr(SIGLA, 'CGTP-IN') > 0;")

	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,' . ','. ')  WHERE instr(NOME_ENTIDADE, ' . ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,' NAC. ',' NACIONAL ')  WHERE instr(NOME_ENTIDADE, ' NAC. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,' TRAB. ',' TRABALHADORES ')  WHERE instr(NOME_ENTIDADE, ' TRAB. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,' DO DIST. D',' DO DISTRITO D')  WHERE instr(NOME_ENTIDADE, ' DO DIST. D') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,' DOS DIST. ',' DOS DISTRITOS ')  WHERE instr(NOME_ENTIDADE, ' DOS DIST. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,' DA IND. ',' DA IND??STRIA ')  WHERE instr(NOME_ENTIDADE, ' DA IND. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,' DAS IND. ',' DAS IND??STRIAS ')  WHERE instr(NOME_ENTIDADE, ' DAS IND. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,' DOS IND. ',' DOS INDUSTRIAIS ')  WHERE instr(NOME_ENTIDADE, ' DOS IND. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,' NA IND. ',' NA IND??STRIA ')  WHERE instr(NOME_ENTIDADE, ' NA IND. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,' NA IND, ',' DA IND??STRIA ')  WHERE instr(NOME_ENTIDADE, ' NA IND, ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,' NAS IND. ',' NAS IND??STRIAS ')  WHERE instr(NOME_ENTIDADE, ' NAS IND. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,' IND. E ',' IND??STRIA E')  WHERE instr(NOME_ENTIDADE, ' IND. E ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,' IND. ',' IND??STRIAS ')  WHERE instr(NOME_ENTIDADE, ' IND. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,' IND.',' IND??STRIAS ')  WHERE instr(NOME_ENTIDADE, ' IND.') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'(IND. ','(IND??STRIA ')  WHERE instr(NOME_ENTIDADE, '(IND. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,' DOS SIND. ',' DOS SINDICATOS ')  WHERE instr(NOME_ENTIDADE, ' DOS SIND. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'SIND. ','SINDICATO ')  WHERE instr(NOME_ENTIDADE, 'SIND. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'ASS. ','ASSOCIA????O ')  WHERE instr(NOME_ENTIDADE, 'ASS. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'ASSOC. ','ASSOCIA????O ')  WHERE instr(NOME_ENTIDADE, 'ASSOC. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'ASSOC ','ASSOCIA????O ')  WHERE instr(NOME_ENTIDADE, 'ASSOC ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'ASSOCIA??AO ','ASSOCIA????O ')  WHERE instr(NOME_ENTIDADE, 'ASSOCIA??AO ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'ASSOCIACAO ','ASSOCIA????O ')  WHERE instr(NOME_ENTIDADE, 'ASSOCIACAO ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'ASS.COMERCIAL','ASSOCIA????O COMERCIAL')  WHERE instr(NOME_ENTIDADE, 'ASS.COMERCIAL') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'ASS.NACIONAL','ASSOCIA????O NACIONAL')  WHERE instr(NOME_ENTIDADE, 'ASS.NACIONAL') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'FED. ','FEDERA????O ')  WHERE instr(NOME_ENTIDADE, 'FED. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'FEDER. ','FEDERA????O ')  WHERE instr(NOME_ENTIDADE, 'FEDER. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'FEDERECAO ','FEDERA????O ')  WHERE instr(NOME_ENTIDADE, 'FEDERA??AO ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'FEDER NACIONAL','FEDERA????O NACIONAL')  WHERE instr(NOME_ENTIDADE, 'FEDER NACIONAL') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'TRTABALHADORES','TRABALHADORES')  WHERE instr(NOME_ENTIDADE, 'TRTABALHADORES') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'ASSOC.DOS ','ASSOCIA????O DOS ')  WHERE instr(NOME_ENTIDADE, 'ASSOC.DOS ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'ESCRIT. ','ESCRIT??RIO ')  WHERE instr(NOME_ENTIDADE, 'ESCRIT. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'IMP.E EXP. ','IMPORT E EXPORT ')  WHERE instr(NOME_ENTIDADE, 'IMP.E EXP. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'DEMOC. ','DEMOCR??TICO ')  WHERE instr(NOME_ENTIDADE, 'DEMOC. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'DIST. ','DISTRITO ')  WHERE instr(NOME_ENTIDADE, 'DIST. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'TRANSP. ','TRANSPORTES ')  WHERE instr(NOME_ENTIDADE, 'TRANSP. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'DOS AGRICULT. ','DOS AGRICULTORES ')  WHERE instr(NOME_ENTIDADE, 'DOS AGRICULT. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'SOC. ','SOCIEDADE ')  WHERE instr(NOME_ENTIDADE, 'SOC. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'COOP. ','COOPERATIVA ')  WHERE instr(NOME_ENTIDADE, 'COOP. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'IMPORT. ','IMPORTA????O ')  WHERE instr(NOME_ENTIDADE, 'IMPORT. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'AS PORT. ','AS PORTUGUESAS ')  WHERE instr(NOME_ENTIDADE, 'AS PORT. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'PORT. ','PORTUGUESA ')  WHERE instr(NOME_ENTIDADE, 'PORT. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,' S. A. ','S.A. ')  WHERE instr(NOME_ENTIDADE, 'S.A. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,' SA ',' S.A. ')  WHERE instr(NOME_ENTIDADE, ' SA ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,' S. A.','S.A.')  WHERE instr(NOME_ENTIDADE, 'S. A.') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,',S.A.',', S.A.')  WHERE instr(NOME_ENTIDADE, ',S.A.') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,',I.P.',', I.P.')  WHERE instr(NOME_ENTIDADE, ',I.P.') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'DOS TRANSPORTAD. ','DOS TRANSPORTADORES ')  WHERE instr(NOME_ENTIDADE, 'DOS TRANSPORTAD. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'DAS TRANSPORTAD. ','DAS TRANSPORTADORAS ')  WHERE instr(NOME_ENTIDADE, 'DAS TRANSPORTAD. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'INT. ','INTERNACIONAL ')  WHERE instr(NOME_ENTIDADE, 'INT. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'LAB.FARM.,','LABORAT??RIOS FARMAC??UTICOS, ')  WHERE instr(NOME_ENTIDADE, 'LAB.FARM.,') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'DE PROD. DE ','DE PRODUTORES DE ')  WHERE instr(NOME_ENTIDADE, 'DE PROD. DE ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'DE PROD. QU','DE PRODUTOS QU')  WHERE instr(NOME_ENTIDADE, 'DE PROD. QU') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'PROD. E C','PRODU????O E C')  WHERE instr(NOME_ENTIDADE, 'PROD. E COM') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,' DIS. ',' DISTRITO ')  WHERE instr(NOME_ENTIDADE, ' DIS. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'SERV. ','SERVI??OS ')  WHERE instr(NOME_ENTIDADE, 'SERV. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'REG. ','REGIONAL ')  WHERE instr(NOME_ENTIDADE, 'REG. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'TRABALHADORES PORTUG.','TRABALHADORES PORTUGUESES')  WHERE instr(NOME_ENTIDADE, 'TRABALHADORES PORTUG.') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'SEG. ','SEGURAN??A ')  WHERE instr(NOME_ENTIDADE, 'SEG. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'DOS COM. ','DOS COMERCIANTES ')  WHERE instr(NOME_ENTIDADE, 'DOS COM. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'SOCIEDADE COM. ','SOCIEDADE COMERCIAL ')  WHERE instr(NOME_ENTIDADE, 'SOCIEDADE COM. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'TRABALHADORES AGRI. ','TRABALHADORES AGR??COLAS ')  WHERE instr(NOME_ENTIDADE, 'TRABALHADORES AGRI. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'DOS COMER. ','DOS COMERCIANTES ')  WHERE instr(NOME_ENTIDADE, 'DOS COMER. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'DOS PROF.','DOS PROFISSIONAIS')  WHERE instr(NOME_ENTIDADE, 'DOS PROF.') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'CONST. CIV.','CONSTRU????O CIVIL')  WHERE instr(NOME_ENTIDADE, 'CONST. CIV.') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'TRANSP.RODOVIARIOS,','TRANSPORTES RODOVIARIOS, ')  WHERE instr(NOME_ENTIDADE, 'TRANSP.RODOVIARIOS,') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'AGRICULT, ESCRIT, COMER, SERV,','AGRICULTURA, ESCRIT??RIO, COM??RCIO, SERVI??OS,')  WHERE instr(NOME_ENTIDADE, 'AGRICULT, ESCRIT, COMER, SERV,') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'METALOMEC, METAL,','METALOMEC??NICA, METALURGIA,')  WHERE instr(NOME_ENTIDADE, 'METALOMEC, METAL,') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'DO CONC. ','DO CONCELHO ')  WHERE instr(NOME_ENTIDADE, 'DO CONC. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'DOS CONC. ','DOS CONCELHOS ')  WHERE instr(NOME_ENTIDADE, 'DOS CONC. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'MUNIC??PIOS. ','MUNIC??PIOS ')  WHERE instr(NOME_ENTIDADE, 'MUNIC??PIOS. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'DOS IND??STRIA DE CONST. ','DOS INDUSTRIAIS DE CONSTRU????O ')  WHERE instr(NOME_ENTIDADE, 'DOS IND??STRIA DE CONST. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'DOS IND??STRIA REFINAD. ','DOS INDUSTRIAIS REFINADORES ')  WHERE instr(NOME_ENTIDADE, 'DOS IND??STRIA REFINAD. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'DOS DISTRIT. ','DOS DISTRITOS ')  WHERE instr(NOME_ENTIDADE, 'DOS DISTRIT. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'TECNOLOG. ','TECNOLOGIAS ')  WHERE instr(NOME_ENTIDADE, 'TECNOLOG. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'INST. ','INSTITUTO ')  WHERE instr(NOME_ENTIDADE, 'INST. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'ASS ','ASSOCIA????O ')  WHERE instr(NOME_ENTIDADE, 'ASS ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'ASS.OCIA????O','ASSOCIA????O ')  WHERE instr(NOME_ENTIDADE, 'ASS.OCIA????O') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'E IMP. ','E IMPORTADORES ')  WHERE instr(NOME_ENTIDADE, 'E IMP. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'CONF. ','CONFEDERA????O ')  WHERE instr(NOME_ENTIDADE, 'CONF. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'DOS COMERC. ','DOS COMERCIANTES ')  WHERE instr(NOME_ENTIDADE, 'DOS COMERC. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'IND??STRIA EXPORTADORES ','IND??STRIA, EXPORTADORES ')  WHERE instr(NOME_ENTIDADE, 'IND??STRIA EXPORTADORES ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'DOS IND??STRIA ','DOS INDUSTRIAIS ')  WHERE instr(NOME_ENTIDADE, 'DOS IND??STRIA ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'REP. ','REPARA????O ')  WHERE instr(NOME_ENTIDADE, 'REP. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'COM. ','COM??RCIO ')  WHERE instr(NOME_ENTIDADE, 'COM. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'COMP. ','COMPANHIA ')  WHERE instr(NOME_ENTIDADE, 'COMP. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,' LDX',' LDA')  WHERE instr(NOME_ENTIDADE, ' LDX') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,',LDX',', LDA')  WHERE instr(NOME_ENTIDADE, ',LDX') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,',LD.X',', LDA')  WHERE instr(NOME_ENTIDADE, ',LD.X') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,',LDA',', LDA')  WHERE instr(NOME_ENTIDADE, ',LDA') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,' LDA',' LDA')  WHERE instr(NOME_ENTIDADE, ' LDA') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,' LDA..',' LDA')  WHERE instr(NOME_ENTIDADE, ' LDA..') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'CX. ','COMPANHIA ')  WHERE instr(NOME_ENTIDADE, 'CX. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,', SA',', S.A.')  WHERE instr(NOME_ENTIDADE, ', SA') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,', S.A.RL',', SARL')  WHERE instr(NOME_ENTIDADE, ', S.A.RL') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,', S.A.N',', SAN')  WHERE instr(NOME_ENTIDADE, ', S.A.N') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'METAL., E PROJ.IND.','METAL??RGICA, E PROJECTOS INDUSTRIAIS')  WHERE instr(NOME_ENTIDADE, 'METAL., E PROJ.IND.') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'DUSTRIES SA','DUSTRIES, S.A.')  WHERE instr(NOME_ENTIDADE, 'DUSTRIES SA') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'FIGUEIRA SA','FIGUEIRA, S.A.')  WHERE instr(NOME_ENTIDADE, 'FIGUEIRA SA') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'PARVALOREM SA','PARVALOREM, S.A.')  WHERE instr(NOME_ENTIDADE, 'PARVALOREM SA') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'PORTUGAL) SA','PORTUGAL), S.A.')  WHERE instr(NOME_ENTIDADE, 'PORTUGAL) SA') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'PAPEL SA','PAPEL, S.A.')  WHERE instr(NOME_ENTIDADE, 'PAPEL SA') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'ASSIST??NCIA SA','ASSIST??NCIA, S.A.')  WHERE instr(NOME_ENTIDADE, 'ASSISTXNCIA SA') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,', EPE',', E.P.E.')  WHERE instr(NOME_ENTIDADE, ', EPE') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,' S. ',' SXO ')  WHERE instr(NOME_ENTIDADE, ' S. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,' ST. ',' SANTO ')  WHERE instr(NOME_ENTIDADE, ' ST. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'E.U.A.','ESTADOS UNIDOS DA AM??RICA')  WHERE instr(NOME_ENTIDADE, 'E.U.A.') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'F??B. ','F??BRICA ')  WHERE instr(NOME_ENTIDADE, 'F??B. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'NOT. ','NOT??CIAS ')  WHERE instr(NOME_ENTIDADE, 'NOT. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'G.E. ','GENERAL ELECTRIC ')  WHERE instr(NOME_ENTIDADE, 'G.E. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'R. AUT','REGI??ES AUT')  WHERE instr(NOME_ENTIDADE, 'R. AUT') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'GAB. ','GABINETE ')  WHERE instr(NOME_ENTIDADE, 'GAB. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,' R. AUT??NOMAS ',' REGI??ES AUT??NOMAS ')  WHERE instr(NOME_ENTIDADE, ' R. AUT??NOMAS ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'PORTUG. ','PORTUGUESA ')  WHERE instr(NOME_ENTIDADE, 'PORTUG. ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = SUBSTR(NOME_ENTIDADE,3) WHERE NOME_ENTIDADE LIKE 'O %' AND instr(NOME_ENTIDADE,'ALMADA') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'.','')  WHERE NOME_ENTIDADE LIKE '%.';")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'METALOM.','METALOMEC??NICA')  WHERE instr(NOME_ENTIDADE, 'METALOM.') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'FISCALIA????O.','FISCALIZA????O ')  WHERE instr(NOME_ENTIDADE, 'FISCALIA????O.') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'TEXTIL','T??XTIL')  WHERE instr(NOME_ENTIDADE, 'TEXTIL') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'INDUSTRIAS','IND??STRIAS')  WHERE instr(NOME_ENTIDADE, 'INDUSTRIAS') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'INDUSTRIA','IND??STRIA')  WHERE instr(NOME_ENTIDADE, 'INDUSTRIA') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'IND??STRIAL','INDUSTRIAL')  WHERE instr(NOME_ENTIDADE, 'IND??STRIAL') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'CERAMICA','CER??MICA')  WHERE instr(NOME_ENTIDADE, 'CERAMICA') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'FERROVIARIOS','FERROVI??RIOS')  WHERE instr(NOME_ENTIDADE, 'FERROVIARIOS ') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'AGUA','??GUA')  WHERE instr(NOME_ENTIDADE, 'AGUA') > 0;")
	cursor.execute("UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = replace(NOME_ENTIDADE,'??GUA','??GUA')  WHERE instr(NOME_ENTIDADE, '??GUA') > 0;")


	cursor.execute("""UPDATE TEMP_ENTIDADES SET SIGLA = TRIM(SUBSTR(NOME_ENTIDADE,0,instr(NOME_ENTIDADE,'-'))) 
	WHERE (SIGLA IS NULL OR TRIM(SIGLA)='') AND instr(NOME_ENTIDADE, '-') > 0 AND instr(TRIM(SUBSTR(NOME_ENTIDADE,0,instr(NOME_ENTIDADE,'-'))),' ') <=0;""")

	cursor.execute("""UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = TRIM(SUBSTR(NOME_ENTIDADE,instr(NOME_ENTIDADE,'-') + 1, LENGTH(NOME_ENTIDADE) - instr(NOME_ENTIDADE,'-') + 1)) 
	WHERE instr(NOME_ENTIDADE, '-') > 0 AND instr(TRIM(SUBSTR(NOME_ENTIDADE,0,instr(NOME_ENTIDADE,'-'))),' ') <=0
	AND (TRIM(SIGLA)='' OR TRIM(SIGLA)=TRIM(SUBSTR(NOME_ENTIDADE,0,instr(NOME_ENTIDADE,'-'))));""")

	cursor.execute("""UPDATE TEMP_ENTIDADES SET SIGLA = TRIM(SUBSTR(NOME_ENTIDADE,instr(NOME_ENTIDADE,'-') + 1, LENGTH(NOME_ENTIDADE) - instr(NOME_ENTIDADE,'-') + 1)) 
	WHERE (SIGLA IS NULL OR TRIM(SIGLA)='') AND instr(NOME_ENTIDADE, '-') > 0 AND instr(TRIM(SUBSTR(NOME_ENTIDADE,instr(NOME_ENTIDADE,'-') + 1, LENGTH(NOME_ENTIDADE) - instr(NOME_ENTIDADE,'-') + 1)),' ') <=0;""")

	cursor.execute("""UPDATE TEMP_ENTIDADES SET NOME_ENTIDADE = TRIM(SUBSTR(NOME_ENTIDADE,0,instr(NOME_ENTIDADE,'-')))
	WHERE instr(NOME_ENTIDADE, '-') > 0 AND instr(TRIM(SUBSTR(NOME_ENTIDADE,instr(NOME_ENTIDADE,'-') + 1, LENGTH(NOME_ENTIDADE) - instr(NOME_ENTIDADE,'-') + 1)) , ' ') <=0
	AND (TRIM(SIGLA)='' OR TRIM(SIGLA)=TRIM(SUBSTR(NOME_ENTIDADE,instr(NOME_ENTIDADE,'-') + 1, LENGTH(NOME_ENTIDADE) - instr(NOME_ENTIDADE,'-') + 1)));""")


	cursor.execute("""CREATE VIEW TEMP_DATAS_ENTIDADES AS SELECT ID_ENTIDADE, MIN(DATA) AS MIN_DATA, MAX(DATA) AS MAX_DATA FROM (
		SELECT ID_ENTIDADE, date(replace(DATABTE,'.','-')) AS DATA FROM TEMP_ALTERACOES_ESTATUTOS
		UNION
		SELECT ID_ENTIDADE, date(replace(DATABTE,'.','-')) AS DATA FROM TEMP_PROCESSOS LEFT OUTER JOIN TEMP_ELEICAO_CORPOS_GERENTES ON TEMP_PROCESSOS.PROCESSO=TEMP_ELEICAO_CORPOS_GERENTES.PROCESSO
		UNION
		SELECT ID_ENTIDADE, date(replace(DATA_ELEICAO,'.','-')) AS DATA FROM TEMP_PROCESSOS LEFT OUTER JOIN TEMP_ELEICAO_CORPOS_GERENTES ON TEMP_PROCESSOS.PROCESSO=TEMP_ELEICAO_CORPOS_GERENTES.PROCESSO
	) GROUP BY ID_ENTIDADE;""")


	cursor.execute("""INSERT INTO Org_Patronal 
	SELECT TEMP_ENTIDADES.ID_ENTIDADE, NULL,
				 CASE trim(NOME_ENTIDADE) WHEN '' THEN trim(SIGLA) ELSE trim(NOME_ENTIDADE) END,
				 CASE trim(SIGLA) WHEN '' THEN NULL ELSE trim(SIGLA) END,
				 NULL, 
				 NULL,
				 REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(trim(DISTRITO_DESCRICAO),'DISTRITO DO ',''),'DIST. DO ',''),'DISTRITO DE ',''),'DIST. DE ',''),'DISTRITO DA ',''),'DIST. DA ',''),
				 trim(CODIGOPOSTAL_ENTIDADE),
				 MORADA_ENTIDADE,
				 LOCAL_MORADA_ENTIDADE,
				 AREA_POSTAL_ENTIDADE,
		 	     TELEFONE_ENTIDADE,
		         FAX_ENTIDADE, 
				 NULL, 
				 NULL, 
				 NULL, 
				 MIN_DATA,
				 MAX_DATA,
				 CASE lower(trim(ESTADO_ENTIDADE)) WHEN 'ativa' THEN 1 ELSE 0 END,
				 NULL
	FROM TEMP_ENTIDADES LEFT JOIN TEMP_DATAS_ENTIDADES ON TEMP_ENTIDADES.ID_ENTIDADE=TEMP_DATAS_ENTIDADES.ID_ENTIDADE WHERE CAST(SUBSTR(TEMP_ENTIDADES.ID_ENTIDADE,0,INSTR(TEMP_ENTIDADES.ID_ENTIDADE, '.')) AS INT) > 4;""")

	cursor.execute("""UPDATE Org_Patronal SET TIPO = CASE 
			 WHEN CAST(SUBSTR(ID,0,INSTR(ID, '.')) AS INT) = 1 THEN 'SINDICATO'
			 WHEN CAST(SUBSTR(ID,0,INSTR(ID, '.')) AS INT) = 2 THEN 'FEDERA????O SINDICAL'
			 WHEN CAST(SUBSTR(ID,0,INSTR(ID, '.')) AS INT) = 3 THEN 'UNI??O SINDICAL'
			 WHEN CAST(SUBSTR(ID,0,INSTR(ID, '.')) AS INT) = 4 THEN 'CONFEDERA????O SINDICAL'
			 WHEN CAST(SUBSTR(ID,0,INSTR(ID, '.')) AS INT) = 5 THEN 'ASSOCIA????O DE EMPREGADORES'
			 WHEN CAST(SUBSTR(ID,0,INSTR(ID, '.')) AS INT) = 6 THEN 'FEDERA????O DE EMPREGADORES'
			 WHEN CAST(SUBSTR(ID,0,INSTR(ID, '.')) AS INT) = 7 THEN 'UNI??O DE EMPREGADORES'
			 WHEN CAST(SUBSTR(ID,0,INSTR(ID, '.')) AS INT) = 8 THEN 'CONFEDERA????O DE EMPREGADORES'
	END;""")

	cursor.execute("""UPDATE Org_Patronal SET Ambito_Geografico = ( 
	 SELECT DISTINCT AMBITO_GEOGRAFICO FROM TEMP_ENTIDADES NATURAL JOIN (SELECT DISTINCT ID_ENTIDADE, AMBITO_GEOGRAFICO FROM TEMP_ALTERACOES_ESTATUTOS WHERE trim(AMBITO_GEOGRAFICO) <> '')
	 WHERE ID_ENTIDADE = ID
	) WHERE ID IN (
	 SELECT DISTINCT ID_ENTIDADE
	 FROM TEMP_ENTIDADES NATURAL JOIN (SELECT DISTINCT ID_ENTIDADE, AMBITO_GEOGRAFICO FROM TEMP_ALTERACOES_ESTATUTOS WHERE trim(AMBITO_GEOGRAFICO) <> '')
	 GROUP BY ID_ENTIDADE
	 HAVING COUNT(DISTINCT AMBITO_GEOGRAFICO) = 1
	);""")


	cursor.execute("UPDATE Org_Patronal SET Website = (SELECT wp.WEBSITE FROM TEMP_WEBSITES_PATRONAIS wp WHERE Org_Patronal.Nome = wp.NOME_ORG);")



	cursor.execute("""INSERT INTO Org_Sindical 
	SELECT TEMP_ENTIDADES.ID_ENTIDADE, NULL,
				 CASE trim(NOME_ENTIDADE) WHEN '' THEN trim(SIGLA) ELSE trim(NOME_ENTIDADE) END,
				 CASE trim(SIGLA) WHEN '' THEN NULL ELSE trim(SIGLA) END, 
				 NULL,
				 NULL,
				 REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(trim(DISTRITO_DESCRICAO),'DISTRITO DO ',''),'DIST. DO ',''),'DISTRITO DE ',''),'DIST. DE ',''),'DISTRITO DA ',''),'DIST. DA ',''),
				 trim(CODIGOPOSTAL_ENTIDADE),
				 MORADA_ENTIDADE,
				 LOCAL_MORADA_ENTIDADE,
				 AREA_POSTAL_ENTIDADE,
		 	     TELEFONE_ENTIDADE,
		         FAX_ENTIDADE,   
				 NULL, 
				 NULL, 
				 NULL, 
				 MIN_DATA,
				 MAX_DATA,
				 CASE lower(trim(ESTADO_ENTIDADE)) WHEN 'ativa' THEN 1 ELSE 0 END,
				 NULL
	FROM TEMP_ENTIDADES LEFT JOIN TEMP_DATAS_ENTIDADES ON TEMP_ENTIDADES.ID_ENTIDADE=TEMP_DATAS_ENTIDADES.ID_ENTIDADE WHERE CAST(SUBSTR(TEMP_ENTIDADES.ID_ENTIDADE,0,INSTR(TEMP_ENTIDADES.ID_ENTIDADE, '.')) AS INT) < 5;""")


	#chamar aqui a fun????o

	cursor.execute("""UPDATE Org_Sindical SET TIPO = CASE 
			 WHEN CAST(SUBSTR(ID,0,INSTR(ID, '.')) AS INT) = 1 THEN 'SINDICATO'
			 WHEN CAST(SUBSTR(ID,0,INSTR(ID, '.')) AS INT) = 2 THEN 'FEDERA????O SINDICAL'
			 WHEN CAST(SUBSTR(ID,0,INSTR(ID, '.')) AS INT) = 3 THEN 'UNI??O SINDICAL'
			 WHEN CAST(SUBSTR(ID,0,INSTR(ID, '.')) AS INT) = 4 THEN 'CONFEDERA????O SINDICAL'
			 WHEN CAST(SUBSTR(ID,0,INSTR(ID, '.')) AS INT) = 5 THEN 'ASSOCIA????O PATRONAL'
			 WHEN CAST(SUBSTR(ID,0,INSTR(ID, '.')) AS INT) = 6 THEN 'FEDERA????O PATRONAL'
			 WHEN CAST(SUBSTR(ID,0,INSTR(ID, '.')) AS INT) = 7 THEN 'UNI??O PATRONAL'
			 WHEN CAST(SUBSTR(ID,0,INSTR(ID, '.')) AS INT) = 8 THEN 'CONFEDERA????O PATRONAL'
	END;""")

	cursor.execute("""UPDATE Org_Sindical SET Ambito_Geografico = ( 
	 SELECT DISTINCT AMBITO_GEOGRAFICO FROM TEMP_ENTIDADES NATURAL JOIN (SELECT DISTINCT ID_ENTIDADE, AMBITO_GEOGRAFICO FROM TEMP_ALTERACOES_ESTATUTOS WHERE trim(AMBITO_GEOGRAFICO) <> '')
	 WHERE ID_ENTIDADE = ID
	) WHERE ID IN (
	 SELECT DISTINCT ID_ENTIDADE
	 FROM TEMP_ENTIDADES NATURAL JOIN (SELECT DISTINCT ID_ENTIDADE, AMBITO_GEOGRAFICO FROM TEMP_ALTERACOES_ESTATUTOS WHERE trim(AMBITO_GEOGRAFICO) <> '')
	 GROUP BY ID_ENTIDADE
	 HAVING COUNT(DISTINCT AMBITO_GEOGRAFICO) = 1
	);""")

	cursor.execute("""UPDATE ORG_SINDICAL SET Nome_Organizacao_Pai="CONFEDERA????O GERAL DOS TRABALHADORES PORTUGUESES - INTERSINDICAL NACIONAL" WHERE instr(ACRONIMO,'/CGTP') > 0 OR instr(ACRONIMO,' CGTPIN') > 0;""")
	cursor.execute("""UPDATE ORG_SINDICAL SET Nome_Organizacao_Pai="UNI??O GERAL DE TRABALHADORES" WHERE instr(ACRONIMO,'UGT/') > 0 OR instr(ACRONIMO,'/UGT') > 0 OR instr(ACRONIMO,' UGT') > 0;""")

	cursor.execute("""INSERT INTO Relacoes_Entre_Org_Sindical
	SELECT A.ID AS ID_Organizacao_Sindical_1, B.ID AS ID_Organizacao_Sindical_2, 'CONTIDO EM', NULL
	FROM ORG_SINDICAL AS A, ORG_SINDICAL AS B
	WHERE A.Nome_Organizacao_Pai IS NOT NULL AND B.NOME = A.Nome_Organizacao_Pai;""")

	cursor.execute("""INSERT INTO Relacoes_Entre_Org_Sindical
	SELECT B.ID AS ID_Organizacao_Sindical_1, A.ID AS ID_Organizacao_Sindical_2, 'CONTEM', NULL
	FROM ORG_SINDICAL AS A, ORG_SINDICAL AS B
	WHERE A.Nome_Organizacao_Pai IS NOT NULL AND B.NOME = A.Nome_Organizacao_Pai;""")


	cursor.execute("UPDATE ORG_SINDICAL SET ACRONIMO=replace(ACRONIMO, ' CGTPIN', '') WHERE instr(ACRONIMO, ' CGTPIN') > 0;")
	cursor.execute("UPDATE ORG_SINDICAL SET ACRONIMO=replace(ACRONIMO, '/CGTPIN', '') WHERE instr(ACRONIMO, '/CGTPIN') > 0;")
	cursor.execute("UPDATE ORG_SINDICAL SET ACRONIMO=replace(ACRONIMO, '/CGTP-I', '') WHERE instr(ACRONIMO, '/CGTP-I') > 0;")
	cursor.execute("UPDATE ORG_SINDICAL SET ACRONIMO=replace(ACRONIMO, '/UGT', '') WHERE instr(ACRONIMO, '/UGT') > 0;")
	cursor.execute("UPDATE ORG_SINDICAL SET ACRONIMO=replace(ACRONIMO, 'UGT/', '') WHERE instr(ACRONIMO, 'UGT/') > 0;")
	cursor.execute("UPDATE ORG_SINDICAL SET ACRONIMO=replace(replace(ACRONIMO, '(', ''), ')', '') WHERE instr(ACRONIMO, '(') > 0 OR instr(ACRONIMO, ')') > 0;")
	cursor.execute("UPDATE ORG_SINDICAL SET ACRONIMO=replace(ACRONIMO, '.', '') WHERE instr(ACRONIMO, '.') > 0;")
	cursor.execute("UPDATE ORG_SINDICAL SET ACRONIMO=replace(ACRONIMO, ' - ', '-') WHERE instr(ACRONIMO, ' - ') > 0;")
	cursor.execute("""UPDATE ORG_SINDICAL SET ACRONIMO="STMTM" WHERE Acronimo = "OS-MONTES";""")
	cursor.execute("""UPDATE ORG_SINDICAL SET Nome="SINDICATO T??XTIL DO MINHO E TR??S-OS-MONTES" WHERE Acronimo = "STMTM";""")

	cursor.execute("UPDATE Org_Sindical SET Website = (SELECT ws.WEBSITE FROM TEMP_WEBSITES_SINDICAIS ws WHERE Org_Sindical.Nome = ws.NOME_ORG);")

	cursor.execute("""INSERT INTO Mencoes_BTE_Org_Sindical
	SELECT DISTINCT TEMP_ENTIDADES.ID_ENTIDADE, NULL,
				 strftime('%Y',date(replace(DATABTE,'.','-'))) AS Ano,
				 NUMBTE AS Numero,
				 SERIEBTE AS Serie,
				 NULL AS Descricao,
				 0 AS Mudanca_Estatuto,
				 0 AS Eleicoes,
				 1 AS Confianca
	FROM TEMP_ENTIDADES NATURAL JOIN TEMP_ALTERACOES_ESTATUTOS WHERE CAST(SUBSTR(TEMP_ENTIDADES.ID_ENTIDADE,0,INSTR(TEMP_ENTIDADES.ID_ENTIDADE, '.')) AS INT) < 5
	UNION
	SELECT DISTINCT TEMP_ENTIDADES.ID_ENTIDADE, NULL,
				 strftime('%Y',date(replace(DATABTE,'.','-'))) AS Ano,
				 NUMBTE AS Numero,
				 SERIEBTE AS Serie,
				 NULL AS Descricao,
				 0 AS Mudanca_Estatuto,
				 0 AS Eleicoes,
				 1 AS Confianca
	FROM TEMP_ENTIDADES NATURAL JOIN TEMP_ELEICAO_CORPOS_GERENTES WHERE CAST(SUBSTR(TEMP_ENTIDADES.ID_ENTIDADE,0,INSTR(TEMP_ENTIDADES.ID_ENTIDADE, '.')) AS INT) < 5;""")

	cursor.execute("""UPDATE Mencoes_BTE_Org_Sindical SET Mudanca_Estatuto = 1 WHERE ID_Organizacao_Sindical || ' - ' || Ano || ' - ' || Numero || ' - ' || Serie IN (
		SELECT TEMP_ENTIDADES.ID_ENTIDADE || ' - ' || strftime('%Y',date(replace(DATABTE,'.','-'))) || ' - ' || NUMBTE || ' - ' || SERIEBTE 
		FROM TEMP_ENTIDADES NATURAL JOIN TEMP_ALTERACOES_ESTATUTOS 
	);""")

	cursor.execute("""UPDATE Mencoes_BTE_Org_Sindical SET Eleicoes = 1 WHERE ID_Organizacao_Sindical || ' - ' || Ano || ' - ' || Numero || ' - ' || Serie IN (
		SELECT TEMP_ENTIDADES.ID_ENTIDADE || ' - ' || strftime('%Y',date(replace(DATABTE,'.','-'))) || ' - ' || NUMBTE || ' - ' || SERIEBTE 
		FROM TEMP_ENTIDADES NATURAL JOIN TEMP_ELEICAO_CORPOS_GERENTES
	);""")


	cursor.execute("""INSERT INTO Mencoes_BTE_Org_Patronal
	SELECT DISTINCT TEMP_ENTIDADES.ID_ENTIDADE, NULL,
				 strftime('%Y',date(replace(DATABTE,'.','-'))) AS Ano,
				 NUMBTE AS Numero,
				 SERIEBTE AS Serie,
				 NULL AS Descricao, 
				 0 AS Mudanca_Estatuto,
				 0 AS Eleicoes,
				 1 AS Confianca
	FROM TEMP_ENTIDADES NATURAL JOIN TEMP_ALTERACOES_ESTATUTOS WHERE CAST(SUBSTR(TEMP_ENTIDADES.ID_ENTIDADE,0,INSTR(TEMP_ENTIDADES.ID_ENTIDADE, '.')) AS INT) > 4
	UNION
	SELECT DISTINCT TEMP_ENTIDADES.ID_ENTIDADE, NULL,
				 strftime('%Y',date(replace(DATABTE,'.','-'))) AS Ano,
				 NUMBTE AS Numero,
				 SERIEBTE AS Serie,
				 NULL AS Descricao, 
				 0 AS Mudanca_Estatuto,
				 0 AS Eleicoes,
				 1 AS Confianca
	FROM TEMP_ENTIDADES NATURAL JOIN TEMP_ELEICAO_CORPOS_GERENTES WHERE CAST(SUBSTR(TEMP_ENTIDADES.ID_ENTIDADE,0,INSTR(TEMP_ENTIDADES.ID_ENTIDADE, '.')) AS INT) > 4;""")


	cursor.execute("""UPDATE Mencoes_BTE_Org_Patronal SET Mudanca_Estatuto = 1 WHERE ID_Organizacao_Patronal || ' - ' || Ano || ' - ' || Numero || ' - ' || Serie IN (
		SELECT TEMP_ENTIDADES.ID_ENTIDADE || ' - ' || strftime('%Y',date(replace(DATABTE,'.','-'))) || ' - ' || NUMBTE || ' - ' || SERIEBTE 
		FROM TEMP_ENTIDADES NATURAL JOIN TEMP_ALTERACOES_ESTATUTOS 
	);""")

	cursor.execute("""UPDATE Mencoes_BTE_Org_Patronal SET Eleicoes = 1 WHERE ID_Organizacao_Patronal || ' - ' || Ano || ' - ' || Numero || ' - ' || Serie IN (
		SELECT TEMP_ENTIDADES.ID_ENTIDADE || ' - ' || strftime('%Y',date(replace(DATABTE,'.','-'))) || ' - ' || NUMBTE || ' - ' || SERIEBTE 
		FROM TEMP_ENTIDADES NATURAL JOIN TEMP_ELEICAO_CORPOS_GERENTES
	);""")

	cursor.execute("""INSERT INTO Actos_Eleitorais_Org_Sindical
	SELECT TEMP_ENTIDADES.ID_ENTIDADE AS ID_Organizacao_Sindical,
				 date(replace(DATA_ELEICAO,'.','-')) AS Data,
				 MAX(NUM_H_EFECT + NUM_H_SUPL + NUM_M_EFECT + NUM_M_SUPL) AS Numero_Membros_Cadernos_Eleitoriais,
				 MAX(INSCRITOS) AS Numero_Membros_Inscritos,
				 MAX(VOTANTES) AS Numero_Membros_Votantes,
				 MAX(MESES_MANDATO) AS Meses_de_Mandato,
				 NULL
	FROM TEMP_ENTIDADES NATURAL JOIN TEMP_ELEICAO_CORPOS_GERENTES WHERE CAST(SUBSTR(TEMP_ENTIDADES.ID_ENTIDADE,0,INSTR(TEMP_ENTIDADES.ID_ENTIDADE, '.')) AS INT) < 5
	GROUP BY ID_Organizacao_Sindical, Data;""")


	cursor.execute("""INSERT INTO Actos_Negociacao_Colectiva
	SELECT DISTINCT TEMP_IRCT.NUMERO AS ID,
				 NUMERO_SEQUENCIAL AS ID_SEQUENCIAL,
				 NOMECC AS Nome_Acto,
				 TIPO_CONVENCAO_DESCR_LONG AS Tipo_Acto,
				 NATUREZA_DESCRICAO AS Natureza,
				 ANO as Ano,
				 NUMBTE AS Numero,
				 SERIEBTE AS Serie,
				 date(replace(DATABTE,'.','-')) AS Data,
				 NULL as URL,
				 AMBITO_GEOGRAFICO_IRCT AS Ambito_Geografico
	FROM TEMP_IRCT;""")


	#cursor.execute("""UPDATE Actos_Negociacao_Colectiva SET (Nome_Acto, Natureza, Numero, Serie, Data, Ambito_Geografico) =
	#	(SELECT TEMP_IRCT.NOMECC, TEMP_IRCT.NATUREZA_DESCRICAO, TEMP_IRCT.NUMBTE, TEMP_IRCT.SERIEBTE, date(replace(TEMP_IRCT.DATABTE,'.','-')), 
	#	TEMP_IRCT.AMBITO_GEOGRAFICO_IRCT FROM TEMP_IRCT WHERE ID = TEMP_IRCT.NUMERO AND ID_SEQUENCIAL = TEMP_IRCT.NUMERO_SEQUENCIAL AND Tipo_Acto = TEMP_IRCT.TIPO_CONVENCAO_DESCR_LONG AND Ano = TEMP_IRCT.ANO);""")


	cursor.execute("CREATE INDEX IDX1 ON TEMP_IRCT(NUMERO,NUMERO_SEQUENCIAL,ANO,CODCAE);")

	cursor.execute("""INSERT INTO Outorgantes_Actos
	SELECT DISTINCT
		TEMP_OUTORGANTES.NUM AS ID,
		TEMP_OUTORGANTES.NUMSEQ AS ID_SEQUENCIAL,
		TEMP_OUTORGANTES.ANO AS Ano,
		(SELECT TEMP_OUTORGANTES.CODENTG || '.' || TEMP_OUTORGANTES.CODENTE || '.' || TEMP_OUTORGANTES.NUMALT AS ID_Organizacao_Sindical FROM Org_Sindical WHERE ID_Organizacao_Sindical=Org_Sindical.ID) as ID_Organizacao_Sindical,
		(SELECT TEMP_OUTORGANTES.CODENTG || '.' || TEMP_OUTORGANTES.CODENTE || '.' || TEMP_OUTORGANTES.NUMALT AS ID_Organizacao_Patronal FROM Org_Patronal WHERE ID_Organizacao_Patronal=Org_Patronal.ID) as ID_Organizacao_Patronal,
		CODCAE AS CAE,
		CASE
			WHEN CODCAE IS NULL THEN NULL
			WHEN TEMP_IRCT.REVCAE = '2.0' OR TEMP_IRCT.REVCAE = '2.1' THEN
				CASE
					WHEN SUBSTR(CODCAE, 0, 3) IN ('01','02','05') THEN 'AGRICULTURA, PRODU????O ANIMAL, CA??A, FLORESTA E PESCA'
					WHEN SUBSTR(CODCAE, 0, 3) IN ('10','11','12','13','14') THEN 'IND??STRIAS EXTRACTIVAS'
					WHEN SUBSTR(CODCAE, 0, 3) IN ('15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31','32','33','34','35','36','37') THEN 'IND??STRIAS TRANSFORMADORAS'
					WHEN SUBSTR(CODCAE, 0, 3) IN ('40') THEN 'ELECTRICIDADE, G??S, VAPOR, ??GUA QUENTE E FRIA E AR FRIO'
					WHEN SUBSTR(CODCAE, 0, 3) IN ('41') THEN 'CAPTA????O, TRATAMENTO E DISTRIBUI????O DE ??GUA; SANEAMENTO GEST??O DE RES??DUOS E DESPOLUI????O'
					WHEN SUBSTR(CODCAE, 0, 3) IN ('45') THEN 'CONSTRU????O'
					WHEN SUBSTR(CODCAE, 0, 3) IN ('50','51','52') THEN 'COM??RCIO POR GROSSO E A RETALHO; REPARA????O DE VE??CULOS AUTOM??VEIS E MOTOCICLOS'
					WHEN SUBSTR(CODCAE, 0, 3) IN ('55') THEN 'ALOJAMENTO, RESTAURA????O E SIMILARES'
					WHEN SUBSTR(CODCAE, 0, 3) IN ('60','61','62','63','64') THEN 'TRANSPORTES E ARMAZENAGEM'
					WHEN SUBSTR(CODCAE, 0, 3) IN ('65','66','67') THEN 'ACTIVIDADES FINANCEIRAS E DE SEGUROS'
					WHEN SUBSTR(CODCAE, 0, 3) IN ('70','71','72','73','74') THEN 'ACTIVIDADES IMOBILI??RIAS'
					WHEN SUBSTR(CODCAE, 0, 3) IN ('75') THEN 'ADMINISTRA????O P??BLICA E DEFESA; SEGURAN??A SOCIAL OBRIGAT??RIA'
					WHEN SUBSTR(CODCAE, 0, 3) IN ('80') THEN 'EDUCA????O'
					WHEN SUBSTR(CODCAE, 0, 3) IN ('85') THEN 'ACTIVIDADES DE SA??DE HUMANA E APOIO SOCIAL'
					WHEN SUBSTR(CODCAE, 0, 3) IN ('90','91','92','93') THEN 'OUTRAS ACTIVIDADES DE SERVI??OS'
					WHEN SUBSTR(CODCAE, 0, 3) IN ('95','96','97') THEN 'ACTIVIDADES DAS FAM??LIAS EMPREGADORAS DE PESSOAL DOM??STICO E ACTIVIDADES DE PRODU????O DAS FAM??LIAS PARA USO PR??PRIO'
					WHEN SUBSTR(CODCAE, 0, 3) IN ('99') THEN 'ACTIVIDADES DOS ORGANISMOS INTERNACIONAIS E OUTRAS INSTITUI????ES EXTRA-TERRITORIAIS'
				END
			WHEN TEMP_IRCT.REVCAE = '3.0' THEN
		 		CASE
					WHEN SUBSTR(CODCAE, 0, 3) IN ('01','02','03') THEN 'AGRICULTURA, PRODU????O ANIMAL, CA??A, FLORESTA E PESCA'
					WHEN SUBSTR(CODCAE, 0, 3) IN ('05','06','07','08','09') THEN 'IND??STRIAS EXTRACTIVAS'
					WHEN SUBSTR(CODCAE, 0, 3) IN ('10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31','32','33') THEN 'IND??STRIAS TRANSFORMADORAS'
					WHEN SUBSTR(CODCAE, 0, 3) IN ('35') THEN 'ELECTRICIDADE, G??S, VAPOR, ??GUA QUENTE E FRIA E AR FRIO'
					WHEN SUBSTR(CODCAE, 0, 3) IN ('36','37','38','39') THEN 'CAPTA????O, TRATAMENTO E DISTRIBUI????O DE ??GUA; SANEAMENTO GEST??O DE RES??DUOS E DESPOLUI????O'
					WHEN SUBSTR(CODCAE, 0, 3) IN ('41','42','43') THEN 'CONSTRU????O'
					WHEN SUBSTR(CODCAE, 0, 3) IN ('45','46','47') THEN 'COM??RCIO POR GROSSO E A RETALHO; REPARA????O DE VE??CULOS AUTOM??VEIS E MOTOCICLOS'
					WHEN SUBSTR(CODCAE, 0, 3) IN ('49','50','51','52','53') THEN 'TRANSPORTES E ARMAZENAGEM'
					WHEN SUBSTR(CODCAE, 0, 3) IN ('55','56') THEN 'ALOJAMENTO, RESTAURA????O E SIMILARES'
					WHEN SUBSTR(CODCAE, 0, 3) IN ('59','59','60','61','62','63') THEN 'ACTIVIDADES DE INFORMA????O E DE COMUNICA????O'
					WHEN SUBSTR(CODCAE, 0, 3) IN ('64','65','66') THEN 'ACTIVIDADES FINANCEIRAS E DE SEGUROS'
					WHEN SUBSTR(CODCAE, 0, 3) IN ('68') THEN 'ACTIVIDADES IMOBILI??RIAS'
					WHEN SUBSTR(CODCAE, 0, 3) IN ('69','70','71','72','73','74','75') THEN 'ACTIVIDADES DE CONSULTORIA, CIENT??FICAS, T??CNICAS E SIMILARES'
					WHEN SUBSTR(CODCAE, 0, 3) IN ('77','78','79','80','81','82') THEN 'ACTIVIDADES ADMINISTRATIVAS E DOS SERVI??OS DE APOIO'
					WHEN SUBSTR(CODCAE, 0, 3) IN ('84') THEN 'ADMINISTRA????O P??BLICA E DEFESA; SEGURAN??A SOCIAL OBRIGAT??RIA'
					WHEN SUBSTR(CODCAE, 0, 3) IN ('85') THEN 'EDUCA????O'
					WHEN SUBSTR(CODCAE, 0, 3) IN ('86','87','88') THEN 'ACTIVIDADES DE SA??DE HUMANA E APOIO SOCIAL'
					WHEN SUBSTR(CODCAE, 0, 3) IN ('90','91','92','93') THEN 'ACTIVIDADES ART??STICAS, DE ESPECT??CULOS, DESPORTIVAS E RECREATIVAS'
					WHEN SUBSTR(CODCAE, 0, 3) IN ('94','95','96') THEN 'OUTRAS ACTIVIDADES DE SERVI??OS'
					WHEN SUBSTR(CODCAE, 0, 3) IN ('97','98') THEN 'ACTIVIDADES DAS FAM??LIAS EMPREGADORAS DE PESSOAL DOM??STICO E ACTIVIDADES DE PRODU????O DAS FAM??LIAS PARA USO PR??PRIO'
					WHEN SUBSTR(CODCAE, 0, 3) IN ('99') THEN 'ACTIVIDADES DOS ORGANISMOS INTERNACIONAIS E OUTRAS INSTITUI????ES EXTRA-TERRITORIAIS'
				END
		END AS Sector
	FROM TEMP_OUTORGANTES, TEMP_IRCT
	WHERE TEMP_IRCT.NUMERO=TEMP_OUTORGANTES.NUM AND TEMP_IRCT.NUMERO_SEQUENCIAL=TEMP_OUTORGANTES.NUMSEQ AND TEMP_IRCT.ANO=TEMP_OUTORGANTES.ANO AND TEMP_IRCT.TIPO_CONVENCAO_CODIGO = TEMP_OUTORGANTES.TIPOCONV;""")


	cursor.execute("UPDATE Outorgantes_Actos SET ID_Organizacao_Sindical = ( SELECT ID FROM Org_Sindical WHERE ID LIKE SUBSTR(ID_Organizacao_Sindical,0,LENGTH(ID_Organizacao_Sindical)-1) ORDER BY ID DESC) WHERE ID_Organizacao_Sindical IS NULL;")
	cursor.execute("UPDATE Outorgantes_Actos SET ID_Organizacao_Patronal = ( SELECT ID FROM Org_Patronal WHERE ID LIKE SUBSTR(ID_Organizacao_Patronal,0,LENGTH(ID_Organizacao_Patronal)-1) ORDER BY ID DESC) WHERE ID_Organizacao_Patronal IS NULL;")
	cursor.execute("DELETE FROM Outorgantes_Actos WHERE rowid NOT IN (SELECT MAX(rowid) from Outorgantes_Actos group by ID, ID_SEQUENCIAL, ANO, ID_Organizacao_Sindical, ID_Organizacao_Patronal, Sector );")

	cursor.execute("UPDATE Outorgantes_Actos SET ID_Organizacao_Sindical = ( SELECT ID_Organizacao_Sindical FROM Outorgantes_Actos O2 WHERE O2.ID_Organizacao_Sindical IS NOT NULL AND Outorgantes_Actos.ID=O2.ID AND Outorgantes_Actos.ID_SEQUENCIAL=O2.ID_SEQUENCIAL AND Outorgantes_Actos.Ano=O2.Ano) WHERE ID_Organizacao_Sindical IS NULL;")

	cursor.execute("DELETE FROM Outorgantes_Actos WHERE rowid NOT IN (SELECT MAX(rowid) from Outorgantes_Actos group by ID, ID_SEQUENCIAL, ANO, ID_Organizacao_Sindical, ID_Organizacao_Patronal, Sector );")


	#Tabela temporaria para matching de mes com o numero do mes correspondente, usado na construcao das datas 
	cursor.execute("CREATE TABLE TEMP_MESES_NUMERO(Mes_EN VARCHAR(3), Mes_PT VARCHAR(3), Numero VARCHAR(2));")

	cursor.execute("""INSERT INTO TEMP_MESES_NUMERO (Mes_EN, Mes_PT, Numero)
	VALUES
		("Jan", "jan", "01"),("Feb", "fev", "02"),("Mar", "mar", "03"),("Apr", "abr", "04"),("May", "mai", "05"),("Jun", "jun", "06"),("Jul", "jul", "07"),("Aug", "ago", "08"),("Sep", "set", "09"),("Oct", "out", "10"),
		("Nov", "nov", "11"),("Dec", "dez", "12");""")

	cursor.execute("DROP VIEW TEMP_DATAS_ENTIDADES;")
	cursor.execute("DROP TABLE TEMP_ALTERACOES_ESTATUTOS;")
	cursor.execute("DROP TABLE TEMP_ALTERACOES_ESTATUTOS1;")
	cursor.execute("DROP TABLE TEMP_ELEICAO_CORPOS_GERENTES;")
	cursor.execute("DROP TABLE TEMP_ELEICAO_CORPOS_GERENTES1;")
	cursor.execute("DROP TABLE TEMP_ENTIDADES1;")
	cursor.execute("DROP TABLE TEMP_ENTIDADES;")
	cursor.execute("DROP TABLE TEMP_PROCESSOS1;")
	cursor.execute("DROP TABLE TEMP_PROCESSOS;")
	cursor.execute("DROP TABLE TEMP_IRCT;")
	cursor.execute("DROP TABLE TEMP_OUTORGANTES;")
	cursor.execute("DROP TABLE TEMP_WEBSITES_SINDICAIS;")
	cursor.execute("DROP TABLE TEMP_WEBSITES_PATRONAIS;")

	cursor.execute("CREATE TABLE CAE_SECCOES_TEMP( SECCAO CHAR(1) PRIMARY KEY , RANK INTEGER, TITLE VARCHAR(100) , SALARY FLOAT );")
	cursor.execute("CREATE TABLE CAE_SECCOES_KEYWORDS_TEMP( SECCAO CHAR(1) , KEYWORD VARCHAR(100) , FOREIGN KEY(SECCAO) REFERENCES CAE_SECCOES_TEMP );")

	cursor.execute("INSERT INTO CAE_SECCOES_TEMP VALUES ( 'A' , 1, 'AGRICULTURA, PRODU????O ANIMAL, CA??A, FLORESTA E PESCA', 726.3);")
	cursor.execute("INSERT INTO CAE_SECCOES_TEMP VALUES ( 'B' , 2, 'IND??STRIAS EXTRACTIVAS', 867.2);")
	cursor.execute("INSERT INTO CAE_SECCOES_TEMP VALUES ( 'C' , 2, 'IND??STRIAS TRANSFORMADORAS', 868.5);")
	cursor.execute("INSERT INTO CAE_SECCOES_TEMP VALUES ( 'D' , 2, 'ELECTRICIDADE, G??S, VAPOR, ??GUA QUENTE E FRIA E AR FRIO', 867.2);")
	cursor.execute("INSERT INTO CAE_SECCOES_TEMP VALUES ( 'E' , 2, 'CAPTA????O, TRATAMENTO E DISTRIBUI????O DE ??GUA; SANEAMENTO GEST??O DE RES??DUOS E DESPOLUI????O', NULL);")
	cursor.execute("INSERT INTO CAE_SECCOES_TEMP VALUES ( 'F' , 2, 'CONSTRU????O', 798.3);")
	cursor.execute("INSERT INTO CAE_SECCOES_TEMP VALUES ( 'G' , 3, 'COM??RCIO POR GROSSO E A RETALHO; REPARA????O DE VE??CULOS AUTOM??VEIS E MOTOCICLOS', NULL);")
	cursor.execute("INSERT INTO CAE_SECCOES_TEMP VALUES ( 'H' , 2, 'TRANSPORTES E ARMAZENAGEM', NULL);")
	cursor.execute("INSERT INTO CAE_SECCOES_TEMP VALUES ( 'I' , 3, 'ALOJAMENTO, RESTAURA????O E SIMILARES', NULL);")
	cursor.execute("INSERT INTO CAE_SECCOES_TEMP VALUES ( 'J' , 3, 'ACTIVIDADES DE INFORMA????O E DE COMUNICA????O', NULL);")
	cursor.execute("INSERT INTO CAE_SECCOES_TEMP VALUES ( 'K' , 3, 'ACTIVIDADES FINANCEIRAS E DE SEGUROS', NULL);")
	cursor.execute("INSERT INTO CAE_SECCOES_TEMP VALUES ( 'L' , 3, 'ACTIVIDADES IMOBILI??RIAS', NULL);")
	cursor.execute("INSERT INTO CAE_SECCOES_TEMP VALUES ( 'M' , 3, 'ACTIVIDADES DE CONSULTORIA, CIENT??FICAS, T??CNICAS E SIMILARES', NULL);")
	cursor.execute("INSERT INTO CAE_SECCOES_TEMP VALUES ( 'N' , 3, 'ACTIVIDADES ADMINISTRATIVAS E DOS SERVI??OS DE APOIO', 953.7);")
	cursor.execute("INSERT INTO CAE_SECCOES_TEMP VALUES ( 'O' , 3, 'ADMINISTRA????O P??BLICA E DEFESA; SEGURAN??A SOCIAL OBRIGAT??RIA', NULL);")
	cursor.execute("INSERT INTO CAE_SECCOES_TEMP VALUES ( 'P' , 3, 'EDUCA????O', NULL);")
	cursor.execute("INSERT INTO CAE_SECCOES_TEMP VALUES ( 'Q' , 3, 'ACTIVIDADES DE SA??DE HUMANA E APOIO SOCIAL', NULL);")
	cursor.execute("INSERT INTO CAE_SECCOES_TEMP VALUES ( 'R' , 3, 'ACTIVIDADES ART??STICAS, DE ESPECT??CULOS, DESPORTIVAS E RECREATIVAS', NULL);")
	cursor.execute("INSERT INTO CAE_SECCOES_TEMP VALUES ( 'S' , 3, 'OUTRAS ACTIVIDADES DE SERVI??OS', NULL);")
	cursor.execute("INSERT INTO CAE_SECCOES_TEMP VALUES ( 'T' , 3, 'ACTIVIDADES DAS FAM??LIAS EMPREGADORAS DE PESSOAL DOM??STICO E ACTIVIDADES DE PRODU????O DAS FAM??LIAS PARA USO PR??PRIO', NULL);")
	cursor.execute("INSERT INTO CAE_SECCOES_TEMP VALUES ( 'U' , 3, 'ACTIVIDADES DOS ORGANISMOS INTERNACIONAIS E OUTRAS INSTITUI????ES EXTRA-TERRITORIAIS', NULL);")

	cursor.execute("INSERT INTO CAE_SECCOES_KEYWORDS_TEMP VALUES ( 'A' , 'AGRICULTURA'), ( 'A' , 'PRODU????O ANIMAL'), ( 'A' , 'CA??A'), ( 'A' , 'FLORESTA'), ( 'A' , 'PESCA'), ( 'A' , 'PECU??RIA'), ( 'A' , 'AGRICULA'), ( 'A' , 'AGR??CULA');")
	cursor.execute("INSERT INTO CAE_SECCOES_KEYWORDS_TEMP VALUES ( 'B' , 'IND??STRIAS EXTRACTIVAS'), ( 'B' , 'MINAS'), ( 'B' , 'CORTI??EIRA');")
	cursor.execute("INSERT INTO CAE_SECCOES_KEYWORDS_TEMP VALUES ( 'C' , 'IND??STRIAS TRANSFORMADORAS'), ( 'C' , 'TEXTEIS'), ( 'B' , 'T??XT'), ( 'B' , 'CONSERVAS');")
	cursor.execute("INSERT INTO CAE_SECCOES_KEYWORDS_TEMP VALUES ( 'D' , 'ELECTRICIDADE'), ( 'D' , 'G??S'), ( 'D' , 'VAPOR'), ( 'D' , '??GUA QUENTE E FRIA'), ( 'D' , 'AR FRIO');")
	cursor.execute("INSERT INTO CAE_SECCOES_KEYWORDS_TEMP VALUES ( 'E' , '??GUA'), ( 'E' , 'SANEAMENTO'), ( 'E' , 'RES??DUOS'), ( 'E' , 'DESPOLUI????O');")
	cursor.execute("INSERT INTO CAE_SECCOES_KEYWORDS_TEMP VALUES ( 'F' , 'CONSTRU????O');")
	cursor.execute("INSERT INTO CAE_SECCOES_KEYWORDS_TEMP VALUES ( 'G' , 'COM??RCIO'), ( 'G' , 'REPARA????O DE VE??CULOS AUTOM??VEIS'), ( 'G' , 'REPARA????O DE MOTOCICLOS');")
	cursor.execute("INSERT INTO CAE_SECCOES_KEYWORDS_TEMP VALUES ( 'H' , 'TRANSPORTES'), ( 'H' , 'ARMAZENAGEM'), ( 'H' , 'CARGAS'), ( 'H' , 'MERCANTE'), ( 'H' , 'METRO'), ( 'H' , 'FERREA'), ( 'H' , 'FERROVI??RI'), ( 'H' , 'ESTIVADOR'), ( 'H' , 'PORTUARIO'), ( 'H' , 'PORTU??RIO');")
	cursor.execute("INSERT INTO CAE_SECCOES_KEYWORDS_TEMP VALUES ( 'I' , 'ALOJAMENTO'), ( 'I' , 'RESTAURA????O'), ( 'I' , 'HOTEL');")
	cursor.execute("INSERT INTO CAE_SECCOES_KEYWORDS_TEMP VALUES ( 'J' , 'INFORMA????O E COMUNICA????O'), ( 'J' , 'JORNAL'), ( 'J' , 'TELECOM');")
	cursor.execute("INSERT INTO CAE_SECCOES_KEYWORDS_TEMP VALUES ( 'K' , 'FINAN??AS'), ( 'K' , 'SEGUROS'), ( 'K' , 'BANCA'), ( 'K' , 'SEGURADOR');")
	cursor.execute("INSERT INTO CAE_SECCOES_KEYWORDS_TEMP VALUES ( 'L' , 'IMOBILI??RI');")
	cursor.execute("INSERT INTO CAE_SECCOES_KEYWORDS_TEMP VALUES ( 'M' , 'CONSULTORIA T??CNICA'), ( 'M' , 'CIENT??'), ( 'M' , 'CI??NT??'), ( 'M' , 'CI??NCIA');")
	cursor.execute("INSERT INTO CAE_SECCOES_KEYWORDS_TEMP VALUES ( 'N' , 'ACTIVIDADES ADMINISTRATIVAS'), ( 'N' , 'SERVI??OS'), ( 'N' , 'CORREIOS');")
	cursor.execute("INSERT INTO CAE_SECCOES_KEYWORDS_TEMP VALUES ( 'O' , 'ADMINISTRA????O P??BLICA'), ( 'O' , 'DEFESA'), ( 'O' , 'PRISIO'), ( 'O' , 'POL??CIA'), ( 'O' , 'POLICIA'), ( 'O' , 'SEGURAN??A P??BLICA'), ( 'O' , 'SEGURAN??A SOCIAL'), ( 'O' , 'SEGURAN??A'), ( 'O' , 'MAGISTRA');")
	cursor.execute("INSERT INTO CAE_SECCOES_KEYWORDS_TEMP VALUES ( 'P' , 'EDUCA????O'), ( 'P' , 'PROFESSORES');")
	cursor.execute("INSERT INTO CAE_SECCOES_KEYWORDS_TEMP VALUES ( 'Q' , 'SA??DE'), ( 'Q' , 'SAUDE'), ( 'Q' , 'MEDICO'), ( 'Q' , 'M??DICO'), ( 'Q' , 'ENFERMEIR'), ( 'Q' , 'APOIO SOCIAL'), ( 'Q' , 'HOSPITAL');")
	cursor.execute("INSERT INTO CAE_SECCOES_KEYWORDS_TEMP VALUES ( 'R' , 'ART??ST'), ( 'R' , 'ESPECT??CULOS'), ( 'R' , 'DESPORTI'), ( 'R' , 'RECREATIV'), ( 'R' , 'FUTEBOL');")
	cursor.execute("INSERT INTO CAE_SECCOES_KEYWORDS_TEMP VALUES ( 'S' , 'OUTROS SERVI??OS'), ( 'S' , 'NOT??RI');")
	cursor.execute("INSERT INTO CAE_SECCOES_KEYWORDS_TEMP VALUES ( 'T' , 'PESSOAL DOM??STICO');")
	cursor.execute("INSERT INTO CAE_SECCOES_KEYWORDS_TEMP VALUES ( 'U' , 'ORGANISMOS INTERNACIONAIS'), ( 'U' , 'INSTITUI????ES EXTRA-TERRITORIAIS');")

	cursor.execute("INSERT INTO Sectores_Profissionais SELECT TITLE AS Sector, NULL, SALARY AS Salario_Medio FROM CAE_SECCOES_TEMP;")

	cursor.execute("""UPDATE Sectores_Profissionais SET Nome_Abrev="ADMINISTRATIVAS E SERVI??OS DE APOIO" WHERE Sector = "ACTIVIDADES ADMINISTRATIVAS E DOS SERVI??OS DE APOIO";""")
	cursor.execute("""UPDATE Sectores_Profissionais SET Nome_Abrev="ART??STICAS, ESPECT??CULOS, DESPORTIVAS E RECREATIVAS" WHERE Sector = "ACTIVIDADES ART??STICAS, DE ESPECT??CULOS, DESPORTIVAS E RECREATIVAS";""")
	cursor.execute("""UPDATE Sectores_Profissionais SET Nome_Abrev="FAM??LIAS EMPREGADORAS E ACTIVIDADES DE PRODU????O" WHERE Sector = "ACTIVIDADES DAS FAM??LIAS EMPREGADORAS DE PESSOAL DOM??STICO E ACTIVIDADES DE PRODU????O DAS FAM??LIAS PARA USO PR??PRIO";""")
	cursor.execute("""UPDATE Sectores_Profissionais SET Nome_Abrev="CONSULTORIA, CIENT??FICAS, T??CNICAS E SIMILARES" WHERE Sector = "ACTIVIDADES DE CONSULTORIA, CIENT??FICAS, T??CNICAS E SIMILARES";""")
	cursor.execute("""UPDATE Sectores_Profissionais SET Nome_Abrev="INFORMA????O E DE COMUNICA????O" WHERE Sector = "ACTIVIDADES DE INFORMA????O E DE COMUNICA????O";""")
	cursor.execute("""UPDATE Sectores_Profissionais SET Nome_Abrev="SA??DE HUMANA E APOIO SOCIAL" WHERE Sector = "ACTIVIDADES DE SA??DE HUMANA E APOIO SOCIAL";""")
	cursor.execute("""UPDATE Sectores_Profissionais SET Nome_Abrev="ORGANISMOS INTERNACIONAIS E INSTITUI????ES EXTRA-TERRITORIAIS" WHERE Sector = "ACTIVIDADES DOS ORGANISMOS INTERNACIONAIS E OUTRAS INSTITUI????ES EXTRA-TERRITORIAIS";""")
	cursor.execute("""UPDATE Sectores_Profissionais SET Nome_Abrev="FINANCEIRAS E DE SEGUROS" WHERE Sector = "ACTIVIDADES FINANCEIRAS E DE SEGUROS";""")
	cursor.execute("""UPDATE Sectores_Profissionais SET Nome_Abrev="IMOBILI??RIAS" WHERE Sector = "ACTIVIDADES IMOBILI??RIAS";""")
	cursor.execute("""UPDATE Sectores_Profissionais SET Nome_Abrev="ADMINISTRA????O P??BLICA;SEGURAN??A SOCIAL OBRIGAT??RIA." WHERE Sector = "ADMINISTRA????O P??BLICA E DEFESA; SEGURAN??A SOCIAL OBRIGAT??RIA";""")
	cursor.execute("""UPDATE Sectores_Profissionais SET Nome_Abrev="AGRICULTURA, CA??A, FLORESTA E PESCA" WHERE Sector = "AGRICULTURA, PRODU????O ANIMAL, CA??A, FLORESTA E PESCA";""")
	cursor.execute("""UPDATE Sectores_Profissionais SET Nome_Abrev="ALOJAMENTO, RESTAURA????O E SIMILARES" WHERE Sector = "ALOJAMENTO, RESTAURA????O E SIMILARES";""")
	cursor.execute("""UPDATE Sectores_Profissionais SET Nome_Abrev="CAPTA????O, TRATAMENTO, DISTRIBUI????O E SANEAMENTO DE ??GUA" WHERE Sector = "CAPTA????O, TRATAMENTO E DISTRIBUI????O DE ??GUA; SANEAMENTO GEST??O DE RES??DUOS E DESPOLUI????O";""")
	cursor.execute("""UPDATE Sectores_Profissionais SET Nome_Abrev="COM??RCIO POR GROSSO E A RETALHO; REPARA????O DE VE??CULOS" WHERE Sector = "COM??RCIO POR GROSSO E A RETALHO; REPARA????O DE VE??CULOS AUTOM??VEIS E MOTOCICLOS";""")
	cursor.execute("""UPDATE Sectores_Profissionais SET Nome_Abrev="CONSTRU????O" WHERE Sector = "CONSTRU????O";""")
	cursor.execute("""UPDATE Sectores_Profissionais SET Nome_Abrev="EDUCA????O" WHERE Sector = "EDUCA????O";""")
	cursor.execute("""UPDATE Sectores_Profissionais SET Nome_Abrev="ELECTRICIDADE, G??S, VAPOR, ??GUA E AR" WHERE Sector = "ELECTRICIDADE, G??S, VAPOR, ??GUA QUENTE E FRIA E AR FRIO";""")
	cursor.execute("""UPDATE Sectores_Profissionais SET Nome_Abrev="IND??STRIAS EXTRACTIVAS" WHERE Sector = "IND??STRIAS EXTRACTIVAS";""")
	cursor.execute("""UPDATE Sectores_Profissionais SET Nome_Abrev="IND??STRIAS TRANSFORMADORAS" WHERE Sector = "IND??STRIAS TRANSFORMADORAS";""")
	cursor.execute("""UPDATE Sectores_Profissionais SET Nome_Abrev="OUTRAS ACTIVIDADES DE SERVI??OS" WHERE Sector = "OUTRAS ACTIVIDADES DE SERVI??OS";""")
	cursor.execute("""UPDATE Sectores_Profissionais SET Nome_Abrev="TRANSPORTES E ARMAZENAGEM" WHERE Sector = "TRANSPORTES E ARMAZENAGEM";""")

	cursor.execute("UPDATE Org_Sindical SET Sector=(SELECT DISTINCT Sector FROM Sectores_Profissionais, CAE_SECCOES_TEMP, CAE_SECCOES_KEYWORDS_TEMP WHERE Sectores_Profissionais.Sector=CAE_SECCOES_TEMP.TITLE AND CAE_SECCOES_TEMP.SECCAO=CAE_SECCOES_KEYWORDS_TEMP.SECCAO AND Org_Sindical.Nome LIKE '%' || CAE_SECCOES_KEYWORDS_TEMP.KEYWORD || '%' );")
	cursor.execute("UPDATE Org_Patronal SET Sector=(SELECT DISTINCT Sector FROM Sectores_Profissionais, CAE_SECCOES_TEMP, CAE_SECCOES_KEYWORDS_TEMP WHERE Sectores_Profissionais.Sector=CAE_SECCOES_TEMP.TITLE AND CAE_SECCOES_TEMP.SECCAO=CAE_SECCOES_KEYWORDS_TEMP.SECCAO AND Org_Patronal.Nome LIKE '%' || CAE_SECCOES_KEYWORDS_TEMP.KEYWORD || '%' );")
	#cursor.execute("UPDATE Org_Sindical SET Sector=(SELECT DISTINCT Sector FROM Outorgantes_Actos WHERE Outorgantes_Actos.ID_Organizacao_Sindical=Org_Sindical.ID GROUP BY Sector ORDER BY COUNT(*) DESC);")
	#cursor.execute("UPDATE Org_Patronal SET Sector=(SELECT DISTINCT Sector FROM Outorgantes_Actos WHERE Outorgantes_Actos.ID_Organizacao_Patronal=Org_Patronal.ID GROUP BY Sector ORDER BY COUNT(*) DESC);")

	cursor.execute("DROP TABLE CAE_SECCOES_KEYWORDS_TEMP;")
	cursor.execute("DROP TABLE CAE_SECCOES_TEMP;")

	municipios_codigosPostais(cursor)

	cursor.execute("CREATE TABLE TEMP_MCP( DESIG VARCHAR(1000) , CP VARCHAR(9) );")

	cursor.execute("""INSERT INTO TEMP_MCP 
	 SELECT DESIG, CP4 || '-' || CP3 || '-' AS CP FROM TEMP_MUNICIPIOS NATURAL JOIN TEMP_CP 
	UNION
	 SELECT DESIG, CP4 || CP3 || '-' AS CP FROM TEMP_MUNICIPIOS NATURAL JOIN TEMP_CP 
	UNION
	 SELECT DESIG, CP4 || '-' AS CP FROM TEMP_MUNICIPIOS NATURAL JOIN TEMP_CP;""")

	cursor.execute("UPDATE Org_Sindical SET Codigo_Postal=Codigo_Postal || '-' WHERE Codigo_Postal IS NOT NULL AND NOT(substr(Codigo_Postal, -1)='-');")
	cursor.execute("UPDATE Org_Patronal SET Codigo_Postal=Codigo_Postal || '-' WHERE Codigo_Postal IS NOT NULL AND NOT(substr(Codigo_Postal, -1)='-');")

	cursor.execute("CREATE INDEX TMPIDX1 ON Org_Sindical(Codigo_Postal);")
	cursor.execute("CREATE INDEX TMPIDX2 ON Org_Patronal(Codigo_Postal);")
	cursor.execute("CREATE INDEX TMPIDX3 ON TEMP_MCP(CP);")

	cursor.execute("UPDATE Org_Sindical SET Concelho_Sede=(SELECT DISTINCT UPPER(DESIG) FROM TEMP_MCP WHERE Codigo_Postal = CP);")
	cursor.execute("UPDATE Org_Patronal SET Concelho_Sede=(SELECT DISTINCT UPPER(DESIG) FROM TEMP_MCP WHERE Codigo_Postal = CP);")
	cursor.execute("UPDATE Org_Sindical SET Codigo_Postal=SUBSTR(Codigo_Postal, 0, LENGTH(Codigo_Postal)) WHERE Codigo_Postal IS NOT NULL AND substr(Codigo_Postal, -1)='-';")
	cursor.execute("UPDATE Org_Patronal SET Codigo_Postal=SUBSTR(Codigo_Postal, 0, LENGTH(Codigo_Postal)) WHERE Codigo_Postal IS NOT NULL AND substr(Codigo_Postal, -1)='-';")
	cursor.execute("UPDATE Org_Sindical SET Codigo_Postal=SUBSTR(Codigo_Postal, 0, 4) || '-' || SUBSTR(Codigo_Postal, 5, 3) WHERE Codigo_Postal IS NOT NULL AND instr(Codigo_Postal,'-') <= 0 AND LENGTH(Codigo_Postal)=7;")
	cursor.execute("UPDATE Org_Patronal SET Codigo_Postal=SUBSTR(Codigo_Postal, 0, 4) || '-' || SUBSTR(Codigo_Postal, 5, 3) WHERE Codigo_Postal IS NOT NULL AND instr(Codigo_Postal,'-') <= 0 AND LENGTH(Codigo_Postal)=7;")

	cursor.execute("UPDATE Org_Sindical SET Distrito_Sede=replace(Distrito_Sede,'EVORA','??VORA') WHERE Distrito_Sede IS NOT NULL AND instr(Distrito_Sede, 'EVORA');")
	cursor.execute("UPDATE Org_Patronal SET Distrito_Sede=replace(Distrito_Sede,'EVORA','??VORA') WHERE Distrito_Sede IS NOT NULL AND instr(Distrito_Sede, 'EVORA');")

	#PREENCHE A TABELA DA DIRECCAO_ORG_SINDICAL
	listasDirigentes(cursor)

	cursor.execute("DROP INDEX TMPIDX1;")
	cursor.execute("DROP INDEX TMPIDX2;")
	cursor.execute("DROP INDEX TMPIDX3;")
	cursor.execute("DROP TABLE TEMP_MCP;")
	cursor.execute("DROP TABLE TEMP_MUNICIPIOS;")
	cursor.execute("DROP TABLE TEMP_CP;")

	stats = {}

	cursorObj = cursor.execute("SELECT count() FROM Org_Sindical;") 

	stats['Org Sindicais'] = cursorObj.fetchone()[0]

	cursorObj = cursor.execute("SELECT count() FROM Org_Patronal;")

	stats['Org Patronais'] = cursorObj.fetchone()[0]

	cursorObj = cursor.execute("SELECT count() FROM Outorgantes_Actos;")

	stats['Actos Outorgantes'] = cursorObj.fetchone()[0]

	cursorObj = cursor.execute("SELECT count() FROM Actos_Eleitorais_Org_Sindical;")

	stats['Actos Eleitorais Org Sindicais'] = cursorObj.fetchone()[0]

	cursorObj = cursor.execute("SELECT count() FROM Actos_Negociacao_Colectiva;")

	stats['Actos Negocia????o Colectiva'] = cursorObj.fetchone()[0]

	connection.commit()

	connection.close()

	greves.main()

	return stats

if __name__ == '__main__':
	repDatabase()
