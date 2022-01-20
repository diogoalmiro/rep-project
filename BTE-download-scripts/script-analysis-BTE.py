#!/usr/bin/env python
import sys
sys.stdout.reconfigure(encoding='utf-8')
import datetime
import math
import operator
import optparse
import os
import re
import sys
import threading
import time
import webbrowser
import pandas as pd
import numpy as np
import json
from collections import namedtuple, OrderedDict
from functools import wraps
from getpass import getpass
from io import TextIOWrapper
import sqlite3
import nltk


os.chdir("../BTE-download-scripts/testar/")
ficheiros = glob.glob("*.txt"):


def orgMatches(nome):
	if nome == "SINDICATO DOS PROFISSIONAIS DE LACTICINIOS, ALIMENTAÇÃO, AGRICULTURA, ESCRITORIOS, COMERCIO, SERVIÇOS, TRANSPORTES RODOVIARIOS,METALOMECANICA, METALURGIA, CONSTRUÇAO CIVIL E MADEIRAS":
		name = "SINDICATO DOS PROFISSIONAIS DE LACTICÍNIOS, ALIMENTAÇÃO, AGRICULTURA, ESCRITÓRIOS, COMÉRCIO, SERVIÇOS, TRANSPORTES RODOVIÁRIOS, METALOMECÂNICA, METALURGIA, CONSTRUÇÃO CIVIL E MADEIRAS"
	
	elif nome == "SINDICATO NACIONAL DOS TRABALHADORES DA INDÚSTRIA E COMÉRCIO DE ALIMENTAÇÃO BEBIDAS E AFINS":
		name = "SINDICATO NACIONAL DOS TRABALHADORES DA INDÚSTRIA E COMÉRCIO DE ALIMENTAÇÃO, BEBIDAS E AFINS"

	elif nome == "SINDICADO DEMOCRATICO DOS TRABALHADORES DOS CORREIOS TELECOMUNICAÇOES, MÉDIA E SERVIÇOS":
		name = "SINDICATO DEMOCRÁTICO DOS TRABALHADORES DOS CORREIOS, TELECOMUNICAÇÕES, MEDIA E SERVIÇOS"
	else:
		name = nome.replace("FEDERAÇAO NACIONAL.","FEDERAÇÃO NACIONAL")
		name = name.replace("ASSOCIAÇAO","ASSOCIAÇÃO")
		name = name.replace("VÔO","VOO")
		name = name.replace("TECNICOS","TÉCNICOS")
		name = name.replace("FEDERAÇAO","FEDERAÇÃO")
		name = name.replace("POLICIA","POLÍCIA")
		name = name.replace("FERROVIARIOS","FERROVIÁRIOS")
		name = name.replace("E ACTIVIDADES","E DE ACTIVIDADES")
		name = name.replace(",DE ESCRITÓRIOS", ", DE ESCRITÓRIOS")
		name = name.replace(" (SMTP)","")
		name = name.replace("AUTONOMA","AUTÓNOMA")
		name = name.replace("JUDICIARIA", "JUDICIÁRIA")
		name = name.replace(" (ASAPOL)", "")
		name = name.replace(" (ASFIC/PJ)", "")
		name = name.replace(" (SINAPSA)","")
		name = name.replace(" (SINCESAHT)","")
		name = name.replace(" (SMZS)", "")
		name = name.replace(" (SINTICAVS)","")
		name = name.replace(" (SICOMP)","")
		name = name.replace(" (ASSOCIAÇÃO SINDICAL DE DOCENTES E INVESTIGADORES)","")
		name = name.replace(" (ASSOCIAÇÃO SINDICAL DE INVESTIGADORES E DOCENTES DO ENSINO SUPERIOR PARTICULAR E COOPERATIVO)","")
		name = name.replace(" (SIPE)","")
		name = name.replace("TRÁS-OS MONTES", "TRÁS-OS-MONTES")
		name = name.replace("VESTUARIO","VESTUÁRIO")
		name = name.replace("INTERPRETES","INTÉRPRETES")
		name = name.replace("COMERCIO","COMÉRCIO")
		name = name.replace("RESTAURAÇAO","RESTAURAÇÃO")
		name = name.replace("CAMARAS","CÂMARAS")
		name = name.replace("TEXTEIS","TÊXTEIS")
		name = name.replace("LANIFICIOS","LANIFÍCIOS")
		name = name.replace("CONFEÇÃO","CONFECÇÃO")
		name = name.replace("TRABABALADORES", "TRABALHADORES")
		name = name.replace("VIDRO", "VIDROS")
		name = name.replace("CONSTRUÇAO","CONSTRUÇÃO")
		name = name.replace("UNIAO","UNIÃO")
		name = name.replace("PUBLICAS","PÚBLICAS")
		name = name.replace("DAS INDÚSTRIAIS","DAS INDÚSTRIAS")
		name = name.replace("DA INDÚSTRIAIS","DA INDÚSTRIA")
		name = name.replace("TRABRABALHADORES", "TRABALHADORES")
		name = name.replace("E DE ENTIDADES COM FINS PUBLICOS","E DE ENTIDADES COM FINS PÚBLICOS")
		name = name.replace("DA EDUCAÇÃO DO ESTADO","DA EDUCAÇÃO, DO ESTADO")
		name = name.replace("INDÚSTRIAL","INDUSTRIAL")
		name = name.replace("DAS INDÚSTRIAS E COMÉRCIO DE PANIFICAÇÃO","DA INDÚSTRIA E COMÉRCIO DE PANIFICAÇÃO")
		name = name.replace("DEMOCRATICO","DEMOCRÁTICO")
		name = name.replace("DE QUADROS DAS TELECOMUNICAÇÕES","DOS QUADROS DAS TELECOMUNICAÇÕES")
		name = name.replace("DOSEDUCADORES", "DOS EDUCADORES")
		name = name.replace("DE DE ","DE ")
		name = name.replace("DOS MUSICOS","DOS MÚSICOS,")
		name = name.replace("TELECOMUNICAÇOES","TELECOMUNICAÇÕES")
		name = name.replace("AUTORIDADE SEGURANÇA","AUTORIDADE DE SEGURANÇA")
		name = name.replace("REGIOES", "REGIÕES")
		name = name.replace("INVESTIGAÇAO","INVESTIGAÇÃO")
		name = name.replace("USS/CGTPIN","UNIÃO DOS SINDICATOS DE SETÚBAL")
		name = name.replace("USL/CGTPIN","UNIÃO DOS SINDICATOS DE LISBOA")
		name = name.replace("USB/CGTPIN","UNIÃO DOS SINDICATOS DE BRAGANÇA")
		name = name.replace("USVC/CGTPIN","UNIÃO DOS SINDICATOS DE VIANA DO CASTELO")
		name = name.replace("USV/CGTPIN","UNIÃO DOS SINDICATOS DE VISEU")
		name = name.replace("USG/CGTPIN","UNIÃO DOS SINDICATOS DA GUARDA")
		name = name.replace("USSSCGA/CGTPIN","UNIÃO DOS SINDICATOS DE SINES, SANTIAGO DO CACÉM, GRÂNDOLA E ALCÁCER DO SAL")
		name = name.replace("USCB/CGTPIN","UNIÃO DOS SINDICATOS DE CASTELO BRANCO")
		name = name.replace("USDE/CGTPIN","UNIÃO DOS SINDICATOS DO DISTRITO DE ÉVORA")
		name = name.replace("USFF/CGTPIN","UNIÃO DOS SINDICATOS DA FIGUEIRA DA FOZ")
		name = name.replace("USBEJA/CGTPIN","UNIÃO DOS SINDICATOS DO DISTRITO DE BEJA")
		name = name.replace("UGT - COIMBRA - UNIÃO GERAL DE TRABALHADORES DE COIMBRA","UNIÃO GERAL DE TRABALHADORES DE COIMBRA")
		name = name.replace("UNIÃO GERAL DE TRABALHADORES - UGT PORTO","UNIÃO GERAL DE TRABALHADORES DO PORTO")
		name = name.replace("UGT - SETÚBAL, UNIÃO GERAL DE TRABALHADORES DE SETÚBAL","UNIÃO GERAL DE TRABALHADORES DE SETÚBAL")
		name = name.replace("UGT - BRAGA, UNIÃO GERAL DE TRABALHADORES DE BRAGA","UNIÃO GERAL DE TRABALHADORES - UGT - BRAGA")
		name = name.replace("UGT - VIANA DO CASTELO, UNIÃO GERAL DOS TRABALHADORES DE VIANA DO CASTELO","UNIÃO GERAL DE TRABALHADORES DE VIANA DO CASTELO")
		name = name.replace("UGT - CASTELO BRANCO - UNIÃO GERAL DE TRABALHADORES DE CASTELO BRANCO", "UNIÃO GERAL DE TRABALHADORES DE CASTELO BRANCO")
		name = name.replace("UGT - ALGARVE, UNIÃO GERAL DE TRABALHADORES DO ALGARVE","UNIÃO GERAL DE TRABALHADORES DO ALGARVE")
		name = name.replace("UGT - ÉVORA - UNIÃO GERAL DE TRABALHADORES DE ÉVORA","UNIÃO GERAL DE TRABALHADORES DE ÉVORA")
		name = name.replace("UGT - VILA REAL, UNIÃO GERAL DE TRABALHADORES DE VILA REAL","UNIÃO GERAL DE TRABALHADORES DE VILA REAL")
		name = name.replace("UGT - GUARDA, UNIÃO GERAL DE TRABALHADORES DA GUARDA","UNIÃO GERAL DE TRABALHADORES DA GUARDA")
		name = name.replace("UGT BEJA - UNIÃO GERAL DE TRABALHADORES DE BEJA","UNIÃO GERAL DE TRABALHADORES DE BEJA")
		name = name.replace("UGT PORTALEGRE - UNIÃO GERAL DE TRABALHADORES DE PORTALEGRE", "UNIÃO GERAL DE TRABALHADORES DE PORTALEGRE")
		name = name.replace("USP/CGTPIN","UNIÃO DOS SINDICATOS DO PORTO")
		name = name.replace("UGT-SANTAREM, UNIÃO GERAL DE TRABALHADORES DE SANTAREM","UGT - SANTARÉM")
		name = name.replace("UGT-BRAGANÇA - UNIÃO GERAL DE TRABALHADORES DE BRAGANÇA","UNIÃO GERAL DE TRABALHADORES DE BRAGANÇA")
		name = name.replace("UGT - VISEU, UNIÃO GERAL DE TRABALHADORES DE VISEU","UGT - VISEU")
		name = name.replace("UGT - AVEIRO, UNIÃO GERAL DE TRABALHADORES DE AVEIRO","UNIÃO GERAL DE TRABALHADORES DE AVEIRO")
		name = name.replace("UGT - LEIRIA - UNIÃO GERAL DE TRABALHADORES DE LEIRIA","UGT - LEIRIA")
		name = name.replace("UGT-LISBOA, UNIÃO GERAL DE TRABALHADORES DE LISBOA","UNIÃO GERAL DE TRABALHADORES DE LISBOA")
		name = name.replace("UNIÃO DOS SINDICATOS DE COIMBRA/CONFEDERAÇÃO GERAL DOS TRABALHADORES PORTUGUESES-INTERSINDICAL NACIONAL","UNIÃO DOS SINDICATOS DE COIMBRA")
		name = name.replace("USA/CGTPIN","UNIÃO DOS SINDICATOS DO ALGARVE")
		name = name.replace("U.S.A./CGTPIN","UNIÃO DOS SINDICATOS DE AVEIRO")
		name = name.replace("CGTPIN","CONFEDERAÇÃO GERAL DOS TRABALHADORES PORTUGUESES")
		name = name.replace("NAVEGAÇAO AEREA","NAVEGAÇÃO AÉREA")
		name = name.replace("FEDEDRAÇÃO","FEDERAÇÃO")
		name = name.replace("FEDEREÇAO","FEDERAÇÃO")
		name = name.replace("ORGANIZAÇAO","ORGANIZAÇÃO")
		name = name.replace("FEDERAÇÃO DOS SINDICATOS DA ADMINISTRAÇÃO PÚBLICA E ENTIDADES COM FINS PÚBLICOS","FEDERAÇÃO DOS SINDICATOS DA ADMINISTRAÇÃO PÚBLICA E DE ENTIDADES COM FINS PÚBLICOS")
		name = name.replace("PAPEL GRÁFICA","PAPEL, GRÁFICA")
		name = name.replace("CERÂMICA E VIDROS", "CERÂMICA E VIDRO")
		name = name.replace("MATERIAS","MATÉRIAS")
		name = name.replace("FLORESTA PESCA TURISMO INDÚSTRIA ALIMENTAR BEBIDAS E AFINS","FLORESTA, PESCA, TURISMO, INDÚSTRIA ALIMENTAR, BEBIDAS E AFINS" )
		name = name.replace("PORTUGUSES","PORTUGUESES")
		name = name.replace("DO MUSICOS","DOS MÚSICOS")
		name = name.replace("ESPECTACULOS","ESPECTÁCULOS")
		name = name.replace("FISCALIZAÇAO","FISCALIZAÇÃO")
		name = name.replace("AREA METROPLITANA","ÁREA METROPOLITANA")
		name = name.replace("MUNICIPIO","MUNICÍPIO")
		name = name.replace("AUTÓNOMA DE POLÍCIAS","AUTÓNOMA DE POLÍCIA")
		name = name.replace("E DE ACTIVIDADES DO AMBIENTE","E ACTIVIDADES DO AMBIENTE")
		name = name.replace(",PRE-HOSPITALAR - STEPH"," PRÉ-HOSPITALAR")
		name = name.replace("SINDICATO DOS TRABALHADORES DE AGRICULTURA","SINDICATO DOS TRABALHADORES DA AGRICULTURA")
		name = name.replace("SINDICATO DE OFICIAIS DE POLÍCIA, DA PSP","SINDICATO DOS OFICIAIS DE POLÍCIA, DA POLÍCIA DE SEGURANÇA PÚBLICA")
		name = name.replace("DO VIDROS", "DO VIDRO")
		name = name.replace("SINDICATO DOS TRABALHADORES DA ADMINISTRAÇÃO PÚBLICA E ENTIDADES COM FINS PÚBLICOS PÚBLICOS","SINDICATO DOS TRABALHADORES DA ADMINISTRAÇÃO PÚBLICA E ENTIDADES COM FINS PÚBLICOS")
		name = name.replace("FEDERAÇÃO DOS SINDICATOS DA ADMINISTRAÇÃO PÚBLICA","FEDERAÇÃO DE SINDICATOS DA ADMINISTRAÇÃO PÚBLICA")
		name = name.replace("TURÍSTICA, TRADUTORES E INTÉRPRETES","TURÍSTICA TRADUTORES E INTÉRPRETES")
		name = name.replace("ASSOCIAÇÃO DOS TRABALHADORES DA ESCOLA SUPERIOR DE MEDICINA DENTÁRIA DE LISBOA","ASSOCIAÇÃO DE TRABALHADORES DA FACULDADE DE MEDICINA DENTÁRIA DE LISBOA")
		name = name.replace("FISCALIZAÇÃO DO SERVIÇO","FISCALIZAÇÃO DOS SERVIÇOS")
		name = name.replace("DE BOMBEIROS","DOS BOMBEIROS")
		name = name.replace("FEDERAÇÃO NACIONAL DOS TRANSPORTES, INDÚSTRIA E ENERGIA","FEDERAÇÃO NACIONAL DE SINDICATOS DE TRANSPORTES, INDÚSTRIA E ENERGIA")
		name = name.replace("SINDICATO NACIONAL DOS FERROVIÁRIOS ADMINISTRATIVOS, TÉCNICOS E DE SERVIÇOS","SINDICATO NACIONAL DOS FERROVIÁRIOS ADMINISTRATIVOS TÉCNICOS E DE SERVIÇOS")
		name = name.replace("CHEFES DA POLÍCIA DE SEGURANÇA PÚBLICA","CHEFES DA PSP")
	return name

def electionMatches(nome):
	nome = nome.replace("quadriénio", "mandato")
	nome = nome.replace("biénio","mandato")
	nome = nome.replace("triénio", "mandato")
	nome = nome.replace("man-dato", "mandato")
	nome = nome.replace("elei-ção","eleição")
	return nome

def hifenReplaces(nome, sindicato):

	founds = re.findall("[^\d\s]-[^\d\s]", nome)

	for match in founds:
		if not(("TRÁS-OS-MONTES" in sindicato and match == "S-O" and match == "S-M") or ("PRÉ-HOSPITALAR" in sindicato and match == "É-H")):
			new_name = match.replace("-","")
			excerto = nome[ nome.find(match) + len(match) : ]
			nome = nome[ : nome.find(match) ] + new_name + excerto

	return nome


conn = None
try:
    conn = sqlite3.connect("../rep-database.db")
except Error as e:
    print(e)

c = conn.cursor()
c.execute('SELECT DISTINCT o.Nome, o.Acronimo, m.Numero, m.Ano FROM Mencoes_BTE_Org_Sindical m, Org_Sindical o WHERE m.Id_Organizacao_Sindical = o.ID AND m.Eleicoes = 1 AND m.Ano >= 2008 AND m.Ano <= 2018;')

f4 = open("./vazios/vazios-nome.txt", "w")
vazios = []
vazio = 0
encontrados = 0

for row in c.fetchall():
	name = orgMatches(row[0])
	acronimo = row[1]
	numero = row[2]
	ano = row[3]
	f3 = open("./vazios/vazios-" + str(ano) + ".txt", "a")

	filename = "bte" + str(numero) + "_" + str(ano) + ".txt"
	f = open("../BTE-data/" + filename, "r", encoding="utf-8")
	
	ir=0
	#tentar saber em que página começa e em que página acaba a lista dos dirigentes
	#obter a lista e fazer o tratamento da mesma
	resultados = []
	resultado = ''
	#ver se estou no indice, se sim não avanço
	page = 0
	for x in f:
		page = page + 1
		x = electionMatches(x.upper())
		x = hifenReplaces(x, name)
		if name in x and page > 10:
			exc1 = x[x.find(name) : ]
			if "MANDATO" in exc1:
				ir = 1
				exc = exc1[ : exc1.find("MANDATO") + len("MANDATO") ]
				excerto = exc1[ exc1.find("MANDATO") + len("MANDATO") : ]
				if "ELEIÇÃO" in excerto:
					resultado = exc + excerto[: excerto.find("ELEIÇÃO") + len("ELEIÇÃO")]
					resultados.append(resultado.replace('.º','º'))
					break
				elif "ASSOCIAÇÕES" in excerto:
					resultado = exc + excerto[: excerto.find("ASSOCIAÇÕES") + len("ASSOCIAÇÕES")]
					resultados.append(resultado.replace('.º','º'))
					break
				else:
					resultado = exc1
					resultados.append(resultado.replace('.º','º'))

		elif ir==1:
			if "ELEIÇÃO" in x:
				resultado = x[: x.find("ELEIÇÃO") + len("ELEIÇÃO")]
				resultados.append(resultado.replace('.º','º'))
				break
			elif "ASSOCIAÇÕES" in x:
				resultado = x[: x.find("ASSOCIAÇÕES") + len("ASSOCIAÇÕES")]
				resultados.append(resultado.replace('.º','º'))
				break
			elif "PAGE" not in x:
				resultado = x
				resultados.append(resultado.replace('.º','º'))
				

	f.close()

	partido = []

	for resultado in resultados:
		res = resultado.replace('.', '. ')
		partido.append(res)

	if len(partido) == 0:
		if name not in vazios:
			vazios.append(name)
			f4.write(name)
			f4.write("\n")
		
		print(row[0])
		print(name + "_" + filename)
		f3.write(name + "_" + filename)
		f3.write("\n")
		vazio = vazio + 1
	else:
		encontrados = encontrados + 1
		if name == "SINDICATO DOS PROFISSIONAIS DE LACTICÍNIOS, ALIMENTAÇÃO, AGRICULTURA, ESCRITÓRIOS, COMÉRCIO, SERVIÇOS, TRANSPORTES RODOVIÁRIOS, METALOMECÂNICA, METALURGIA, CONSTRUÇÃO CIVIL E MADEIRAS":
			f2 = open("encontrados-" + str(ano) + "/" + "SIND DOS PROF DE LACTICÍNIOS, ALIMENTAÇÃO, ESCRITÓRIOS, COMÉRCIO, SERVIÇOS, TRANSP. RODOVIÁRIOS, METALOMECÂNICA, METALURGIA, CONSTR. CIVIL E MADEIRAS" + "_" + filename, "w", encoding="utf-8")
		elif "/CGTPIN" in row[0]:
			if "USS" in row[0]:
				value = "USS"
			elif "USL" in row[0]:
				value = "USL"
			elif "USVC" in row[0]:
				value = "USVC"
			elif "USV" in row[0]:
				value = "USV"
			elif "USG" in row[0]:
				value = "USG"
			elif "USB" in row[0]:
				value = "USB"
			elif "USCB" in row[0]:
				value = "USCB"

			f2 = open("encontrados-" + str(ano) + "/" + value + "_" + filename, "w", encoding="utf-8")

		elif "UNIÃO DOS SINDICATOS DE COIMBRA" in row[0]:
			value = "UNIÃO DOS SINDICATOS DE COIMBRA"
			f2 = open("encontrados-" + str(ano) + "/" + value + "_" + filename, "w", encoding="utf-8")
		else:
			f2 = open("encontrados-" + str(ano) + "/" + row[0] + "_" + filename, "w", encoding="utf-8")

		final = ''
		frase_id = 0
		
		for resultado in partido:
			frases = nltk.sent_tokenize(resultado)
			for frase in frases:
				
				frase_id = frase_id + 1

				if "ELEIÇÃO" in frase or "ASSOCIAÇÕES" in frase or 'BOLETIM' in frase:
					
					if frase_id == 1:
					
						founds_1 = re.findall("20[0-2][0-9]- 20[0-2][0-9]", frase)
						founds_2 = re.findall("20[0-2][0-9] -20[0-2][0-9]", frase)
						founds_1.extend(founds_2)
						
						for match in founds_1:
							
							if "- " in match:
								date = match.replace("- ","-")
							elif " -" in match:
								date = match.replace(" -","-")

							excerto = frase[ frase.find(match) +  len(match) : ]
							frase = frase[ : frase.find(match) ] + date + excerto

						final = frase
						f2.write(final)
						f2.write("\n")
					
					else:
						if "BOLETIM" in frase:
							final = frase
							f2.write(final)
							f2.write("\n")

					

				else:
					final = frase[ : frase.find(',')].replace('. ','.')
					f2.write(final)
					f2.write("\n")

		f2.close()

	f3.close()

f4.close()


print("Vazios: " + str(vazio) + "\n")
print("Encontrados: " + str(encontrados) + "\n")