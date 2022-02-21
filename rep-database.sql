PRAGMA foreign_keys = ON;
--- RECREATE DGERT DATABASE LOCALY
CREATE TABLE IF NOT EXISTS Entidades(
  codEntG INTEGER,
  codEntE INTEGER,
  numAlt INTEGER,
  sigla TEXT,
  nomeEntidade TEXT,
  codigoPostalEntidade TEXT,
  idDistrito INTEGER,
  distritoDescricao TEXT,
  estadoEntidade TEXT,
  moradaEntidade TEXT,
  localMoradaEntidade TEXT,
  areaPostalEntidade TEXT,
  dataBteConstituicao DATE,
  numeroBteConstituicao INTEGER,
  dataBteExtincao DATE,
  numeroBteExtincao INTEGER,
  telefoneEntidade TEXT,
  faxEntidade TEXT,
  PRIMARY KEY(codEntG, codEntE, numAlt)
);

CREATE TABLE IF NOT EXISTS Processos(
  tipo INTEGER,
  especie INTEGER,
  subEspecie INTEGER,
  numero INTEGER,
  ano INTEGER,
  controlo INTEGER,
  servico TEXT,
  codAssunto INTEGER,
  assunto TEXT,
  designacao TEXT,
  titulo TEXT,
  dataAberturaProcesso DATE,
  PRIMARY KEY(tipo,especie,subEspecie,numero,ano,servico)
);

CREATE TABLE IF NOT EXISTS EleicoesCorposGerentes(
  codEntG INTEGER,
  codEntE INTEGER,
  numAlt INTEGER,
  numeroEleicao INTEGER,
  dataEleicao DATE,
  mesesMandato INTEGER,
  dataBTE DATE,
  numBTE INTEGER,
  serieBTE INTEGER,
  numMaxEfect INTEGER,
  numMinEfect INTEGER,
  numMaxSupl INTEGER,
  numMinSupl INTEGER,
  numHEfect INTEGER,
  numHSupl INTEGER,
  numMEfect INTEGER,
  numMSupl INTEGER,
  tipo INTEGER,
  especie INTEGER,
  subEspecie INTEGER,
  numero INTEGER,
  ano INTEGER,
  controlo INTEGER,
  servico TEXT,
  FOREIGN KEY(codEntG, codEntE, numAlt) REFERENCES Entidades(codEntG, codEntE, numAlt),
  FOREIGN KEY(tipo,especie,subEspecie,numero,ano,servico) REFERENCES Processos(tipo,especie,subEspecie,numero,ano,servico),
  PRIMARY KEY(codEntG, codEntE, numAlt,tipo,especie,subEspecie,numero,ano,servico) --  we might be missing a field
);

CREATE TRIGGER IF NOT EXISTS create_Processo_when_insert_EleicaoCorpoGerentes
  BEFORE INSERT ON EleicoesCorposGerentes
  WHEN (SELECT count() FROM Processos WHERE tipo = NEW.tipo AND especie = NEW.especie AND subEspecie = NEW.subEspecie AND numero = NEW.numero AND ano = NEW.ano AND servico = ifnull(NEW.servico,"")) = 0
  BEGIN
    INSERT INTO Processos(tipo,especie,subEspecie,numero,ano,controlo,servico,codAssunto,assunto,designacao,titulo)
    VALUES(
      NEW.tipo,NEW.especie,NEW.subEspecie,NEW.numero,NEW.ano,NEW.controlo,ifnull(NEW.servico,""),
      120,"ELEIÇÃO DE CORPOS GERENTES","ELEIÇÃO DE CORPOS GERENTES DE ASS. " || CASE WHEN NEW.codEntG < 5 THEN "SINDICAL" ELSE "PATRONAL" END, "DEDUCED WHEN EleicaoCorpoGerentes INSERTED"); -- titulo e dataAberturaProcesso empty
  END;

CREATE TABLE IF NOT EXISTS AlteracoesEstatutos(
  tipo INTEGER,
  especie INTEGER,
  subEspecie INTEGER,
  numero INTEGER,
  ano INTEGER,
  controlo INTEGER,
  codEntG INTEGER,
  codEntE INTEGER,
  numAlt INTEGER,
  numSeqAlt INTEGER,
  numBTE INTEGER,
  dataBTE DATE,
  serieBTE INTEGER,
  ambitoGeografico TEXT,
  servico TEXT,
  FOREIGN KEY(codEntG, codEntE, numAlt) REFERENCES Entidades(codEntG, codEntE, numAlt),
  FOREIGN KEY(tipo,especie,subEspecie,numero,ano,servico) REFERENCES Processos(tipo,especie,subEspecie,numero,ano,servico)
  PRIMARY KEY(codEntG, codEntE, numAlt, numSeqAlt)
);

CREATE TRIGGER IF NOT EXISTS create_Processo_when_insert_AlteracoesEstatutos
  BEFORE INSERT ON AlteracoesEstatutos
  WHEN (SELECT count() FROM Processos WHERE tipo = NEW.tipo AND especie = NEW.especie AND subEspecie = NEW.subEspecie AND numero = NEW.numero AND ano = NEW.ano AND servico = ifnull(NEW.servico,"")) = 0
  BEGIN
    INSERT INTO Processos(tipo,especie,subEspecie,numero,ano,controlo,servico,codAssunto,assunto,designacao,titulo)
    VALUES(
      NEW.tipo,NEW.especie,NEW.subEspecie,NEW.numero,NEW.ano,NEW.controlo,ifnull(NEW.servico,""),
      123,"ALTERAÇÃO DE ESTATUTOS","ALTERAÇÃO DE ESTATUTOS DE ASS. " || CASE WHEN NEW.codEntG < 5 THEN "SINDICAL" ELSE "PATRONAL" END, "DEDUCED WHEN AltearcoesEstatutos INSERTED"); -- titulo e dataAberturaProcesso empty
  END;

CREATE TABLE IF NOT EXISTS PK_IRCTs(
  numero INTEGER,
  numeroSequencial INTEGER,
  ano INTEGER,
  tipoConvencaoCodigo INTEGER,
  PRIMARY KEY(numero, numeroSequencial, ano, tipoConvencaoCodigo)
);

CREATE TABLE IF NOT EXISTS IRCTs(
  numero INTEGER,
  numeroSequencial INTEGER,
  ano INTEGER,
  tipoConvencaoCodigo INTEGER,
  tipoConvencaoDescr TEXT,
  tipoConvencaoDescrLong TEXT,
  tipoConvencaoOrdem INTEGER,
  naturezaCodigo INTEGER,
  naturezaDescricao TEXT,
  nomeCC TEXT,
  ambitoGeograficoIRCT TEXT,
  ambitoGeograficoCodeIRCT INTEGER,
  numBTE INTEGER,
  dataBTE DATE,
  serieBTE INTEGER,
  ambGeg TEXT,
  dataEfeitos DATE,
  area INTEGER,
  dist INTEGER,
  conc INTEGER,
  prov INTEGER,
  codCAE TEXT,
  revCAE TEXT,
  FOREIGN KEY(numero, numeroSequencial, ano, tipoConvencaoCodigo) REFERENCES PK_IRCTs(numero, numeroSequencial, ano, tipoConvencaoCodigo)
  --PRIMARY KEY(numero,numeroSequencial,ano,tipoConvencaoCodigo,tipoConvencaoOrdem,naturezaCodigo,ambitoGeograficoCodeIRCT,dist,conc,codCAE,revCAE)
);

CREATE TRIGGER IF NOT EXISTS create_pk_IRCTs_when_insert_IRCTs
  BEFORE INSERT ON IRCTs
  WHEN (SELECT count() FROM PK_IRCTs WHERE numero = NEW.numero AND numeroSequencial = NEW.numeroSequencial AND ano = NEW.ano AND tipoConvencaoCodigo = NEW.tipoConvencaoCodigo) = 0
  BEGIN
    INSERT INTO PK_IRCTs(numero, numeroSequencial, ano, tipoConvencaoCodigo)
    VALUES(NEW.numero, NEW.numeroSequencial, NEW.ano, NEW.tipoConvencaoCodigo);
  END;

CREATE TABLE IF NOT EXISTS Outorgantes(
  numero INTEGER,
  numSeq INTEGER,
  ano INTEGER,
  tipoConv INTEGER,
  CodEntG INTEGER,
  CondEntE INTEGER,
  numAlt INTEGER,
  nomeEntE TEXT,
  siglaEntE TEXT,
  FOREIGN KEY (numero, numSeq, ano, tipoConv) REFERENCES PK_IRCTs(numero, numeroSequencial, ano, tipoConvencaoCodigo),
  FOREIGN KEY (CodEntG, CondEntE, numAlt) REFERENCES Entidades(codEntG, codEntE, numAlt),
  PRIMARY KEY (numero, numSeq, ano, tipoConv, CodEntG, CondEntE, numAlt)
);

CREATE TABLE IF NOT EXISTS Avisos_Greve(
  _id_greve INTEGER PRIMARY KEY AUTOINCREMENT,
  inicio_ano,
  inicio_mes,
  fim_ano,
  fim_mes,
  dadosExcel TEXT
);

CREATE TABLE IF NOT EXISTS Avisos_Greve_Entidades(
  _id_greve INTEGER,
  codEntG INTEGER,
  codEntE INTEGER,
  numAlt INTEGER,
  FOREIGN KEY (_id_greve) REFERENCES Avisos_Greve(_id_greve),
  FOREIGN KEY (codEntG, codEntE, numAlt) REFERENCES Entidades(codEntG, codEntE, numAlt)
);

--- USEFULL VIEWS
CREATE VIEW IF NOT EXISTS Organizacoes(
	codEntG, codEntE, numAlt, sigla, nomeEntidade, codigoPostalEntidade, idDistrito, distritoDescricao, estadoEntidade,
	moradaEntidade, localMoradaEntidade, areaPostalEntidade, telefoneEntidade, faxEntidade
) AS SELECT 
	codEntG, codEntE, numAlt, sigla, nomeEntidade, codigoPostalEntidade, idDistrito, distritoDescricao, estadoEntidade,
	moradaEntidade, localMoradaEntidade, areaPostalEntidade, telefoneEntidade, faxEntidade 
FROM 
	Entidades
JOIN
	(SELECT codEntG as G, codEntE as E, max(numAlt) as N FROM Entidades GROUP BY codEntG, codEntE) AS max_numAlt
WHERE
	Entidades.codEntG = max_numAlt.G AND
	Entidades.codEntE = max_numAlt.E AND
	Entidades.numAlt = max_numAlt.N;