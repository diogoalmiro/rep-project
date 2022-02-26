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

-- WHERE -- Filtrar os tipos de processos P.TIPO||'.'||P.ESPECIE||'.'||P.SUB_ESPECIE IN ('2.4.15' -- ELEIÇÃO DE CORPOS GERENTES DE ASS. SINDICAL ,'2.5.15' -- ELEIÇÃO DE CORPOS GERENTES DE ASS. PATRONAL --,'2.6.17' -- ELEIÇÃO DE COM. DE TRABALHADORES ,'2.4.13' -- CONSTITUIÇÃO DE ASS. SINDICAL ,'2.4.14' -- ALTERAÇÃO DE ESTATUTOS DE ASS. SINDICAL ,'2.4.18' -- EXTINÇÃO DE ASS. SINDICAL ,'2.4.85' -- ANULAÇÃO DE NORMAS DE ESTATUTOS OU DE DELIBERAÇOES ,'2.5.13' -- CONSTITUIÇÃO DE ASS. PATRONAL ,'2.5.14' -- ALTERAÇÃO DE ESTATUTOS DE ASS. PATRONAL ,'2.5.18' -- EXTINÇÃO DE ASS. PATRONAL ,'2.5.85' -- ANULAÇÃO DE NORMAS DE ESTATUTOS OU DE DELIBERAÇOES --,'2.6.14' -- ALT. ESTATUTOS C. TRAB. --,'2.6.16' -- CONSTITUIÇÃO E ESTATUTOS DE COM. TRABALHADORES --,'2.6.18' -- EXTINÇÃO DE COM. TRABALHADORES --,'2.6.85' -- ANULAÇÃO DE NORMAS DE ESTATUTOS OU DE DELIBER 

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
  PRIMARY KEY(codEntG, codEntE, numAlt,numeroEleicao,tipo,especie,subEspecie,numero,ano,servico)
);

CREATE TRIGGER IF NOT EXISTS create_Processo_when_insert_EleicaoCorpoGerentes
  BEFORE INSERT ON EleicoesCorposGerentes
  WHEN (SELECT count() FROM Processos WHERE tipo = NEW.tipo AND especie = NEW.especie AND subEspecie = NEW.subEspecie AND numero = NEW.numero AND ano = NEW.ano AND servico = ifnull(NEW.servico,"")) = 0
  BEGIN
    INSERT INTO Processos(tipo,especie,subEspecie,numero,ano,controlo,servico,codAssunto,assunto,designacao,titulo)
    VALUES(
      NEW.tipo,NEW.especie,NEW.subEspecie,NEW.numero,NEW.ano,NEW.controlo,ifnull(NEW.servico,""),
      120,"ELEIÇÃO DE CORPOS GERENTES","ELEIÇÃO DE CORPOS GERENTES DE ASS. " || CASE WHEN NEW.codEntG < 5 THEN "SINDICAL" ELSE "PATRONAL" END, "AVISO: Este processo foi deduzido ao se inserir uma EleicaoCorpoGerentes, poderá haver algum erro na tabela original."); -- titulo e dataAberturaProcesso empty
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
  PRIMARY KEY(codEntG, codEntE, numAlt, numSeqAlt,tipo,especie,subEspecie,numero,ano,servico)
);

CREATE TRIGGER IF NOT EXISTS create_Processo_when_insert_AlteracoesEstatutos
  BEFORE INSERT ON AlteracoesEstatutos
  WHEN (SELECT count() FROM Processos WHERE tipo = NEW.tipo AND especie = NEW.especie AND subEspecie = NEW.subEspecie AND numero = NEW.numero AND ano = NEW.ano AND servico = ifnull(NEW.servico,"")) = 0
  BEGIN
    INSERT INTO Processos(tipo,especie,subEspecie,numero,ano,controlo,servico,codAssunto,assunto,designacao,titulo)
    VALUES(
      NEW.tipo,NEW.especie,NEW.subEspecie,NEW.numero,NEW.ano,NEW.controlo,ifnull(NEW.servico,""),
      123,"ALTERAÇÃO DE ESTATUTOS","ALTERAÇÃO DE ESTATUTOS DE ASS. " || CASE WHEN NEW.codEntG < 5 THEN "SINDICAL" ELSE "PATRONAL" END, "AVISO: Este processo foi deduzido ao se inserir uma AltearcoesEstatutos, poderá haver algum erro na tabela original."); -- titulo e dataAberturaProcesso empty
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
  codCAE TEXT, -- MULTIPLOS VALORES ASSOCIADOS AO CODIGO CAE
  revCAE TEXT, --
  FOREIGN KEY(numero, numeroSequencial, ano, tipoConvencaoCodigo) REFERENCES PK_IRCTs(numero, numeroSequencial, ano, tipoConvencaoCodigo)
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
  FOREIGN KEY (_id_greve) REFERENCES Avisos_Greve(_id_greve) ON DELETE CASCADE,
  FOREIGN KEY (codEntG, codEntE, numAlt) REFERENCES Entidades(codEntG, codEntE, numAlt),
  PRIMARY KEY (_id_greve, codEntG, codEntE, numAlt)
);

--- USEFULL VIEWS
DROP VIEW IF EXISTS Organizacoes;
CREATE VIEW Organizacoes AS SELECT 
	codEntG, codEntE, numAlt, sigla, nomeEntidade, codigoPostalEntidade, idDistrito, distritoDescricao, estadoEntidade,
	moradaEntidade, localMoradaEntidade, areaPostalEntidade, telefoneEntidade, faxEntidade,
  CASE 
    WHEN codEntG = 1 THEN 'SINDICATO'
    WHEN codEntG = 2 THEN 'FEDERAÇÃO SINDICAL'
    WHEN codEntG = 3 THEN 'UNIÃO SINDICAL'
    WHEN codEntG = 4 THEN 'CONFEDERAÇÃO SINDICAL'
    WHEN codEntG = 5 THEN 'ASSOCIAÇÃO DE EMPREGADORES'
    WHEN codEntG = 6 THEN 'FEDERAÇÃO DE EMPREGADORES'
    WHEN codEntG = 7 THEN 'UNIÃO DE EMPREGADORES'
    WHEN codEntG = 8 THEN 'CONFEDERAÇÃO DE EMPREGADORES'
  END as Tipo,
  dataBteConstituicao,
  dataBteExtincao,
  upper(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(nomeEntidade,'á','A'), 'ã','A'), 'â','A'), 'é','E'), 'ê','E'), 'í','I'), 'ó','O') ,'õ','O') ,'ô','O'),'ú','U'),'ç','C'),'ñ','N'),'Á','A'), 'Ã','A'), 'Â','A'), 'É','E'), 'Ê','E'), 'Í','E'), 'Ó','O') ,'Õ','O') ,'Ô','O'),'Ú','U'),'Ç','C'),'Ñ','N')) as normalized_nome,
  codEntG || "." || codEntE as id,
  codEntG || "." || codEntE || "." || numAlt as unique_id
FROM 
	Entidades
JOIN
	(SELECT codEntG as G, codEntE as E, max(numAlt) as N FROM Entidades GROUP BY codEntG, codEntE) AS max_numAlt
WHERE
	Entidades.codEntG = max_numAlt.G AND
	Entidades.codEntE = max_numAlt.E AND
	Entidades.numAlt = max_numAlt.N;

-- refs: https://stackoverflow.com/questions/5492508/ignore-accents-sqlite3


-- EXPORT VIEWS
DROP VIEW IF EXISTS Export_IRCTs;
CREATE VIEW Export_IRCTs AS SELECT 
  E.codEntG || "." || E.codEntE || "." || E.numAlt as ID,
  E.nomeEntidade, I.numero, I.nomeCC, I.tipoConvencaoDescrLong, I.naturezaDescricao, I.ano, I.numBTE, I.serieBTE,
  CASE WHEN I.serieBTE = 1 THEN "http://bte.gep.msess.gov.pt/completos/" || I.Ano || "/bte" || I.Numero || "_" || I.Ano || ".pdf" ELSE "" END as urlBTE,
  I.ambitoGeograficoIRCT
FROM 
  (SELECT * FROM IRCTs as I JOIN Outorgantes as O 
    ON I.numero = O.numero AND I.numeroSequencial = O.numSeq AND I.ano = O.ano AND I.tipoConvencaoCodigo = O.tipoConv 
    GROUP BY O.codEntG, O.condEntE, O.numAlt, I.numero, I.numeroSequencial, I.ano, I.tipoConvencaoCodigo) as I
JOIN Entidades as E ON E.codEntG = I.codEntG AND E.codEntE = I.condEntE AND E.numAlt = I.numAlt;

DROP VIEW IF EXISTS Export_Avisos_Greve;
CREATE VIEW Export_Avisos_Greve AS SELECT 
  A._id_greve,
  E.codEntG || "." || E.codEntE || "." || E.numAlt as ID,
  E.nomeEntidade,
  A.inicio_ano, A.inicio_mes, A.fim_ano, A.fim_mes
FROM Avisos_Greve as A
JOIN Avisos_Greve_Entidades as AE ON A._id_greve = AE._id_greve
JOIN Entidades as E ON AE.codEntG = E.codEntG AND AE.codEntE = E.codEntE AND AE.numAlt = E.numAlt;

-- "Código Identificador da Organização", "Denominação da Organização", "Ano", "Número", "Série", "URL para BTE"
DROP VIEW IF EXISTS Export_AlteracoesEstatutos;
CREATE VIEW Export_AlteracoesEstatutos AS SELECT
  E.codEntG || "." || E.codEntE || "." || E.numAlt as ID,
  E.nomeEntidade,
  A.ano, A.numBTE, A.serieBTE,
  CASE WHEN A.serieBTE = 1 THEN "http://bte.gep.msess.gov.pt/completos/" || A.Ano || "/bte" || A.numBTE || "_" || A.Ano || ".pdf" ELSE "" END as urlBTE
FROM AlteracoesEstatutos as A
  JOIN Entidades as E ON A.codEntG = E.codEntG AND A.codEntE = E.codEntE AND A.numAlt = E.numAlt;

DROP VIEW IF EXISTS Export_EleicoesCorposGerentes;
CREATE VIEW Export_EleicoesCorposGerentes AS SELECT
  E.codEntG || "." || E.codEntE || "." || E.numAlt as ID,
  E.nomeEntidade,
  A.ano, A.numBTE, A.serieBTE,
  CASE WHEN A.serieBTE = 1 THEN "http://bte.gep.msess.gov.pt/completos/" || A.Ano || "/bte" || A.numBTE || "_" || A.Ano || ".pdf" ELSE "" END as urlBTE
FROM EleicoesCorposGerentes as A
  JOIN Entidades as E ON A.codEntG = E.codEntG AND A.codEntE = E.codEntE AND A.numAlt = E.numAlt;