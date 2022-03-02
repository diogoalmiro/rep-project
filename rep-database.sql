--
-- SQL Instructions for Creating the REP Database
--

DROP TABLE IF EXISTS Avisos_Greve;
DROP TABLE IF EXISTS Outorgantes_Actos;
DROP TABLE IF EXISTS Actos_Negociacao_Colectiva;
DROP TABLE IF EXISTS Direccao_Org_Patronal;
DROP TABLE IF EXISTS Direccao_Org_Sindical;
DROP TABLE IF EXISTS Membros_Org_Sindical;
DROP TABLE IF EXISTS Actos_Eleitorais_Org_Sindical;
DROP TABLE IF EXISTS Relacoes_Entre_Org_Sindical;
DROP TABLE IF EXISTS Mencoes_BTE_Org_Patronal;
DROP TABLE IF EXISTS Mencoes_BTE_Org_Sindical;
DROP TABLE IF EXISTS Org_Sindical;
DROP TABLE IF EXISTS Org_Patronal;
DROP TABLE IF EXISTS Sectores_Profissionais;


CREATE TABLE Sectores_Profissionais (
		Sector         VARCHAR(100) NOT NULL PRIMARY KEY,
		Nome_Abrev     VARCHAR(40),
		Salario_Medio  NUMERIC
);

CREATE TABLE Org_Patronal (
	ID                       VARCHAR(10) PRIMARY KEY,
	Tipo         		 VARCHAR(100),
	Nome                     VARCHAR(100) NOT NULL,
	Acronimo                 VARCHAR(100),
	Nome_Organizacao_Pai     VARCHAR(100),
	Concelho_Sede            VARCHAR(100),
	Distrito_Sede            VARCHAR(100),
	Codigo_Postal            VARCHAR(8),
	Morada_Entidade 	 VARCHAR(100),
	Local_Morada_Entidade 	 VARCHAR(100),
	Area_Postal_Entidade 	 VARCHAR(100),
	Telefone_Entidade 	 VARCHAR(9),
	Fax_Entidade 		 VARCHAR(9),
	Ambito_Geografico        VARCHAR(100),  
	Sector                   VARCHAR(100),
	Numero_Membros           INT,
	Data_Primeira_Actividade DATE,
	Data_Ultima_Actividade   DATE,
	Activa       		 BOOLEAN,
	Website                  VARCHAR(1000), 
	FOREIGN KEY (Nome_Organizacao_Pai) REFERENCES Org_Patronal(ID),
	FOREIGN KEY (Sector) REFERENCES Sectores_Profissionais(Sector)
);
	
CREATE TABLE Org_Sindical (
	ID                       VARCHAR(10) PRIMARY KEY,
	Tipo                     VARCHAR(100),
	Nome                     VARCHAR(100) NOT NULL,
	Acronimo                 VARCHAR(100),
	Nome_Organizacao_Pai     VARCHAR(100),
	Concelho_Sede            VARCHAR(100),
	Distrito_Sede            VARCHAR(100),
	Codigo_Postal            VARCHAR(8),
	Morada_Entidade 	 VARCHAR(100),
	Local_Morada_Entidade 	 VARCHAR(100),
	Area_Postal_Entidade 	 VARCHAR(100),
	Telefone_Entidade 	 VARCHAR(9),
	Fax_Entidade 		 VARCHAR(9),
	Ambito_Geografico        VARCHAR(100),
	Sector                   VARCHAR(100),
	Numero_Membros           INT,
	Data_Primeira_Actividade DATE,
	Data_Ultima_Actividade   DATE,
	Activa                   BOOLEAN,
	Website                  VARCHAR(1000),
	FOREIGN KEY (Nome_Organizacao_Pai) REFERENCES Org_Sindical(ID),
	FOREIGN KEY (Sector) REFERENCES Sectores_Profissionais(Sector)
);

CREATE TABLE Mencoes_BTE_Org_Sindical (
	ID_Organizacao_Sindical               VARCHAR(10),
	URL                                   VARCHAR(100),
	Ano                                   INT,
	Numero                                INT,
	Serie                                 INT,
	Descricao                             VARCHAR(100),
	Mudanca_Estatuto                      BOOLEAN,
	Eleicoes                              BOOLEAN,
	Confianca                             NUMERIC,
	PRIMARY KEY (ID_Organizacao_Sindical,Ano,Numero,Serie),
	FOREIGN KEY (ID_Organizacao_Sindical) REFERENCES Org_Sindical(ID)
);

CREATE TABLE Mencoes_BTE_Org_Patronal (
	ID_Organizacao_Patronal               VARCHAR(10),
	URL                     	      VARCHAR(100),
	Ano                                   INT,
	Numero                                INT,
	Serie                                 INT,
	Descricao                             VARCHAR(100),
	Mudanca_Estatuto                      BOOLEAN,
	Eleicoes                              BOOLEAN,
	Confianca                             NUMERIC,
	PRIMARY KEY (ID_Organizacao_Patronal,Ano,Numero,Serie),
	FOREIGN KEY (ID_Organizacao_Patronal) REFERENCES Org_Patronal(ID)
);

CREATE TABLE Relacoes_Entre_Org_Sindical (
	ID_Organizacao_Sindical_1  VARCHAR(10),
	ID_Organizacao_Sindical_2  VARCHAR(10),
	Tipo_de_Relacao            VARCHAR(100),
	Data                       DATE,
	PRIMARY KEY (ID_Organizacao_Sindical_1,ID_Organizacao_Sindical_2),
	FOREIGN KEY (ID_Organizacao_Sindical_1) REFERENCES Org_Sindical(ID),
	FOREIGN KEY (ID_Organizacao_Sindical_2) REFERENCES Org_Sindical(ID)
);

CREATE TABLE Actos_Eleitorais_Org_Sindical (
	ID_Organizacao_Sindical               VARCHAR(10),
	Data                                  DATE,
	Numero_Membros_Cadernos_Eleitoriais   INT,
	Numero_Membros_Inscritos              INT,
	Numero_Membros_Votantes               INT,
	Meses_de_Mandato                      INT,
	Numero_Listas_Concorrentes            INT,
	PRIMARY KEY (ID_Organizacao_Sindical,Data),
	FOREIGN KEY (ID_Organizacao_Sindical) REFERENCES Org_Sindical(ID)
);

CREATE TABLE Membros_Org_Sindical (
	ID_Organizacao_Sindical               VARCHAR(10),
	Data_Eleicao 			      DATE,
	Data_Inicio                           INT,
	Data_Fim                              INT,
	Numero_Membros                        INT,
	PRIMARY KEY (ID_Organizacao_Sindical,Data_Eleicao,Data_Inicio,Data_Fim),
	FOREIGN KEY (ID_Organizacao_Sindical) REFERENCES Org_Sindical(ID)
);
	
CREATE TABLE Direccao_Org_Sindical(
	ID_Organizacao_Sindical     VARCHAR(10),
	Nome_Pessoa                 VARCHAR(100),
	Genero_Sexo                 VARCHAR(100),
	Cargo                       VARCHAR(100),
	Data_Eleicao  		    DATE,
	Data_Inicio                 INT,
	Data_Fim                    INT,
	PRIMARY KEY (ID_Organizacao_Sindical,Nome_Pessoa,Data_Eleicao,Data_Inicio,Data_Fim),
	FOREIGN KEY (ID_Organizacao_Sindical) REFERENCES Org_Sindical(ID)
);

CREATE TABLE Actos_Negociacao_Colectiva (
	ID                         INT,
	ID_SEQUENCIAL        	   INT,
	Nome_Acto                  VARCHAR(100),
	Tipo_Acto                  VARCHAR(100),
	Natureza                   VARCHAR(100),
	Ano                        INT,
	Numero                     INT,
	Serie                      INT,
	Data                       DATE,
	URL                        VARCHAR(100),
	Ambito_Geografico          VARCHAR(100),
	PRIMARY KEY (ID,ID_SEQUENCIAL,Ano,Tipo_Acto)
);

CREATE TABLE Outorgantes_Actos (
	ID                         INT,
	ID_SEQUENCIAL              INT,
	Ano                        INT,
	ID_Organizacao_Sindical    VARCHAR(10),
	ID_Organizacao_Patronal    VARCHAR(10),
	CAE                        VARCHAR(10),
	Sector                     VARCHAR(100),
	PRIMARY KEY (ID,ID_SEQUENCIAL,Ano,ID_Organizacao_Sindical,ID_Organizacao_Patronal,Sector),
	FOREIGN KEY (ID,ID_SEQUENCIAL,Ano) REFERENCES Actos_Negociacao_Colectiva(ID,ID_SEQUENCIAL,Ano),
	FOREIGN KEY (ID_Organizacao_Sindical) REFERENCES Org_Sindical(ID),
	FOREIGN KEY (ID_Organizacao_Patronal) REFERENCES Org_Patronal(ID),
	FOREIGN KEY (Sector) REFERENCES Sectores_Profissionais(Sector)
);

CREATE TABLE Avisos_Greve (
	Id_Entidade_Sindical      VARCHAR(10),
	Ano_Inicio                INT,
	Mes_Inicio                INT,
	Entidade_Sindical         VARCHAR(100),
	Entidade_Patronal         VARCHAR(100),
	Ano_Fim                   INT,
	Mes_Fim                   INT,
	Duracao                   INT,
	PRIMARY KEY(Id_Entidade_Sindical,Ano_Inicio,Mes_Inicio,Entidade_Sindical,Entidade_Patronal,Ano_Fim,Mes_Fim,Duracao),
        FOREIGN KEY (Id_Entidade_Sindical) REFERENCES Org_Sindical(ID)
);


CREATE TRIGGER Mencoes_BTE_Org_Sindical_update AFTER UPDATE ON Mencoes_BTE_Org_Sindical WHEN NEW.Serie = 0 OR NEW.Serie = 1 BEGIN
		UPDATE Mencoes_BTE_Org_Sindical
		SET    URL = "http://bte.gep.msess.gov.pt/completos/" || NEW.Ano || "/bte" || NEW.Numero || "_" || NEW.Ano || ".pdf"
		WHERE 
			ID_Organizacao_Sindical = NEW.ID_Organizacao_Sindical AND
			Ano = NEW.Ano AND
			Numero = NEW.Numero AND
			Serie = NEW.Serie;
END;

CREATE TRIGGER Mencoes_BTE_Org_Sindical_insert AFTER INSERT ON Mencoes_BTE_Org_Sindical WHEN NEW.Serie = 0 OR NEW.Serie = 1 BEGIN
		UPDATE Mencoes_BTE_Org_Sindical
		SET    URL = "http://bte.gep.msess.gov.pt/completos/" || NEW.Ano || "/bte" || NEW.Numero || "_" || NEW.Ano || ".pdf"
		WHERE ID_Organizacao_Sindical = NEW.ID_Organizacao_Sindical AND
			Ano = NEW.Ano AND
			Numero = NEW.Numero AND
			Serie = NEW.Serie; 
END;

CREATE TRIGGER Mencoes_BTE_Org_Patronal_update AFTER UPDATE ON Mencoes_BTE_Org_Patronal WHEN NEW.Serie = 0 OR NEW.Serie = 1 BEGIN
		UPDATE Mencoes_BTE_Org_Patronal
		SET    URL = "http://bte.gep.msess.gov.pt/completos/" || NEW.Ano || "/bte" || NEW.Numero || "_" || NEW.Ano || ".pdf"
		WHERE ID_Organizacao_Patronal = NEW.ID_Organizacao_Patronal AND
			Ano = NEW.Ano AND
			Numero = NEW.Numero AND
			Serie = NEW.Serie;
END;

CREATE TRIGGER Mencoes_BTE_Org_Patronal_insert AFTER INSERT ON Mencoes_BTE_Org_Patronal WHEN NEW.Serie = 0 OR NEW.Serie = 1 BEGIN
		UPDATE Mencoes_BTE_Org_Patronal
		SET    URL = "http://bte.gep.msess.gov.pt/completos/" || NEW.Ano || "/bte" || NEW.Numero || "_" || NEW.Ano || ".pdf"
		WHERE ID_Organizacao_Patronal = NEW.ID_Organizacao_Patronal AND
			Ano = NEW.Ano AND
			Numero = NEW.Numero AND
			Serie = NEW.Serie;
END;

CREATE TRIGGER Actos_Negociacao_Colectiva_update AFTER UPDATE ON Actos_Negociacao_Colectiva WHEN NEW.Serie = 0 OR NEW.Serie = 1 BEGIN
		UPDATE Actos_Negociacao_Colectiva
		SET    URL = "http://bte.gep.msess.gov.pt/completos/" || NEW.Ano || "/bte" || NEW.Numero || "_" || NEW.Ano || ".pdf"
		WHERE ID = NEW.ID AND
			ID_SEQUENCIAL = NEW.ID_SEQUENCIAL AND
			Ano = NEW.Ano AND
			Tipo_Acto = NEW.Tipo_Acto;
END;

CREATE TRIGGER Actos_Negociacao_Colectiva_insert AFTER INSERT ON Actos_Negociacao_Colectiva WHEN NEW.Serie = 0 OR NEW.Serie = 1 BEGIN
		UPDATE Actos_Negociacao_Colectiva
		SET    URL = "http://bte.gep.msess.gov.pt/completos/" || NEW.Ano || "/bte" || NEW.Numero || "_" || NEW.Ano || ".pdf"
		WHERE ID = NEW.ID AND
			ID_SEQUENCIAL = NEW.ID_SEQUENCIAL AND
			Ano = NEW.Ano AND
			Tipo_Acto = NEW.Tipo_Acto;
END;
