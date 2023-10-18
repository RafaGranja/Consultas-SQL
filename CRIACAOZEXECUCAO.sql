CREATE TABLE ZEXECUCAO(
	ID INT IDENTITY(1,1),
    DESCRICAO VARCHAR(120) COLLATE SQL_Latin1_General_CP1_CI_AI NOT NULL,
    NUMDESENHO VARCHAR(60) COLLATE SQL_Latin1_General_CP1_CI_AI NOT NULL,
    REVISAODESENHO VARCHAR(10) COLLATE SQL_Latin1_General_CP1_CI_AI,
    NUMDBI VARCHAR(50) COLLATE SQL_Latin1_General_CP1_CI_AI,
    REVISAODBI VARCHAR(10) COLLATE SQL_Latin1_General_CP1_CI_AI,
    DESQTDE INT DEFAULT 1,
    TOTALQTDE INT DEFAULT 1,
    COMPRIMENTO NUMERIC(15,4),
    MATERIAL VARCHAR(90) COLLATE SQL_Latin1_General_CP1_CI_AI,
    OBSERVACOES VARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AI,
    OS VARCHAR(25) COLLATE SQL_Latin1_General_CP1_CI_AI NOT NULL,
    DATAREVISAO DATETIME DEFAULT GETDATE(),
    PESOBRUTO NUMERIC(15,4),
    PESOLIQUIDO NUMERIC(15,4),
    OBSERVACOESDESENHO VARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AI,
    PERIMETROCORTE NUMERIC(15,4),
    AREAPINTURA NUMERIC(15,4),
    OBSPROCESSO VARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AI,
    OBSGERAL VARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AI,
    TIPODESENHO VARCHAR(20) COLLATE SQL_Latin1_General_CP1_CI_AI,
    ESPESSURA NUMERIC(15,4),
    LARGURA NUMERIC(15,4),
    BITOLA NUMERIC(15,4),
    MASSALINEAR NUMERIC(15,4),
    DIAMETROEXTERNO NUMERIC(15,4),
    DIAMETROINTERNO NUMERIC(15,4),
    ESPROSCA NUMERIC(15,4),
    CODIGOPRD VARCHAR(30) COLLATE SQL_Latin1_General_CP1_CI_AI,
    POSICAODESENHO VARCHAR(40) COLLATE SQL_Latin1_General_CP1_CI_AI,
    AREASECAO NUMERIC(15,4),
    ALTURA NUMERIC(15,4),
    ESPALMA NUMERIC(15,4),
    POSICAOINDICE INT DEFAULT 1,
    PRODUTORM VARCHAR(160) COLLATE SQL_Latin1_General_CP1_CI_AI,
    IDPRD INT,
    UNDMEDIDA VARCHAR(5) COLLATE SQL_Latin1_General_CP1_CI_AI,
    LARGURAABA NUMERIC(15,4),
    ESPABA NUMERIC(15,4),
    COMPORLISTA VARCHAR(3) COLLATE SQL_Latin1_General_CP1_CI_AI,
    DESCOS VARCHAR(254) COLLATE SQL_Latin1_General_CP1_CI_AI,
    NUMDOCDELP VARCHAR(90) COLLATE SQL_Latin1_General_CP1_CI_AI,
    REVISAODOCDELP VARCHAR(10) COLLATE SQL_Latin1_General_CP1_CI_AI,
    CODCOLIGADA INT,
    CODFILIAL INT,
    DIAMETROEXTERNODISCO NUMERIC(15,4),
    DIAMETROINTERNODISCO NUMERIC(15,4),
    CHECKUSINAGEM VARCHAR(1) DEFAULT 'N' COLLATE SQL_Latin1_General_CP1_CI_AI,
    CHECKCALDERARIA VARCHAR(1) DEFAULT 'N' COLLATE SQL_Latin1_General_CP1_CI_AI,
    CODTRFPAI VARCHAR(4) COLLATE SQL_Latin1_General_CP1_CI_AI,
    NOMETRFPAI VARCHAR(90) COLLATE SQL_Latin1_General_CP1_CI_AI,
    EXECUCOES INT DEFAULT 1,
    PESOUNITARIO NUMERIC(15,4), 
    OPSUNITARIAS VARCHAR(3) COLLATE SQL_Latin1_General_CP1_CI_AI,
    QTDEUNCOMP VARCHAR(15) COLLATE SQL_Latin1_General_CP1_CI_AI,
    IDCRIACAO INT DEFAULT 1,
    IDCRIACAOPAI INT DEFAULT 0,
    ITEMEXCLUSIVO INT DEFAULT 0,
	ITEMDERETORNO INT,
    EXECUCAO INT NOT NULL DEFAULT 1,
    RECCREATEDBY VARCHAR(50) COLLATE SQL_Latin1_General_CP1_CI_AI NOT NULL DEFAULT 'fluig',
    RECCREATEDON DATETIME NOT NULL DEFAULT GETDATE(),
    RECMODIFIEDBY VARCHAR(50) COLLATE SQL_Latin1_General_CP1_CI_AI NOT NULL DEFAULT 'fluig',
    RECMODIFIEDON DATETIME NOT NULL DEFAULT GETDATE(),
	CONSTRAINT PK_IDCRIACAOEX PRIMARY KEY ( OS,EXECUCAO,IDCRIACAO ),
	CONSTRAINT CK_TIPODESENHOEX CHECK (TIPODESENHO IN ('SEMIACABADO','ACABADO','NAOMANUFATURADO','INDUSTRIALIZACAO',NULL)),
	CONSTRAINT CK_UNDMEDIDAEX CHECK (UNDMEDIDA IN ('CJ','UN','PC',NULL)),
	CONSTRAINT CK_COMPORLISTAEX CHECK (COMPORLISTA IN ('SIM',NULL)),
	CONSTRAINT CK_OPSUNITARIASEX CHECK (OPSUNITARIAS IN ('SIM',NULL)),
	CONSTRAINT CK_CHECKUSINAGEMEX CHECK (CHECKUSINAGEM IN ('S','N',NULL)),
	CONSTRAINT CK_CHECKCALDERARIAEX CHECK (CHECKCALDERARIA IN ('S','N',NULL)),
	CONSTRAINT CK_QTDEUNCOMPEX CHECK (QTDEUNCOMP IN ('PESOUNITARIO','DESENHO',NULL)),
	CONSTRAINT CK_ITEMEXCLUSIVOEX CHECK (ITEMEXCLUSIVO IN (0,1,2)),
    INDEX IDCRIACAO2 (OS,CODTRFPAI,EXECUCAO,IDCRIACAO),
	INDEX IDCRIACAO (OS,EXECUCAO,IDCRIACAO),
    INDEX EXECUCAO (OS,CODTRFPAI,EXECUCAO),
	INDEX CODTRF (OS,CODTRFPAI),
	INDEX OS (OS)
)


CREATE TABLE ZATIVIDADESEX(
	ID INT IDENTITY(1,1),
	OS VARCHAR(25) COLLATE SQL_Latin1_General_CP1_CI_AI NOT NULL,
	IDCRIACAO INT DEFAULT 1 NOT NULL,
	CODATIVIDADE VARCHAR(25) NOT NULL,
	PRIORIDADE INT NOT NULL DEFAULT 1,
	HABILIDADE VARCHAR(16) COLLATE SQL_Latin1_General_CP1_CI_AI NOT NULL,
	CODPOSTO VARCHAR(25) NOT NULL,
	FILA INT,
	CONFIGURACAO INT,
	PROCESSAMENTO INT,
	DESAGREGACAO INT,
	ESPERA INT,
	MOVIMENTACAO INT,
	MINUTOSGASTOS INT,
	DESCPROCESSO VARCHAR(500),
	DOCAPOIOATV1 VARCHAR(90),
	DOCAPOIOATV2 VARCHAR(90),
	DOCAPOIOATV3 VARCHAR(90),
	DOCAPOIOATV4 VARCHAR(90),
	FORNPARA VARCHAR(90),
    EXECUCAO INT NOT NULL,
	RECCREATEDBY VARCHAR(50) COLLATE SQL_Latin1_General_CP1_CI_AI NOT NULL DEFAULT 'fluig',
    RECCREATEDON DATETIME NOT NULL DEFAULT GETDATE(),
    RECMODIFIEDBY VARCHAR(50) COLLATE SQL_Latin1_General_CP1_CI_AI NOT NULL DEFAULT 'fluig',
    RECMODIFIEDON DATETIME NOT NULL DEFAULT GETDATE(),
	CONSTRAINT PK_IDATVEX PRIMARY KEY (OS,EXECUCAO,IDCRIACAO,PRIORIDADE),
    CONSTRAINT FK_ITEMATVEX FOREIGN KEY (OS,EXECUCAO,IDCRIACAO) REFERENCES ZEXECUCAO (OS,EXECUCAO,IDCRIACAO),
	INDEX ATV (OS,IDCRIACAO,EXECUCAO,PRIORIDADE),
	INDEX ATV2 (OS,IDCRIACAO,EXECUCAO,CODATIVIDADE),
	INDEX IDCRIACAO2 (OS,EXECUCAO,IDCRIACAO),
    INDEX IDCRIACAO (OS,IDCRIACAO),
	INDEX OS (OS)
)

CREATE TABLE ZCOMPONENTESEX(
	ID INT IDENTITY(1,1),
	OS VARCHAR(25) COLLATE SQL_Latin1_General_CP1_CI_AI NOT NULL,
	IDCRIACAO INT DEFAULT 1 NOT NULL,
	IDPRD INT NOT NULL,
	CODIGOPRD VARCHAR(30) COLLATE SQL_Latin1_General_CP1_CI_AI NOT NULL,
	CODUND VARCHAR(5) COLLATE SQL_Latin1_General_CP1_CI_AI NOT NULL,
	QTDEUN NUMERIC(15,4),
	QTDETOTAL NUMERIC(15,4),
	CODIGOPRDORIGEM VARCHAR(30) COLLATE SQL_Latin1_General_CP1_CI_AI,
	INSUMO VARCHAR(90) COLLATE SQL_Latin1_General_CP1_CI_AI,
	LISTA VARCHAR(1) COLLATE SQL_Latin1_General_CP1_CI_AI,
	PRIORIDADE INT NOT NULL,
    EXECUCAO INT NOT NULL,
	RECCREATEDBY VARCHAR(50) COLLATE SQL_Latin1_General_CP1_CI_AI NOT NULL DEFAULT 'fluig',
    RECCREATEDON DATETIME NOT NULL DEFAULT GETDATE(),
    RECMODIFIEDBY VARCHAR(50) COLLATE SQL_Latin1_General_CP1_CI_AI NOT NULL DEFAULT 'fluig',
    RECMODIFIEDON DATETIME NOT NULL DEFAULT GETDATE(),
	CONSTRAINT PK_IDCOMPEX PRIMARY KEY (OS,EXECUCAO,IDCRIACAO,IDPRD),
    CONSTRAINT FK_ITEMCOMPEX FOREIGN KEY (OS,EXECUCAO,IDCRIACAO) REFERENCES ZEXECUCAO (OS,EXECUCAO,IDCRIACAO),
	CONSTRAINT FK_ATIVIDADEEX FOREIGN KEY (OS,EXECUCAO,IDCRIACAO,PRIORIDADE) REFERENCES ZATIVIDADESEX (OS,EXECUCAO,IDCRIACAO,PRIORIDADE),
	CONSTRAINT CK_CHECKLISTAEX CHECK (LISTA IN ('S','N',NULL)),
    INDEX ATV (OS,IDCRIACAO,EXECUCAO,PRIORIDADE),
    INDEX ATVCOMP (OS,IDCRIACAO,EXECUCAO,PRIORIDADE,IDPRD),
    INDEX IDPRD (OS,IDCRIACAO,EXECUCAO,IDPRD),
    INDEX IDCRIACAO2 (OS,IDCRIACAO,EXECUCAO),
    INDEX IDCRIACAO (OS,IDCRIACAO),
	INDEX OS (OS)
)