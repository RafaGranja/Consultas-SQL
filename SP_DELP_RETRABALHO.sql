USE [CORPORE_DEV]
GO
/****** Object:  StoredProcedure [dbo].[SP_DELP_RETRABALHO]    Script Date: 28/10/2023 16:47:47 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[SP_DELP_RETRABALHO] (@NUMPROCESSO INT)

AS

BEGIN

SET NOCOUNT ON

DECLARE @MODULO VARCHAR(MAX), @TABELA VARCHAR(MAX),  @CODORDEM VARCHAR(30);

SET @MODULO = 'FACTOR';
	
	SET @TABELA = 'KORDEM';
	-- INSERE A ORDEM DE PRODUÇÃO
	

	INSERT INTO Z_CRM_LOGINTEGRACAO (NUMEROFLUIG, LINHA, STATUS, MODULO, TABELA, MENSAGEM, DATALOG) 
								VALUES  (@NUMPROCESSO, 31,'INSERT','teste ML','ML001026',ISNULL( (SELECT cast(DOCUMENTID as varchar(max)) FROM FLUIG.DBO.ML001026 (NOLOCK) WHERE NUMPROCESSO = @NUMPROCESSO ),'ML NÃO SALVA' ), GETDATE());
	

	INSERT INTO Z_CRM_LOGINTEGRACAO (NUMEROFLUIG, LINHA, STATUS, MODULO, TABELA, MENSAGEM, DATALOG) 
								VALUES  (@NUMPROCESSO, 31,'INSERT','teste ML','ML001028',ISNULL( (SELECT TOP 1 VIEWCODATIVIDADE FROM FLUIG.DBO.ML001028 (NOLOCK) WHERE DOCUMENTID = (SELECT DOCUMENTID FROM FLUIG.DBO.ML001026 (NOLOCK) WHERE NUMPROCESSO = @NUMPROCESSO) ),'ML NÃO SALVA' ), GETDATE());
	
			INSERT INTO KORDEM (CODCOLIGADA, CODCCUSTO,  CODORDEM, DSCORDEM, RESPONSAVEL, DTHRINICIALPREV, STATUS, DTHRFINALPREV, CODFILIAL, REPROCESSAMENTO, RECCREATEDBY, RECCREATEDON, RECMODIFIEDBY, RECMODIFIEDON)
			SELECT I.CODCOLIGADA, I.NUM_OS, CAST( ROW_NUMBER() OVER (ORDER BY I.CODCOLIGADA) + (SELECT VALAUTOINC FROM GAUTOINC WHERE CODAUTOINC = 'KORDEM-'+CAST(I.CODFILIAL COLLATE Latin1_General_CI_AS AS VARCHAR(2))) AS varchar(MAX))+'/'+RIGHT(CAST(YEAR(GETDATE()) AS varchar(MAX)),2),
						LEFT(P.DESCRICAO,80), P.CODIGOPRD+';'+I.NUMPROCESSO COLLATE Latin1_General_CI_AS+'- RETRABALHO', GETDATE(), 0, GETDATE(), I.CODFILIAL, 1, 'fluig', GETDATE(),'fluig', GETDATE() 
			FROM FLUIG.DBO.ML001026 (NOLOCK) I 
					INNER JOIN KESTRUTURA (NOLOCK) K ON K.CODCOLIGADA = I.CODCOLIGADA AND K.CODFILIAL = I.CODFILIAL  AND K.CODESTRUTURA = I.F_CODIGOPRD COLLATE SQL_Latin1_General_CP1_CI_AI
					INNER JOIN TPRD (NOLOCK) P ON P.CODCOLIGADA = K.CODCOLIGADA  AND P.CODIGOPRD = K.CODESTRUTURA
			WHERE I.NUMPROCESSO = @NUMPROCESSO;


		INSERT INTO Z_CRM_LOGINTEGRACAO (NUMEROFLUIG, LINHA, STATUS, MODULO, TABELA, MENSAGEM, DATALOG) 
								VALUES  (@NUMPROCESSO, 31,'INSERT',@MODULO,@TABELA,CAST(@@ROWCOUNT AS VARCHAR(MAX))+' Linhas Afetadas', GETDATE());
		
			-- INSERE O NUMERO DA EXECUÇÃO NA ORDEM DE PRODUÇÃO DE RETRABALHO
		SET @TABELA = 'KORDEMCOMPL';

	
		INSERT INTO KORDEMCOMPL (CODCOLIGADA,CODORDEM,CODFILIAL,RECCREATEDBY,RECCREATEDON,RECMODIFIEDBY,RECMODIFIEDON,NUMEXEC)
		SELECT I.CODCOLIGADA, O.CODORDEM, O.CODFILIAL, 'fluig', GETDATE(),'fluig', GETDATE(), R.NUMEXEC
		FROM FLUIG.DBO.ML001026 (NOLOCK) I
			INNER JOIN KESTRUTURA (NOLOCK) K ON K.CODCOLIGADA = I.CODCOLIGADA AND K.CODFILIAL = I.CODFILIAL AND K.CODESTRUTURA = I.F_CODIGOPRD COLLATE SQL_Latin1_General_CP1_CI_AI
			INNER JOIN KORDEM (NOLOCK) O ON O.RESPONSAVEL = I.F_CODIGOPRD+';'+I.NUMPROCESSO COLLATE Latin1_General_CI_AS+'- RETRABALHO'
			INNER JOIN KORDEMCOMPL (NOLOCK) R ON R.CODCOLIGADA = O.CODCOLIGADA AND R.CODFILIAL = O.CODFILIAL AND R.CODORDEM = I.CODORDEM COLLATE SQL_Latin1_General_CP1_CI_AI
		WHERE I.NUMPROCESSO = @NUMPROCESSO;

		INSERT INTO Z_CRM_LOGINTEGRACAO (NUMEROFLUIG, LINHA, STATUS, MODULO, TABELA, MENSAGEM, DATALOG) 
					VALUES  (@NUMPROCESSO, 48,'INSERT',@MODULO,@TABELA,CAST(@@ROWCOUNT AS VARCHAR(MAX))+' Linhas Afetadas', GETDATE());

			-- INSERE O ITEM DA ORDEM DE PRODUÇÃO
		SET @TABELA = 'KITEMORDEM';

			INSERT INTO KITEMORDEM(CODCOLIGADA, CODORDEM, CODESTRUTURA, CODMODELO, CODLINHA, STATUS, QTDEPLANEJADA, DTHRINICIALPREV, DTHRFINALPREV, IDITEMORDEM, TIPOAPONTAMENTO, CODFILIAL, CODCCUSTO,  RECCREATEDBY, RECCREATEDON, RECMODIFIEDBY, RECMODIFIEDON)
			SELECT I.CODCOLIGADA, O.CODORDEM,
					K.CODESTRUTURA, 'base', '001', 0,CASE WHEN LEFT(K.CODESTRUTURA,2) = '04' THEN 1 ELSE I.F_DESQTDE END, GETDATE(), GETDATE(), ROW_NUMBER() OVER (ORDER BY K.CODESTRUTURA)+(SELECT MAX(IDITEMORDEM) FROM KITEMORDEM), 1, I.CODFILIAL, I.NUM_OS, 'fluig', GETDATE(),'fluig', GETDATE() 
			FROM FLUIG.DBO.ML001026 (NOLOCK) I
				INNER JOIN KESTRUTURA (NOLOCK) K ON K.CODCOLIGADA = I.CODCOLIGADA AND K.CODFILIAL = I.CODFILIAL AND K.CODESTRUTURA = I.F_CODIGOPRD COLLATE SQL_Latin1_General_CP1_CI_AI
				INNER JOIN KORDEM (NOLOCK) O ON O.RESPONSAVEL = I.F_CODIGOPRD+';'+I.NUMPROCESSO COLLATE Latin1_General_CI_AS+'- RETRABALHO'
			WHERE I.NUMPROCESSO = @NUMPROCESSO;

			INSERT INTO Z_CRM_LOGINTEGRACAO (NUMEROFLUIG, LINHA, STATUS, MODULO, TABELA, MENSAGEM, DATALOG) 
								VALUES  (@NUMPROCESSO, 62,'INSERT',@MODULO,@TABELA,CAST(@@ROWCOUNT AS VARCHAR(MAX))+' Linhas Afetadas', GETDATE());

	-- TABELAS ADICIONAIS A ORDEM DE PRODUÇÃO

	-- ATIVIDADES DA ORDEM DE PRODUÇÃO
	SET @TABELA = 'KATVORDEM';

	INSERT INTO  KATVORDEM (CODCOLIGADA, CODORDEM, CODESTRUTURA, CODMODELO, CODATIVIDADE, CODETAPA, CODESTRUTURAFEITA, CODPOSTO, DTHRINICIOPREV, DTHRFINALPREV, QTESTOQUE, QTPREVISTA, QUANTIDADE,PERCENTUAL, STATUS, IDATVORDEM, IDATVDEPEND, TEMPO, DTHORAIDEAL, TIPODELAY, CODMODELOFEITO, CODFILIAL, RECCREATEDBY, RECCREATEDON, RECMODIFIEDBY, RECMODIFIEDON)
	SELECT O.CODCOLIGADA, O.CODORDEM, I.CODESTRUTURA, I.CODMODELO, A.VIEWCODATIVIDADE, 'ETP-GLOBAL' , I.CODESTRUTURA, A.VIEWCODPOSTO, I.DTHRFINALPREV, I.DTHRFINALPREV, 0, I.QTDEPLANEJADA, 0, 0, I.STATUS,ROW_NUMBER() OVER ( ORDER BY O.CODORDEM, A.VIEWPRIORIDADE COLLATE SQL_Latin1_General_CP1_CI_AI), -1, ISNULL(A.VIEWMINUTOSGASTOS,0)*I.QTDEPLANEJADA, I.DTHRFINALPREV, -1, I.CODMODELO, I.CODFILIAL, 'fluig',getdate(),'fluig',getdate() 
	FROM KORDEM O (NOLOCK)
		INNER JOIN KITEMORDEM I (NOLOCK) ON O.CODCOLIGADA = I.CODCOLIGADA AND O.CODFILIAL = I.CODFILIAL AND O.CODORDEM = I.CODORDEM
		INNER JOIN FLUIG.DBO.ML001026 M (NOLOCK) ON O.RESPONSAVEL = M.F_CODIGOPRD+';'+M.NUMPROCESSO COLLATE Latin1_General_CI_AS+'- RETRABALHO'
		INNER JOIN FLUIG.DBO.ML001028 A (NOLOCK) ON M.DOCUMENTID = A.DOCUMENTID AND M.VERSION = A.VERSION
	WHERE M.NUMPROCESSO = @NUMPROCESSO;

	INSERT INTO Z_CRM_LOGINTEGRACAO (NUMEROFLUIG, LINHA, STATUS, MODULO, TABELA, MENSAGEM, DATALOG) 
								VALUES  (@NUMPROCESSO, 59,'INSERT',@MODULO,@TABELA,CAST(@@ROWCOUNT AS VARCHAR(MAX))+' Linhas Afetadas', GETDATE());
	
	-- MATERIA PRIMA DAS ATIVIDADES DA ORDENS
	SET @TABELA = 'KATVORDEMMP';
	
	INSERT INTO KATVORDEMMP
		SELECT 
		(SELECT VALAUTOINC 
		FROM GAUTOINC 
		WHERE CODAUTOINC = 'IDATVORDEMMATERIAPRIMA' 
		AND CODSISTEMA = 'K' 
		AND CODCOLIGADA = 0) + ROW_NUMBER() OVER (ORDER BY O.CODCOLIGADA) ID, 
	O.CODCOLIGADA, O.CODORDEM, I.F_CODIGOPRD, 'base', 
	(SELECT Z.IDATVORDEM
	FROM KORDEM OA (NOLOCK)
		INNER JOIN KITEMORDEM IA (NOLOCK) ON OA.CODCOLIGADA = IA.CODCOLIGADA AND OA.CODFILIAL = IA.CODFILIAL AND OA.CODORDEM = IA.CODORDEM
		INNER JOIN FLUIG.DBO.ML001026 MA (NOLOCK) ON OA.RESPONSAVEL = MA.F_CODIGOPRD+';'+MA.NUMPROCESSO COLLATE Latin1_General_CI_AS+'- RETRABALHO'
		INNER JOIN FLUIG.DBO.ML001028 AA (NOLOCK) ON MA.DOCUMENTID = AA.DOCUMENTID AND MA.VERSION = AA.VERSION
		INNER JOIN KATVORDEM Z (NOLOCK) ON Z.CODCOLIGADA = OA.CODCOLIGADA AND Z.CODFILIAL = OA.CODFILIAL AND Z.CODORDEM = OA.CODORDEM AND Z.CODATIVIDADE = AA.VIEWCODATIVIDADE COLLATE SQL_Latin1_General_CP1_CI_AI
	WHERE OA.CODCOLIGADA = O.CODCOLIGADA
	AND   OA.CODFILIAL = O.CODFILIAL
	AND  OA.CODORDEM = O.CODORDEM
	AND AA.VIEWPRIORIDADE =  C.VIEWPRIORIDADEATVCOMPONENTES ), 
	O.DTHRINICIALPREV DTAPONTAMENTO,  C.VIEWIDPRDCOMPONENTES, NULL NSEQITMGRD, CAST(REPLACE(C.VIEWQTDETOTALCOMPONENTES,',','.') AS FLOAT) * I.F_DESQTDE QTD,
	0 EFETIVADO, NULL IDLOTE, O.CODFILIAL, CAST(REPLACE(C.VIEWQTDETOTALCOMPONENTES,',','.') AS FLOAT), 0 QTFIXA, 
		ISNULL((SELECT SALDOGERALFISICO FROM TPRD WHERE CODCOLIGADA = O.CODCOLIGADA AND  IDPRD = C.VIEWIDPRDCOMPONENTES),0) ESTOQUE, 'fluig',GETDATE(), 'fluig', GETDATE(),
	NULL ESTOQUETERCEIRO, NULL CODCOLCFO, NULL CODCFO, NULL CODFILIALMP, NULL CODLOCAL

	FROM  FLUIG.DBO.ML001026 I (NOLOCK)
		INNER JOIN FLUIG.DBO.ML001027 C (NOLOCK) ON I.DOCUMENTID = C.DOCUMENTID AND I.VERSION = C.VERSION
		INNER JOIN KORDEM O (NOLOCK) ON I.CODCOLIGADA = O.CODCOLIGADA AND I.CODFILIAL = O.CODFILIAL AND O.RESPONSAVEL = I.F_CODIGOPRD+';'+I.NUMPROCESSO COLLATE Latin1_General_CI_AS+'- RETRABALHO'
	WHERE I.NUMPROCESSO = @NUMPROCESSO;

	INSERT INTO Z_CRM_LOGINTEGRACAO (NUMEROFLUIG, LINHA, STATUS, MODULO, TABELA, MENSAGEM, DATALOG) 
								VALUES  (@NUMPROCESSO, 82,'INSERT',@MODULO,@TABELA,CAST(@@ROWCOUNT AS VARCHAR(MAX))+' Linhas Afetadas', GETDATE());

	UPDATE GAUTOINC SET VALAUTOINC  = (SELECT MAX(IDATVORDEMMATERIAPRIMA) FROM KATVORDEMMP)
	WHERE CODAUTOINC = 'IDATVORDEMMATERIAPRIMA' 
	AND CODSISTEMA = 'K' 
	AND CODCOLIGADA = 0;

	UPDATE GAUTOINC SET VALAUTOINC = (SELECT MAX(CAST(LEFT(CODORDEM, CHARINDEX('/',CODORDEM)-1) AS int)) FROM KORDEM WHERE CODCOLIGADA = 1 AND CODFILIAL = 7) WHERE CODCOLIGADA = 1 AND CODAUTOINC = 'KORDEM-7'
	
	-- DADOS COMPLEMENTARES DA ORDEM DE RETRABALHO

	INSERT INTO KATVORDEMCOMPL (CODCOLIGADA, CODORDEM, CODESTRUTURA, CODMODELO, IDATVORDEM, CODFILIAL, CAUSADOR, NUMERORNC2, RNCRR, RETRABALHO, PRIORIDADE , RECCREATEDBY, RECCREATEDON, RECMODIFIEDBY, RECMODIFIEDON )
	SELECT O.CODCOLIGADA, O.CODORDEM, I.CODESTRUTURA, I.CODMODELO, Z.IDATVORDEM, O.CODFILIAL, A.CODCLIENTECAUSADOR, A.NUMRNC, A.CODCLIENTERNCRR, A.CODCLIENTECAUSA, A.VIEWPRIORIDADE, 'fluig',getdate(),'fluig',getdate() 
	FROM KORDEM O (NOLOCK)
		INNER JOIN KITEMORDEM I (NOLOCK) ON O.CODCOLIGADA = I.CODCOLIGADA AND O.CODFILIAL = I.CODFILIAL AND O.CODORDEM = I.CODORDEM
		INNER JOIN FLUIG.DBO.ML001026 M (NOLOCK) ON O.RESPONSAVEL = M.F_CODIGOPRD+';'+M.NUMPROCESSO COLLATE Latin1_General_CI_AS+'- RETRABALHO'
		INNER JOIN FLUIG.DBO.ML001028 A (NOLOCK) ON M.DOCUMENTID = A.DOCUMENTID AND M.VERSION = A.VERSION
		INNER JOIN KATVORDEM Z (NOLOCK) ON Z.CODCOLIGADA = O.CODCOLIGADA AND Z.CODFILIAL = O.CODFILIAL AND Z.CODORDEM = O.CODORDEM AND Z.CODATIVIDADE = A.VIEWCODATIVIDADE COLLATE SQL_Latin1_General_CP1_CI_AI
		LEFT JOIN KATVORDEMCOMPL L (NOLOCK) ON L.CODCOLIGADA = Z.CODCOLIGADA AND L.CODFILIAL = Z.CODFILIAL AND L.CODORDEM = Z.CODORDEM AND L.IDATVORDEM = Z.IDATVORDEM
	WHERE L.IDATVORDEM IS NULL AND M.NUMPROCESSO = @NUMPROCESSO;

	SELECT @CODORDEM = O.CODORDEM					
	FROM FLUIG.DBO.ML001026 (NOLOCK) I
		INNER JOIN KESTRUTURA (NOLOCK) K ON K.CODCOLIGADA = I.CODCOLIGADA AND K.CODFILIAL = I.CODFILIAL AND K.CODESTRUTURA = I.F_CODIGOPRD COLLATE SQL_Latin1_General_CP1_CI_AI
		INNER JOIN KORDEM (NOLOCK) O ON O.RESPONSAVEL = I.F_CODIGOPRD+';'+I.NUMPROCESSO COLLATE Latin1_General_CI_AS+'- RETRABALHO'
	WHERE I.NUMPROCESSO = @NUMPROCESSO;

	-- ATUALIZAÇÃO DA ML DO FLUIG COM A ORDEM CRIADA
	PRINT @CODORDEM
--print 'ok'
END;


