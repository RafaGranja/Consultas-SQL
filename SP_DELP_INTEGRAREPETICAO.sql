USE [CORPORE_DEV]
GO
/****** Object:  StoredProcedure [dbo].[SP_DELP_INTEGRACAOREPETICAO]    Script Date: 28/10/2023 16:44:02 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


ALTER PROCEDURE [dbo].[SP_DELP_INTEGRACAOREPETICAO] (@CODCOLIGADA INT, @OS VARCHAR(30), @CODTRFPAI VARCHAR(4), @EXECUCOES VARCHAR(MAX)=NULL)
AS


BEGIN

DECLARE @CODFILIAL INT, @INDICE INT, @NUMPROCESSO INT,
		 @IDPAI INT, @IDPAIEXEC INT, @DSCEXECUCAO VARCHAR(MAX), @DATAINICIAL DATE,@EXECUCOES_TEMP VARCHAR(MAX);

SET @EXECUCOES_TEMP = CONCAT(ISNULL(@EXECUCOES,''),',');

CREATE TABLE #EXECUCOES (
EXECUCAO INT NOT NULL,
);

IF @EXECUCOES IS NULL
	BEGIN
		INSERT INTO #EXECUCOES SELECT DISTINCT EXECUCAO FROM FLUIG.DBO.Z_CRM_EX001005 WHERE OS=@OS AND CODTRFPAI=@CODTRFPAI AND CODCOLIGADA=@CODCOLIGADA
	END
ELSE 
	
	BEGIN
		WITH CTE AS 
		(	
			SELECT LEFT(@EXECUCOES_TEMP,CHARINDEX(',',@EXECUCOES_TEMP)-1) PARAM,SUBSTRING(@EXECUCOES_TEMP,CHARINDEX(',',@EXECUCOES_TEMP)+1,LEN(@EXECUCOES)) PARAM1 
			UNION ALL 
			SELECT LEFT(PARAM1,CHARINDEX(',',PARAM1)-1) PARAM,SUBSTRING(PARAM1,CHARINDEX(',',PARAM1)+1,LEN(PARAM1)) PARAM1
			FROM CTE  
			WHERE LEN(PARAM1)>1
		)
		INSERT INTO #EXECUCOES SELECT DISTINCT PARAM FROM CTE
	END
	
	

PRINT 'INICIO DENTRO DA PROC'

SELECT TOP  1  @INDICE = I.INDICE, @NUMPROCESSO  = O.NUMPROCESSO, @CODFILIAL = I.CODFILIAL, @OS = O.NUM_OS
FROM FLUIG.DBO.ML001004 O 
	INNER JOIN FLUIG.DBO.Z_CRM_ML001005 I ON O.NUM_OS = I.OS
WHERE	I.CODCOLIGADA = @CODCOLIGADA
AND		I.OS=@OS
AND		I.CODTRFPAI=@CODTRFPAI
AND		I.NIVEL = ''
ORDER BY O.NUMPROCESSO DESC;
	
	DECLARE @EXEC INT;

	DECLARE EXECUCOES CURSOR FOR 
		
			SELECT DISTINCT EXECUCAO FROM #EXECUCOES

	OPEN EXECUCOES

	FETCH NEXT FROM EXECUCOES INTO @EXEC
	
	WHILE @@FETCH_STATUS=0

	BEGIN
 
		-- ############################################################################################################################################
		--                           INICIO DA INCLUSÃO DO EX001005
		-- ############################################################################################################################################

		-- DELETA DA EX AS ML QUE NÃO TEM ORDEM DE PRODUÇÃO PARA ATUALIZAR AS EX COM AS MODIFICAÇÃO DA EDIÇÃO PRINCIPAL

		-- COMPONENTES
		DELETE E19
		FROM FLUIG.DBO.Z_CRM_EX001005 E5  
			INNER JOIN FLUIG.DBO.Z_CRM_EX001019 E19 ON E5.OS = E19.OSCOMPONENTES AND E5.IDCRIACAO = E19.IDCRIACAOCOMPONENTES AND E5.EXECUCAO = E19.EXECUCAO
			INNER JOIN FLUIG.DBO.Z_CRM_ML001005 M5 ON E5.OS = M5.OS AND E5.IDCRIACAO = M5.IDCRIACAO
			LEFT JOIN (SELECT O.CODCOLIGADA , O.CODFILIAL, I.CODESTRUTURA, C.NUMEXEC, O.CODORDEM, O.STATUS, O.RESPONSAVEL
							FROM KORDEM O
							INNER JOIN KORDEMCOMPL C ON O.CODCOLIGADA = C.CODCOLIGADA AND O.CODFILIAL = C.CODFILIAL AND O.CODORDEM = C.CODORDEM
							INNER JOIN KITEMORDEM I ON  O.CODCOLIGADA = I.CODCOLIGADA AND O.CODFILIAL = I.CODFILIAL AND O.CODORDEM = I.CODORDEM
							WHERE ISNULL(REPROCESSAMENTO,0) <> 1 AND CHARINDEX(';',O.RESPONSAVEL) <> 0 AND C.NUMEXEC IS NOT NULL ) ORDENS ON E5.CODCOLIGADA = ORDENS.CODCOLIGADA AND E5.CODFILIAL = ORDENS.CODFILIAL AND E5.CODIGOPRD = ORDENS.CODESTRUTURA COLLATE SQL_Latin1_General_CP1_CI_AI AND E5.EXECUCAO = ORDENS.NUMEXEC
		WHERE ORDENS.CODORDEM IS NULL
		AND E5.OS = @OS
		AND E5.EXECUCAO = @EXEC
		AND E5.ITEMEXCLUSIVO <> 2

		-- ATIVIADES
		DELETE E21
		FROM FLUIG.DBO.Z_CRM_EX001005 E5
			INNER JOIN FLUIG.DBO.Z_CRM_EX001021 E21 ON E5.OS = E21.OSPROCESSO AND E5.IDCRIACAO = E21.IDCRIACAOPROCESSO AND E5.EXECUCAO = E21.EXECUCAO
			INNER JOIN FLUIG.DBO.Z_CRM_ML001005 M5 ON E5.OS = M5.OS AND E5.IDCRIACAO = M5.IDCRIACAO
			LEFT JOIN (SELECT O.CODCOLIGADA , O.CODFILIAL, I.CODESTRUTURA, C.NUMEXEC, O.CODORDEM, O.STATUS, O.RESPONSAVEL
							FROM KORDEM O
							INNER JOIN KORDEMCOMPL C ON O.CODCOLIGADA = C.CODCOLIGADA AND O.CODFILIAL = C.CODFILIAL AND O.CODORDEM = C.CODORDEM
							INNER JOIN KITEMORDEM I ON  O.CODCOLIGADA = I.CODCOLIGADA AND O.CODFILIAL = I.CODFILIAL AND O.CODORDEM = I.CODORDEM
							WHERE ISNULL(REPROCESSAMENTO,0) <> 1 AND CHARINDEX(';',O.RESPONSAVEL) <> 0 AND C.NUMEXEC IS NOT NULL ) ORDENS ON E5.CODCOLIGADA = ORDENS.CODCOLIGADA AND E5.CODFILIAL = ORDENS.CODFILIAL AND E5.CODIGOPRD = ORDENS.CODESTRUTURA COLLATE SQL_Latin1_General_CP1_CI_AI AND E5.EXECUCAO = ORDENS.NUMEXEC
		WHERE ORDENS.CODORDEM IS NULL
		AND E5.OS = @OS
		AND E5.EXECUCAO = @EXEC
		AND E5.ITEMEXCLUSIVO <> 2

		-- ATUALIZAR ESTRUTURAS SEM OPs
		UPDATE E SET
					TITULOITEM= M.TITULOITEM, 
					--NIVEL= M.NIVEL, 
					POSICAO= M.POSICAO, 
					DESCRICAO= M.DESCRICAO, 
					NUMDESENHO= M.NUMDESENHO, 
					REVISAODESENHO= M.REVISAODESENHO, 
					NUMDBI= M.NUMDBI, 
					REVISAODBI= M.REVISAODBI, 
					DESQTDE= M.DESQTDE, 
					TOTALQTDE= M.TOTALQTDE, 
					BITOLAESP= M.BITOLAESP, 
					LARGURAMASSA= M.LARGURAMASSA, 
					ESPLARGURAROSCA= M.ESPLARGURAROSCA, 
					COMPRIMENTO= M.COMPRIMENTO, 
					MATERIAL= M.MATERIAL, 
					PESO= M.PESO, 
					OBSERVACOES= M.OBSERVACOES, 
					TIPO= M.TIPO, 
					SEQ= M.SEQ, 
					masterid= M.masterid, 
					PESOMATERIAL= M.PESOMATERIAL, 
					OS= M.OS, 
					DATAREVISAO= M.DATAREVISAO, 
					PESOBRUTO= M.PESOBRUTO, 
					PESOLIQUIDO= M.PESOLIQUIDO, 
					OBSERVACOESDESENHO= M.OBSERVACOESDESENHO, 
					PERIMETROCORTE= M.PERIMETROCORTE, 
					AREAPINTURA= M.AREAPINTURA, 
					OBSPROCESSO= M.OBSPROCESSO, 
					OBSGERAL= M.OBSGERAL, 
					QUANTIDADEMATERIAL= M.QUANTIDADEMATERIAL, 
					TIPODESENHO= M.TIPODESENHO, 
					POSICAOCOMPLETA= M.POSICAOCOMPLETA, 
					ESPESSURA= M.ESPESSURA, 
					BITOLA= M.BITOLA, 
					LARGURA= M.LARGURA, 
					MASSALINEAR= M.MASSALINEAR, 
					DIAMETROEXTERNO= M.DIAMETROEXTERNO, 
					DIAMETROINTERNO= M.DIAMETROINTERNO, 
					ESPROSCA= M.ESPROSCA, 
					POSICAOCOMPLETAANTIGA= M.POSICAOCOMPLETAANTIGA,
					NOVOMATERIAL= M.NOVOMATERIAL, 
					MATERIAL_ZOOM= M.MATERIAL_ZOOM, 
					IDMATERIAL= M.IDMATERIAL, 
					CODIGOPRD= M.CODIGOPRD, 
					--INDICE= M.INDICE, 
					POSICAOESTRUTURA= M.POSICAOESTRUTURA, 
					POSICAODESENHO= M.POSICAODESENHO, 
					AREASECAO= M.AREASECAO, 
					ALTURA= M.ALTURA, 
					LARGURAMESA= M.LARGURAMESA, 
					ESPALMA= M.ESPALMA, 
					ESPMESA= M.ESPMESA, 
					--POSICAOINDICE= M.POSICAOINDICE, 
					INDICEANTIGO= M.INDICEANTIGO, 
					IDCRIACAO= M.IDCRIACAO,
					IDCRIACAOPAI = M.IDCRIACAOPAI,
					PRODUTORM= M.PRODUTORM, 
					IDPRD= M.IDPRD, 
					EXPANSOR= M.EXPANSOR, 
					CODIGOPRDMATERIAL= M.CODIGOPRDMATERIAL, 
					CODTRFOS= M.CODTRFOS, 
					UNDMEDIDA= M.UNDMEDIDA, 
					ORDEM= M.ORDEM, 
					LARGURAABA= M.LARGURAABA, 
					ESPABA= M.ESPABA, 
					INTEGRADO= M.INTEGRADO, 
					DSCTRFOS= M.DSCTRFOS,
					SOLICITACAO= M.SOLICITACAO, 
					PAIDETALHADO= M.PAIDETALHADO, 
					COMPORLISTA= M.COMPORLISTA, 
					CODTRFITEM= M.CODTRFITEM, 
					IDTRFITEM= M.IDTRFITEM, 
					NOMETRFITEM= M.NOMETRFITEM, 
					CODIGOTAREFADESC= M.CODIGOTAREFADESC, 
					IDPRJOS= M.IDPRJOS,
					DESCOS= M.DESCOS, 
					DETALHADO= M.DETALHADO, 
					NUMDOCDELP= M.NUMDOCDELP, 
					REVISAODOCDELP= M.REVISAODOCDELP, 
					CODCOLIGADA= M.CODCOLIGADA, 
					DIAMETROEXTERNODISCO= M.DIAMETROEXTERNODISCO,
					DIAMETROINTERNODISCO= M.DIAMETROINTERNODISCO,
					CHECKUSINAGEM= M.CHECKUSINAGEM, 
					CHECKCALDERARIA= M.CHECKCALDERARIA, 
					CODFILIAL= M.CODFILIAL, 
					--CODTRFPAI= M.CODTRFPAI, 
					EXECINTEGRADAS= M.EXECINTEGRADAS, 
					--IDTRFPAI= M.IDTRFPAI, 
					--NOMETRFPAI= M.NOMETRFPAI, 
					EXECUCOES= M.EXECUCOES, 
					PESOUNITARIO= M.PESOUNITARIO, 
					OPSUNITARIAS= M.OPSUNITARIAS, 
					QTDEUNCOMP= M.QTDEUNCOMP, 
					ITEMEXCLUSIVO= 0,
					RECCREATEDBY= M.RECCREATEDBY,
					RECCREATEDON= M.RECCREATEDON,
					RECMODIFIEDBY= M.RECMODIFIEDBY,
					RECMODIFIEDON = M.RECMODIFIEDON
		FROM FLUIG.DBO.Z_CRM_EX001005 E  
						INNER JOIN FLUIG.DBO.Z_CRM_ML001005 M ON E.OS = M.OS AND E.IDCRIACAO = M.IDCRIACAO
						LEFT JOIN (SELECT O.CODCOLIGADA , O.CODFILIAL, I.CODESTRUTURA, C.NUMEXEC, O.CODORDEM, O.STATUS, O.RESPONSAVEL
										FROM KORDEM O
										INNER JOIN KORDEMCOMPL C ON O.CODCOLIGADA = C.CODCOLIGADA AND O.CODFILIAL = C.CODFILIAL AND O.CODORDEM = C.CODORDEM
										INNER JOIN KITEMORDEM I ON  O.CODCOLIGADA = I.CODCOLIGADA AND O.CODFILIAL = I.CODFILIAL AND O.CODORDEM = I.CODORDEM
										WHERE ISNULL(REPROCESSAMENTO,0) <> 1 AND CHARINDEX(';',O.RESPONSAVEL) <> 0 AND C.NUMEXEC IS NOT NULL ) ORDENS ON E.CODCOLIGADA = ORDENS.CODCOLIGADA AND E.CODFILIAL = ORDENS.CODFILIAL AND E.CODIGOPRD = ORDENS.CODESTRUTURA COLLATE SQL_Latin1_General_CP1_CI_AI AND E.EXECUCAO = ORDENS.NUMEXEC
		WHERE ORDENS.CODORDEM IS NULL
		AND E.OS = @OS
		AND E.EXECUCAO = @EXEC
		AND E.ITEMEXCLUSIVO <> 2
			


			DECLARE  @ID INT, @IDCRIACAOPAI INT, @NIVEL VARCHAR(MAX), @IDCRIACAO INT, @POSICAOINDICE INT
						-- CRIA CÓPIA DA EXECUÇÃO


			--RENUMERA

			DECLARE EXECUCAO CURSOR FOR

										SELECT	M.ID, M.IDCRIACAO, M.IDCRIACAOPAI
										FROM FLUIG.dbo.Z_CRM_ML001005 M (NOLOCK)
											LEFT JOIN FLUIG.dbo.Z_CRM_EX001005 E (NOLOCK) ON M.ID = E.ID AND M.OS = E.OS AND E.EXECUCAO = @EXEC
										WHERE E.ID IS NULL AND  M.OS = @OS AND CASE WHEN CHARINDEX('.',M.INDICE) <> 0 THEN CAST(LEFT(M.INDICE,CHARINDEX('.',M.INDICE)-1) AS int) ELSE CAST(M.INDICE AS int) END =  @INDICE
										ORDER BY CAST('/' + REPLACE(M.INDICE, '.', '/') + '/' AS HIERARCHYID)

		
			OPEN EXECUCAO

			FETCH NEXT FROM EXECUCAO INTO @ID, @IDCRIACAO, @IDCRIACAOPAI

			WHILE @@FETCH_STATUS = 0
			BEGIN

					-- INSERE ITENS EXCLUIVOS CORRETAMENTE
					INSERT INTO FLUIG.dbo.Z_CRM_EX001005  (	 ID, companyid, cardid, documentid, version, tableid, TITULOITEM, NIVEL, POSICAO, DESCRICAO, NUMDESENHO, REVISAODESENHO, NUMDBI, REVISAODBI, DESQTDE, TOTALQTDE, BITOLAESP, LARGURAMASSA, 
																ESPLARGURAROSCA, COMPRIMENTO, MATERIAL, PESO, OBSERVACOES, TIPO, SEQ, masterid, PESOMATERIAL, OS, DATAREVISAO, PESOBRUTO, PESOLIQUIDO, OBSERVACOESDESENHO, PERIMETROCORTE, AREAPINTURA, 
																OBSPROCESSO, OBSGERAL, QUANTIDADEMATERIAL, TIPODESENHO, POSICAOCOMPLETA, ESPESSURA, BITOLA, LARGURA, MASSALINEAR, DIAMETROEXTERNO, DIAMETROINTERNO, ESPROSCA, POSICAOCOMPLETAANTIGA, 
																NOVOMATERIAL, MATERIAL_ZOOM, IDMATERIAL, CODIGOPRD,  POSICAOINDICE,  POSICAOESTRUTURA, POSICAODESENHO, AREASECAO, ALTURA, LARGURAMESA, ESPALMA, ESPMESA, INDICE, INDICEANTIGO, IDCRIACAO, 
																PRODUTORM, IDPRD, EXPANSOR, CODIGOPRDMATERIAL, CODTRFOS, UNDMEDIDA, ORDEM, LARGURAABA, ESPABA, INTEGRADO, DSCTRFOS, SOLICITACAO, PAIDETALHADO, COMPORLISTA, CODTRFITEM, IDTRFITEM, 
																NOMETRFITEM, CODIGOTAREFADESC, IDPRJOS, DESCOS, DETALHADO, NUMDOCDELP, REVISAODOCDELP, CODCOLIGADA, DIAMETROEXTERNODISCO, DIAMETROINTERNODISCO, CHECKUSINAGEM, CHECKCALDERARIA, CODFILIAL, 
																CODTRFPAI, EXECINTEGRADAS, IDTRFPAI, NOMETRFPAI, EXECUCOES, PESOUNITARIO, OPSUNITARIAS, QTDEUNCOMP, OP, EXECUCAO, CODTRFEX, IDCRIACAOPAI,ITEMEXCLUSIVO, RECCREATEDBY,RECCREATEDON,RECMODIFIEDBY,RECMODIFIEDON)

							
						SELECT	M.ID, M.companyid, M.cardid, M.documentid, M.version, M.tableid, M.TITULOITEM, 
								ISNULL((   SELECT TOP 1 INDICE
											FROM (
												SELECT INDICE , 1 ORDEM FROM FLUIG.dbo.Z_CRM_EX001005 (NOLOCK) WHERE OS = M.OS AND IDCRIACAO = M.IDCRIACAOPAI AND EXECUCAO = @EXEC
												UNION ALL 
												SELECT INDICE , 2 ORDEM FROM FLUIG.dbo.Z_CRM_ML001005 (NOLOCK) WHERE OS = M.OS AND IDCRIACAO = M.IDCRIACAOPAI
										) AS NIVEL ORDER BY ORDEM ), M.NIVEL) NIVEL, 
							M.POSICAO, M.DESCRICAO, M.NUMDESENHO, M.REVISAODESENHO, M.NUMDBI, M.REVISAODBI, M.DESQTDE, 
							M.TOTALQTDE, M.BITOLAESP, M.LARGURAMASSA, M.ESPLARGURAROSCA, M.COMPRIMENTO, M.MATERIAL, M.PESO, M.OBSERVACOES, M.TIPO, M.SEQ, M.masterid, M.PESOMATERIAL, M.OS, M.DATAREVISAO, M.PESOBRUTO, 
							M.PESOLIQUIDO, M.OBSERVACOESDESENHO, M.PERIMETROCORTE, M.AREAPINTURA, M.OBSPROCESSO, M.OBSGERAL, M.QUANTIDADEMATERIAL, M.TIPODESENHO, M.POSICAOCOMPLETA, M.ESPESSURA, M.BITOLA, M.LARGURA, 
							M.MASSALINEAR, M.DIAMETROEXTERNO, M.DIAMETROINTERNO, M.ESPROSCA, M.POSICAOCOMPLETAANTIGA, M.NOVOMATERIAL, M.MATERIAL_ZOOM, M.IDMATERIAL, M.CODIGOPRD, 
							ISNULL((   SELECT MAX(CAST(POSICAOINDICE AS INT))
										FROM FLUIG.dbo.Z_CRM_EX001005 (NOLOCK)
										WHERE OS = M.OS AND IDCRIACAOPAI = M.IDCRIACAOPAI AND EXECUCAO = @EXEC ),0) +1 POSICAOINDICE, M.POSICAOESTRUTURA, 
							M.POSICAODESENHO, M.AREASECAO, M.ALTURA, M.LARGURAMESA, M.ESPALMA, M.ESPMESA, ISNULL(( SELECT TOP 1 INDICE
																													FROM (
																														SELECT INDICE , 1 ORDEM FROM FLUIG.dbo.Z_CRM_EX001005 (NOLOCK) WHERE  OS = M.OS AND IDCRIACAO = M.IDCRIACAOPAI AND EXECUCAO = @EXEC
																														UNION ALL 
																														SELECT INDICE , 2 ORDEM FROM FLUIG.dbo.Z_CRM_ML001005 (NOLOCK) WHERE  OS = M.OS AND IDCRIACAO = M.IDCRIACAOPAI
																												) AS NIVEL ORDER BY ORDEM )
																													+'.'+
																													CAST(ISNULL((   SELECT MAX(CAST(POSICAOINDICE AS INT))
																																	FROM FLUIG.dbo.Z_CRM_EX001005 (NOLOCK)
																																	WHERE OS = M.OS AND IDCRIACAOPAI = M.IDCRIACAOPAI AND EXECUCAO = @EXEC ),0) +1 AS VARCHAR(MAX)),M.INDICE) INDICE, 
							M.INDICEANTIGO, M.IDCRIACAO, M.PRODUTORM, M.IDPRD, M.EXPANSOR, M.CODIGOPRDMATERIAL, M.CODTRFOS, M.UNDMEDIDA, M.ORDEM, M.LARGURAABA, M.ESPABA, M.INTEGRADO, M.DSCTRFOS, M.SOLICITACAO, NULL PAIDETALHADO, 
							M.COMPORLISTA, M.CODTRFITEM, M.IDTRFITEM, M.NOMETRFITEM, M.CODIGOTAREFADESC, M.IDPRJOS, M.DESCOS, NULL DETALHADO, M.NUMDOCDELP, M.REVISAODOCDELP, M.CODCOLIGADA, M.DIAMETROEXTERNODISCO, M.DIAMETROINTERNODISCO, 
							M.CHECKUSINAGEM, M.CHECKCALDERARIA, M.CODFILIAL, M.CODTRFPAI, M.EXECINTEGRADAS, M.IDTRFPAI, M.NOMETRFPAI, M.EXECUCOES, M.PESOUNITARIO, M.OPSUNITARIAS, M.QTDEUNCOMP, NULL, @EXEC, NULL , M.IDCRIACAOPAI, 0,
							M.RECCREATEDBY, M.RECCREATEDON, M.RECMODIFIEDBY, M.RECMODIFIEDON
					FROM FLUIG.DBO.Z_CRM_ML001005 M (NOLOCK)
					WHERE  M.OS = @OS AND IDCRIACAO = @IDCRIACAO
		
					FETCH NEXT FROM EXECUCAO INTO @ID, @IDCRIACAO, @IDCRIACAOPAI

			END

			CLOSE EXECUCAO
			DEALLOCATE EXECUCAO


							   
		-- ############################################################################################################################################
		--                           FIM DA INCLUSÃO DO EX001005
		-- ############################################################################################################################################
			UPDATE FLUIG.dbo.Z_CRM_EX001005 SET CODTRFPAI = @CODTRFPAI
			WHERE OS = @OS AND EXECUCAO = @EXEC AND  CASE WHEN CHARINDEX('.',INDICE) <> 0 THEN CAST(LEFT(INDICE,CHARINDEX('.',INDICE)-1) AS int) ELSE CAST(INDICE AS int) END =  @INDICE;

		
			INSERT INTO FLUIG.dbo.Z_CRM_EX001021  
			SELECT  M.ID,M.companyid,M.cardid,M.documentid,M.version,M.tableid,M.PRIORIDADE,M.IDCRIACAOPROCESSO,M.CODATIVIDADE,M.OSPROCESSO,M.IDPROCESSO,M.DESCATIVIDADE,
					M.HABILIDADEREQUERIDA,M.CODHABILIDADE,M.CODPOSTO,M.DESCPOSTO,M.FILA,M.CONFIGURACAO,M.PROCESSAMENTO,M.DESAGREGACAO,M.ESPERA,M.MOVIMENTACAO,M.MINUTOSGASTOS,
					M.DESCPROCESSO,M.DOCAPOIOATV1,M.DOCAPOIOATV2,M.DOCAPOIOATV3,M.DOCAPOIOATV4,M.masterid,M.SOLICITACAOPROCESSO,M.FORNPARA,M.INTEGRADOPROCESSO,I.EXECUCAO , NULL,
					M.RECCREATEDBY,M.RECCREATEDON,M.RECMODIFIEDBY,M.RECMODIFIEDON
			FROM  FLUIG.dbo.Z_CRM_ML001021 M (NOLOCK)
				INNER JOIN FLUIG.dbo.Z_CRM_EX001005 I (NOLOCK)  ON I.OS = M.OSPROCESSO AND M.IDCRIACAOPROCESSO = I.IDCRIACAO
				LEFT JOIN FLUIG.dbo.Z_CRM_EX001021 E (NOLOCK) ON M.IDCRIACAOPROCESSO = E.IDCRIACAOPROCESSO AND M.CODATIVIDADE = E.CODATIVIDADE AND I.OS = E.OSPROCESSO AND E.EXECUCAO = I.EXECUCAO
			WHERE E.ID IS NULL AND I.ITEMEXCLUSIVO = 0 AND M.OSPROCESSO = @OS AND  CASE WHEN CHARINDEX('.',I.INDICE) <> 0 THEN CAST(LEFT(I.INDICE,CHARINDEX('.',I.INDICE)-1) AS int) ELSE CAST(I.INDICE AS int) END =  @INDICE AND I.EXECUCAO = @EXEC


			INSERT INTO FLUIG.dbo.Z_CRM_EX001019 
			SELECT M.ID,M.companyid,M.cardid,M.documentid,M.version,M.tableid,M.PRODUTOCOMPONENTES,M.IDPRDCOMPONENTES,M.CODIGOPRDCOMPONENTES,M.CODUNDCOMPONENTES,
					M.IDCRIACAOCOMPONENTES,M.QTDEUNCOMPONENTES,M.QTDETOTALCOMPONENTES,M.SUBSTITUTOCOMPONENTES,M.INSUMOCOMPONENTES,M.LISTACOMPONENTES,M.masterid,
					M.IDCOMPONENTES,M.OSCOMPONENTES,M.SOLICITACAOCOMPONENTES,M.INTEGRADOCOMPONENTES,M.PRIORIDADEATVCOMPONENTES, I.EXECUCAO, NULL,M.RECCREATEDBY,
					M.RECCREATEDON,M.RECMODIFIEDBY,M.RECMODIFIEDON
			FROM  FLUIG.dbo.Z_CRM_ML001019 M  (NOLOCK)
				INNER JOIN FLUIG.dbo.Z_CRM_EX001005 I (NOLOCK) ON I.OS = M.OSCOMPONENTES AND M.IDCRIACAOCOMPONENTES = I.IDCRIACAO
				INNER JOIN FLUIG.dbo.Z_CRM_EX001021 P (NOLOCK) ON  I.OS = P.OSPROCESSO AND P.IDCRIACAOPROCESSO = I.IDCRIACAO AND M.PRIORIDADEATVCOMPONENTES = P.PRIORIDADE AND P.EXECUCAO = I.EXECUCAO
				LEFT JOIN FLUIG.dbo.Z_CRM_EX001019 E (NOLOCK) ON M.OSCOMPONENTES = E.OSCOMPONENTES AND M.IDCRIACAOCOMPONENTES = E.IDCRIACAOCOMPONENTES AND M.PRIORIDADEATVCOMPONENTES = E.PRIORIDADEATVCOMPONENTES AND M.IDPRDCOMPONENTES = E.IDPRDCOMPONENTES  AND E.EXECUCAO = P.EXECUCAO
			WHERE E.ID IS NULL AND I.ITEMEXCLUSIVO = 0 AND M.OSCOMPONENTES = @OS AND  CASE WHEN CHARINDEX('.',I.INDICE) <> 0 THEN CAST(LEFT(I.INDICE,CHARINDEX('.',I.INDICE)-1) AS int) ELSE CAST(I.INDICE AS int) END =  @INDICE AND I.EXECUCAO = @EXEC
		
			INSERT INTO FLUIG.dbo.Z_CRM_EX001019 
			SELECT M.ID,M.companyid,M.cardid,M.documentid,M.version,M.tableid,M.PRODUTOCOMPONENTES,M.IDPRDCOMPONENTES,M.CODIGOPRDCOMPONENTES,M.CODUNDCOMPONENTES,
					M.IDCRIACAOCOMPONENTES,M.QTDEUNCOMPONENTES,M.QTDETOTALCOMPONENTES,M.SUBSTITUTOCOMPONENTES,M.INSUMOCOMPONENTES,M.LISTACOMPONENTES,M.masterid,
					M.IDCOMPONENTES,M.OSCOMPONENTES,M.SOLICITACAOCOMPONENTES,M.INTEGRADOCOMPONENTES,M.PRIORIDADEATVCOMPONENTES, I.EXECUCAO, NULL,M.RECCREATEDBY,
					M.RECCREATEDON,M.RECMODIFIEDBY,M.RECMODIFIEDON
			FROM  FLUIG.dbo.Z_CRM_ML001019 M  (NOLOCK)
				INNER JOIN FLUIG.dbo.Z_CRM_EX001005 I (NOLOCK) ON I.OS = M.OSCOMPONENTES AND M.IDCRIACAOCOMPONENTES = I.IDCRIACAO
				LEFT JOIN FLUIG.dbo.Z_CRM_EX001021 P (NOLOCK) ON  I.OS = P.OSPROCESSO AND P.IDCRIACAOPROCESSO = I.IDCRIACAO AND M.PRIORIDADEATVCOMPONENTES = P.PRIORIDADE AND P.EXECUCAO = I.EXECUCAO
				LEFT JOIN FLUIG.dbo.Z_CRM_EX001019 E (NOLOCK) ON M.OSCOMPONENTES = E.OSCOMPONENTES AND M.IDCRIACAOCOMPONENTES = E.IDCRIACAOCOMPONENTES AND M.PRIORIDADEATVCOMPONENTES = E.PRIORIDADEATVCOMPONENTES AND M.IDPRDCOMPONENTES = E.IDPRDCOMPONENTES  AND E.EXECUCAO = I.EXECUCAO
			WHERE P.ID IS NULL AND  E.ID IS NULL AND I.ITEMEXCLUSIVO = 0 AND M.OSCOMPONENTES = @OS AND  CASE WHEN CHARINDEX('.',I.INDICE) <> 0 THEN CAST(LEFT(I.INDICE,CHARINDEX('.',I.INDICE)-1) AS int) ELSE CAST(I.INDICE AS int) END =  @INDICE AND I.EXECUCAO = @EXEC
		
		EXEC SP_DELP_EDICAOEXECUCAO @NUMPROCESSO,@CODTRFPAI,@EXEC,@OS

		FETCH NEXT FROM EXECUCOES INTO @EXEC

	END;

	CLOSE EXECUCOES;
	DEALLOCATE EXECUCOES;
		

	UPDATE FLUIG.dbo.Z_CRM_EX001005 SET ITEMEXCLUSIVO = 1
	WHERE OS = @OS AND ITEMEXCLUSIVO = 0;

	DROP TABLE #EXECUCOES

END;

