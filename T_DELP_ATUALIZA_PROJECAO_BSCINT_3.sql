USE [CORPORE]
GO
/****** Object:  Trigger [dbo].[T_DELP_ATUALIZA_PROJECAO_BSCINT_3]    Script Date: 20/12/2024 07:24:43 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- AUTHOR:		<AUTHOR,,NAME>
-- CREATE DATE: <CREATE DATE,,>
-- DESCRIPTION:	<DESCRIPTION,,>
-- =============================================
ALTER TRIGGER [dbo].[T_DELP_ATUALIZA_PROJECAO_BSCINT_3]
   ON  [dbo].[KATVORDEMMPLOTE]
   AFTER INSERT,DELETE,UPDATE
AS 
BEGIN
	-- SET NOCOUNT ON ADDED TO PREVENT EXTRA RESULT SETS FROM
	-- INTERFERING WITH SELECT STATEMENTS.
	SET NOCOUNT ON;

    -- INSERT STATEMENTS FOR TRIGGER HERE

	BEGIN TRY
				
		SELECT MT.CODCOLIGADA,MT.IDPRJ,MP.CODPRJ,MT.IDTRF,MT.CODTRF,M.IDPRD,KML.QUANTIDADE,I.PRECOUNITARIO*KML.QUANTIDADE VALOR INTO #CTE
		FROM INSERTED KML 
		INNER JOIN KATVORDEMMP KM ON
		KML.IDATVORDEMMATERIAPRIMA=KM.IDATVORDEMMATERIAPRIMA
		AND KML.CODCOLIGADA=KM.CODCOLIGADA
		AND KML.CODFILIAL=KM.CODFILIAL
		INNER JOIN KITEMORDEM K  ON
		K.CODCOLIGADA=KM.CODCOLIGADA
		AND K.CODFILIAL=KM.CODFILIAL
		AND K.CODORDEM=KM.CODORDEM
		AND K.CODMODELO=KM.CODMODELO
		OUTER APPLY DBO.FN_DELP_RETORNA_LOTE_CCUSTO(KML.CODCOLIGADA,KML.IDLOTE,DEFAULT) I
		INNER JOIN MISMPRD M ON
		M.CODCOLIGADA=KM.CODCOLIGADA
		AND M.IDPRD=KM.IDPRODUTO
		INNER JOIN MPRJ MP ON
		MP.CODCOLIGADA=K.CODCOLIGADA
		AND MP.CODFILIAL=K.CODFILIAL
		AND MP.CODPRJ=K.CODCCUSTO
		INNER JOIN MTRF MT ON
		MT.CODCOLIGADA=MP.CODCOLIGADA
		AND MT.IDPRJ=MP.IDPRJ
		AND MT.IDISM=M.IDISM
		AND MT.IDTRF = ( SELECT MIN(IDTRF) IDTRF FROM MTRF WHERE CODCOLIGADA=MP.CODCOLIGADA AND IDPRJ=MP.IDPRJ AND IDISM=M.IDISM )	
		WHERE KM.EFETIVADO=1 AND K.CODCCUSTO!=I.CODCCUSTO AND MP.POSICAO IN (1,4)
		UNION
		SELECT MT.CODCOLIGADA,MT.IDPRJ,MP.CODPRJ,MT.IDTRF,MT.CODTRF,M.IDPRD,KML.QUANTIDADE*-1 QUANTIDADE,(I.PRECOUNITARIO*KML.QUANTIDADE)*-1 VALOR
		FROM DELETED KML 
		INNER JOIN KATVORDEMMP KM ON
		KML.IDATVORDEMMATERIAPRIMA=KM.IDATVORDEMMATERIAPRIMA
		AND KML.CODCOLIGADA=KM.CODCOLIGADA
		AND KML.CODFILIAL=KM.CODFILIAL
		INNER JOIN KITEMORDEM K  ON
		K.CODCOLIGADA=KM.CODCOLIGADA
		AND K.CODFILIAL=KM.CODFILIAL
		AND K.CODORDEM=KM.CODORDEM
		AND K.CODMODELO=KM.CODMODELO
		OUTER APPLY DBO.FN_DELP_RETORNA_LOTE_CCUSTO(KML.CODCOLIGADA,KML.IDLOTE,DEFAULT) I
		INNER JOIN MISMPRD M ON
		M.CODCOLIGADA=KM.CODCOLIGADA
		AND M.IDPRD=KM.IDPRODUTO
		INNER JOIN MPRJ MP ON
		MP.CODCOLIGADA=K.CODCOLIGADA
		AND MP.CODFILIAL=K.CODFILIAL
		AND MP.CODPRJ=K.CODCCUSTO
		INNER JOIN MTRF MT ON
		MT.CODCOLIGADA=MP.CODCOLIGADA
		AND MT.IDPRJ=MP.IDPRJ
		AND MT.IDISM=M.IDISM
		AND MT.IDTRF = ( SELECT MIN(IDTRF) IDTRF FROM MTRF WHERE CODCOLIGADA=MP.CODCOLIGADA AND IDPRJ=MP.IDPRJ AND IDISM=M.IDISM )	
		WHERE KM.EFETIVADO=1 AND K.CODCCUSTO!=I.CODCCUSTO AND MP.POSICAO IN (1,4) 



		;WITH CTE3 AS (
			
			SELECT CODCOLIGADA,IDPRJ,CODPRJ,IDTRF,CODTRF,SUM(QUANTIDADE) QUANTIDADE,SUM(VALOR) VALOR,(
			SELECT (SELECT DISTINCT CONCAT(TI3.CODORDEM,' - ',TI3.IDATIVIDADE,',') DESCRICAO
			FROM INSERTED TI2
				INNER JOIN KATVORDEMMP TI3 ON
				TI2.CODCOLIGADA=TI3.CODCOLIGADA
				AND TI2.CODFILIAL=TI3.CODFILIAL
				AND TI2.IDATVORDEMMATERIAPRIMA=TI3.IDATVORDEMMATERIAPRIMA
			UNION 			
			SELECT DISTINCT CONCAT(TI3.CODORDEM,' - ',TI3.IDATIVIDADE,',') DESCRICAO
			FROM DELETED TI2
				INNER JOIN KATVORDEMMP TI3 ON
				TI2.CODCOLIGADA=TI3.CODCOLIGADA
				AND TI2.CODFILIAL=TI3.CODFILIAL
				AND TI2.IDATVORDEMMATERIAPRIMA=TI3.IDATVORDEMMATERIAPRIMA) 
			FOR XML PATH(''),ELEMENTS ) MOVIMENTOS
			FROM #CTE 
			GROUP BY CODCOLIGADA,IDPRJ,CODPRJ,IDTRF,CODTRF
		)

		INSERT INTO FLUIG.DBO.Z_DELP_PROJECAOTAREFA(CODCOLIGADA,CODPRJ,IDTRF,PERIODO,VALOR,PRECO,MOTIVO,SEQ,RECCREATEDBY,RECCREATEDON)
		SELECT Z.CODCOLIGADA,Z.CODPRJ,Z.IDTRF,Z.PERIODO,
		CASE WHEN Z.PERIODO=CONCAT(YEAR(GETDATE()),'-',MONTH(GETDATE()))  THEN Z.VALOR-C.QUANTIDADE ELSE Z.VALOR END, 
		CASE WHEN Z.PERIODO=CONCAT(YEAR(GETDATE()),'-',MONTH(GETDATE()))  THEN Z.PRECO-C.VALOR ELSE Z.PRECO END, 
		CASE WHEN Z.PERIODO=CONCAT(YEAR(GETDATE()),'-',MONTH(GETDATE()))  THEN CONCAT(Z.MOTIVO,CHAR(13),CHAR(10),'Apontamentos realizados:',C.MOVIMENTOS) ELSE Z.MOTIVO END, 
		(SELECT MAX(SEQ) FROM FLUIG.DBO.Z_DELP_PROJECAOTAREFA WHERE CODCOLIGADA=Z.CODCOLIGADA AND CODPRJ=Z.CODPRJ AND IDTRF=Z.IDTRF)+1,
		'fluig',GETDATE()
		FROM
		FLUIG.DBO.Z_DELP_PROJECAOTAREFA Z 
		INNER JOIN CTE3 C ON
		C.CODCOLIGADA=Z.CODCOLIGADA
		AND C.CODPRJ=Z.CODPRJ COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI
		AND C.IDTRF=Z.IDTRF
		WHERE SEQ = (SELECT MAX(SEQ) FROM FLUIG.DBO.Z_DELP_PROJECAOTAREFA WHERE CODCOLIGADA=Z.CODCOLIGADA AND CODPRJ=Z.CODPRJ AND IDTRF=Z.IDTRF)

		DROP TABLE #CTE

	END TRY
	BEGIN CATCH
		PRINT 'ERRO AO EXECUTAR TRIGGER DE ATUALIZAÇÃO DO BSC'
	END CATCH


END
