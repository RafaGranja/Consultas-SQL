USE [CORPORE]
GO

/****** Object:  UserDefinedFunction [dbo].[FN_DELP_RETORNA_VALORES_TAREFA]    Script Date: 14/08/2024 09:30:03 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:		RAFAEL.GRANJA
-- Create date: 13/08/2024
-- Description:	BUSCA NAS TABELAS DO SOLUM AS INFORMAÇÕES DE TAREFAS E FAZ A RASTREABILIDADE DE SOLICITAÇÕES DE COMPRA, ORDEM DE COMPRA, NOTA FISCAL, LOTES DA NOTA FISCAL E APONTAMENTO DOS LOTES
-- =============================================
CREATE FUNCTION [dbo].[FN_DELP_RETORNA_VALORES_TAREFA]
(	
	@CODCOLIGADA INT,
	@CODPRJ VARCHAR(50),
	@IDTRF INT=NULL,
	@IDPRD INT=NULL,
	@IDMOV INT=NULL
)
RETURNS TABLE 
AS
RETURN 
(
	WITH CTE AS (
		SELECT
			TI.CODTMV,
			TI.NUMEROMOV,
			TI.SERIE,
			MT.CODCOLIGADA,
			TI.QUANTIDADEORIGINAL,
			TI.QUANTIDADEARECEBER,
			TI.PRECOUNITARIO,
			TI.IDPRD,
			TI.IDMOV,
			NULL IDMOVPAI,
			TI.NSEQITMMOV,
			TI.IDMOV IDMOVSC,
			TI.NUMEROMOV NUMEROSC
		FROM
			MPRJ P
			INNER JOIN MTRF MT ON MT.CODCOLIGADA = P.CODCOLIGADA
			AND MT.IDPRJ = P.IDPRJ
			INNER JOIN MITEMPEDIDOMATEXTRA M ON P.CODCOLIGADA = M.CODCOLIGADA
			AND P.IDPRJ = M.IDPRJ
			AND MT.IDTRF = M.IDTRF
			CROSS APPLY DBO.FN_DELP_RETORNA_ITEM_MOVIMENTO(M.CODCOLIGADA,M.IDMOV,M.NSEQITMMOV) TI
		WHERE
			P.CODPRJ = @CODPRJ
			AND P.CODCOLIGADA=@CODCOLIGADA
			AND MT.IDTRF=ISNULL(@IDTRF,MT.IDTRF)
			AND M.IDPRD=ISNULL(@IDPRD,M.IDPRD)
			AND M.IDMOV=ISNULL(@IDMOV,M.IDMOV)
			AND P.POSICAO IN (1, 4)
			AND ISNULL(TI.STATUSMOV, '') NOT IN ('C')
			AND SUBSTRING(CODTRF, CHARINDEX('.', CODTRF, 0) + 1, LEN(CODTRF)) NOT LIKE '06.%'
			AND (
				SUBSTRING(CODTRF, CHARINDEX('.', CODTRF, 0) + 1, LEN(CODTRF)) NOT LIKE '09.%'
				OR CODTRF = '1.09.001.004'
			)
			AND LEN(MT.CODTRF) >= LEN('1.01.001.052')
		UNION
		ALL
		SELECT
			TI.CODTMV,
			TI.NUMEROMOV,
			TI.SERIE,
			C.CODCOLIGADA,
			TI.QUANTIDADEORIGINAL,
			TI.QUANTIDADEARECEBER,
			TI.PRECOUNITARIO,
			TI.IDPRD,
			TI.IDMOV,
			C.IDMOV IDMOVPAI,
			TI.NSEQITMMOV,
			IDMOVSC,
			NUMEROSC
		FROM
			CTE C
			CROSS APPLY DBO.FN_DELP_RETORNA_ITEM_MOVIMENTO_DESTINO(C.CODCOLIGADA,C.IDMOV,C.NSEQITMMOV) TI
		WHERE
			TI.STATUSMOV NOT IN ('C')
	),
	C2 AS (
		SELECT
			CASE
				WHEN C.CODTMV LIKE '1.1%'
				AND C.SERIE != 'OC'
				AND C.SERIE LIKE 'S%' THEN 'SC'
				WHEN C.CODTMV LIKE '1.1%'
				AND C.SERIE = 'OC' THEN 'OC'
				WHEN C.CODTMV LIKE '1.2%' THEN 'NF'
			END TIPO,
			P.TIPO TIPOP,
			C.QUANTIDADEORIGINAL QUANTIDADETOTAL,
			(C.QUANTIDADEORIGINAL * C.PRECOUNITARIO) VALORTOTAL,
			C.QUANTIDADEARECEBER QUANTIDADESALDO,
			(C.QUANTIDADEARECEBER * C.PRECOUNITARIO) VALORSALDO,
			C.PRECOUNITARIO,
			ISNULL(A.QUANTIDADE, 0) QTDLOTE,
			ISNULL(A.VALOR, 0) VALORLOTE
		FROM
			CTE C
			INNER JOIN TPRD P ON P.CODCOLIGADA = C.CODCOLIGADA
			AND P.IDPRD = C.IDPRD
			LEFT JOIN TITMLOTEPRD L ON L.CODCOLIGADA = C.CODCOLIGADA
			AND L.IDMOV = C.IDMOV
			AND L.NSEQITMMOV = C.NSEQITMMOV
			OUTER APPLY DBO.FN_DELP_RETORNA_APONT_LOTE(L.CODCOLIGADA,L.IDLOTE,DEFAULT) A
	)
	SELECT 
	SUM(SCQTDTOTAL) SCQTDTOTAL,
	SUM(SCVALORTOTAL) SCVALORTOTAL,
	SUM(SCQTDSALDO) SCQTDSALDO,
	SUM(SCVALORSALDO) SCVALORSALDO,
	SUM(OCQTDTOTAL) OCQTDTOTAL,
	SUM(OCVALORTOTAL) OCVALORTOTAL,
	SUM(OCQTDSALDO) OCQTDSALDO,
	SUM(OCVALORSALDO) OCVALORSALDO,
	SUM(NFQTDTOTAL) NFQTDTOTAL,
	SUM(NFVALORTOTAL) NFVALORTOTAL,
	SUM(NFQTDSALDO) NFQTDSALDO,
	SUM(NFVALORSALDO) NFVALORSALDO,
	SUM(QTDAPONTAMENTO) QTDAPONTAMENTO,
	SUM(VALORAPONTAMENTO) VALORAPONTAMENTO
	FROM (
	SELECT
	SCQTDTOTAL,
	SCVALORTOTAL,
	SCQTDSALDO,
	SCVALORSALDO,
	OCQTDTOTAL,
	OCVALORTOTAL,
	OCQTDSALDO,
	OCVALORSALDO,
	NFQTDTOTAL,
	NFVALORTOTAL,
	NFQTDSALDO - QTDAPONTAMENTO NFQTDSALDO,
	NFVALORSALDO - VALORAPONTAMENTO NFVALORSALDO,
	QTDAPONTAMENTO,
	VALORAPONTAMENTO
	FROM
		(
			SELECT
				CAST(SUM(ISNULL(SCQTDTOTAL, 0)) AS DECIMAL(22, 4)) SCQTDTOTAL,
				CAST(SUM(ISNULL(SCVALORTOTAL, 0)) AS DECIMAL(22, 4)) SCVALORTOTAL,
				CAST(SUM(ISNULL(SCQTDSALDO, 0)) AS DECIMAL(22, 4)) SCQTDSALDO,
				CAST(SUM(ISNULL(SCVALORSALDO, 0)) AS DECIMAL(22, 4)) SCVALORSALDO,
				CAST(SUM(ISNULL(OCQTDTOTAL, 0)) AS DECIMAL(22, 4)) OCQTDTOTAL,
				CAST(SUM(ISNULL(OCVALORTOTAL, 0)) AS DECIMAL(22, 4)) OCVALORTOTAL,
				CAST(SUM(ISNULL(OCQTDSALDO, 0)) AS DECIMAL(22, 4)) OCQTDSALDO,
				CAST(SUM(ISNULL(OCVALORSALDO, 0)) AS DECIMAL(22, 4)) OCVALORSALDO,
				CAST(SUM(ISNULL(NFQTDTOTAL, 0)) AS DECIMAL(22, 4)) NFQTDTOTAL,
				CAST(SUM(ISNULL(NFVALORTOTAL, 0)) AS DECIMAL(22, 4)) NFVALORTOTAL,
				CAST(SUM(ISNULL(NFQTDSALDO, 0)) AS DECIMAL(22, 4)) NFQTDSALDO,
				CAST(SUM(ISNULL(NFVALORSALDO, 0)) AS DECIMAL(22, 4)) NFVALORSALDO,
				CAST(
					SUM(
						ISNULL(
							CASE
								WHEN TIPOP != 'P' THEN NFQTDTOTAL
								ELSE QTDLOTE
							END,
							0
						)
					) AS DECIMAL(22, 4)
				) QTDAPONTAMENTO,
				CAST(
					SUM(
						ISNULL(
							CASE
								WHEN TIPOP != 'P' THEN NFVALORTOTAL
								ELSE VALORLOTE
							END,
							0
						)
					) AS DECIMAL(22, 4)
				) VALORAPONTAMENTO
			FROM
				(
					SELECT
						SUM(
							CASE
								WHEN TIPO = 'SC' THEN QUANTIDADETOTAL
								ELSE 0
							END
						) AS SCQTDTOTAL,
						SUM(
							CASE
								WHEN TIPO = 'SC' THEN VALORTOTAL
								ELSE 0
							END
						) AS SCVALORTOTAL,
						SUM(
							CASE
								WHEN TIPO = 'SC' THEN QUANTIDADESALDO
								ELSE 0
							END
						) AS SCQTDSALDO,
						SUM(
							CASE
								WHEN TIPO = 'SC' THEN VALORSALDO
								ELSE 0
							END
						) AS SCVALORSALDO,
						SUM(
							CASE
								WHEN TIPO = 'OC' THEN QUANTIDADETOTAL
								ELSE 0
							END
						) AS OCQTDTOTAL,
						SUM(
							CASE
								WHEN TIPO = 'OC' THEN VALORTOTAL
								ELSE 0
							END
						) AS OCVALORTOTAL,
						SUM(
							CASE
								WHEN TIPO = 'OC' THEN QUANTIDADESALDO
								ELSE 0
							END
						) AS OCQTDSALDO,
						SUM(
							CASE
								WHEN TIPO = 'OC' THEN VALORSALDO
								ELSE 0
							END
						) AS OCVALORSALDO,
						SUM(
							CASE
								WHEN TIPO = 'NF' THEN QUANTIDADETOTAL
								ELSE 0
							END
						) AS NFQTDTOTAL,
						SUM(
							CASE
								WHEN TIPO = 'NF' THEN VALORTOTAL
								ELSE 0
							END
						) AS NFVALORTOTAL,
						SUM(
							CASE
								WHEN TIPO = 'NF' THEN QUANTIDADESALDO
								ELSE 0
							END
						) AS NFQTDSALDO,
						SUM(
							CASE
								WHEN TIPO = 'NF' THEN VALORSALDO
								ELSE 0
							END
						) AS NFVALORSALDO,
						QTDLOTE,
						VALORLOTE,
						TIPO,
						TIPOP
					FROM
						C2
						GROUP BY QTDLOTE,VALORLOTE,TIPO,TIPOP
				) R
		) R
		UNION 
		SELECT
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0
		) R2
)
GO

