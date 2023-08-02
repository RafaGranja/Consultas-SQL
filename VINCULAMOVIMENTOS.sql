-- ================================================
-- Template generated from Template Explorer using:
-- Create Procedure (New Menu).SQL
--
-- Use the Specify Values for Template Parameters 
-- command (Ctrl-Shift-M) to fill in the parameter 
-- values below.
--
-- This block of comments will not be included in
-- the definition of the procedure.
-- ================================================
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE VINCULAPAPC  @CODCOLIGADA INT, @IDMOVORIGEM INT, @IDMOVDESTINO INT
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

    WITH CTE_TMOV AS (
		SELECT T.IDMOV IDMOVORIGEM,	@CODCOLIGADA CODCOLORIGEM, T2.IDMOV IDMOVDESTINO,	@CODCOLIGADA CODCOLDESTINO,	'P' TIPORELAC,	'' IDPROCESSO,	T.RECCREATEDBY,	T.RECCREATEDON,	T.RECMODIFIEDBY, T.RECMODIFIEDON ,	NULL VALORRECEBIDO
		FROM 
			TMOV T INNER JOIN TMOV T2 
			ON T2.CODCOLIGADA=T.CODCOLIGADA
			WHERE T.IDMOV=@IDMOVORIGEM AND T2.IDMOV=@IDMOVDESTINO
	), CTE_ITEM AS (
		SELECT T.IDMOV IDMOVORIGEM, T.NSEQITMMOV NSEQITMMOVORIGEM, @CODCOLIGADA CODCOLORIGEM, T2.IDMOV IDMOVDESTINO, T2.NSEQITMMOV NSEQITMMOVDESTINO,	@CODCOLIGADA CODCOLDESTINO,	T.QUANTIDADE ,	T.RECCREATEDBY,	T.RECCREATEDON,	T.RECMODIFIEDBY, T.RECMODIFIEDON
		FROM 
			TITMMOV T INNER JOIN TITMMOV T2 
			ON T2.CODCOLIGADA=T.CODCOLIGADA
			AND T2.NSEQITMMOV=T.NSEQITMMOV
			WHERE T.IDMOV=@IDMOVORIGEM AND T2.IDMOV=@IDMOVDESTINO
	)

	SELECT * FROM CTE_ITEM

	SELECT * FROM CTE_TMOV
END
GO
