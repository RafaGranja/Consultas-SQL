USE [CORPORE]
GO

/****** Object:  UserDefinedFunction [dbo].[FN_DELP_RETORNA_ITEM_MOVIMENTO]    Script Date: 14/08/2024 09:29:05 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:		RAFAEL.GRANJA
-- Create date: 13/08/2024
-- Description:	FUNCAO PARA RETORNAR ITEM DE MOVIMENTO COM INFORMAÇÕES DO MOVIMENTO
-- =============================================
CREATE FUNCTION [dbo].[FN_DELP_RETORNA_ITEM_MOVIMENTO]
(	
	@CODCOLIGADA INT,
	@IDMOV INT,
	@NSEQ INT=NULL
)
RETURNS TABLE 
AS
RETURN 
(
	SELECT T.SERIE,T.STATUS STATUSMOV,T.CODTMV,T.NUMEROMOV,TI.*
	FROM TMOV T 
	INNER JOIN TITMMOV TI ON 
	TI.CODCOLIGADA = T.CODCOLIGADA
	AND TI.IDMOV = T.IDMOV
	WHERE T.CODCOLIGADA=@CODCOLIGADA
	AND T.IDMOV=@IDMOV
	AND TI.NSEQITMMOV=ISNULL(@NSEQ,TI.NSEQITMMOV)

)
GO

