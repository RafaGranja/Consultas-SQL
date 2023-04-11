declare @param varchar(100)
set @param = '%'
set @param = concat(@param,',')

declare @param_order varchar(100)
set @param_order = 'BITOLA-ASC,POSICAO-DESC'
set @param_order = concat(@param_order,',')
SET @param_order = REPLACE(@param_order,' ','')


;with CTE_ord as(

			select left(@param_order,CHARINDEX(',',@param_order)-1) param,SUBSTRING(@param_order,CHARINDEX(',',@param_order)+1,LEN(@param_order)) param1 ,1 cont
			union all 
			select left(param1,CHARINDEX(',',param1)-1) param,SUBSTRING(param1,CHARINDEX(',',param1)+1,LEN(param1)) param1, cont+1
			from CTE_ord 
			where len(param1)>1

)
,cte as (	

			select left(@param,CHARINDEX(',',@param)-1) param,SUBSTRING(@param,CHARINDEX(',',@param)+1,LEN(@param)) param1 
			union all 
			select left(param1,CHARINDEX(',',param1)-1) param,SUBSTRING(param1,CHARINDEX(',',param1)+1,LEN(param1)) param1
			from cte  
			where len(param1)>1

		)
,CTE_Rec as (
    SELECT 
	Z.NUMPLANOCORTE, EX.OS, EX.DESCOS, MC.TAG, Z.NUMLOTE, EX.NUMDESENHO, EX.POSICAODESENHO, CONCAT(1,'/',Z.QUANTIDADE) QUANTIDADE,Z.QUANTIDADE QTD, Z.CODORDEM, EX.BITOLA, EX.LARGURA, EX.COMPRIMENTO,
    (SELECT CONCAT
    ('# 'COLLATE Latin1_General_CI_AS,
    EX.BITOLA COLLATE Latin1_General_CI_AS,
    ' X 'COLLATE Latin1_General_CI_AS,
    EX.LARGURA COLLATE Latin1_General_CI_AS,
    ' X ' COLLATE Latin1_General_CI_AS,
    EX.COMPRIMENTO COLLATE Latin1_General_CI_AS)) DIMENSOES,1 AS NUMERO
    FROM ZMDPLANOAPROVEITAMENTOCORTE Z
            INNER JOIN FLUIG.DBO.Z_CRM_EX001005 EX ON EX.CODCOLIGADA = Z.CODCOLIGADA AND EX.CODFILIAL = Z.CODFILIAL AND EX.CODIGOPRD = Z.CODESTRUTURA COLLATE Latin1_General_CI_AS
            INNER JOIN MTAREFA MT ON MT.CODCOLIGADA = Z.CODCOLIGADA AND MT.CODTRFAUX  = Z.CODORDEM
            INNER JOIN MPRJ MP ON MP.CODCOLIGADA = MT.CODCOLIGADA AND MP.IDPRJ = MT.IDPRJ
            INNER JOIN MTRFCOMPL MC ON MC.CODCOLIGADA = MT.CODCOLIGADA AND MC.IDPRJ = MT.IDPRJ AND MT.IDTRF = MC.IDTRF





   WHERE NUMPLANOCORTE = 'CNC001258' AND (Z.CODORDEM LIKE '%' OR Z.CODORDEM IN (SELECT DISTINCT param FROM cte))
    AND MP.REVISAO = (SELECT MAX(REVISAO) FROM MPRJ WHERE CODPRJ = EX.OS COLLATE Latin1_General_CI_AS)



   UNION ALL



   SELECT NUMPLANOCORTE, OS, DESCOS, TAG, NUMLOTE, NUMDESENHO, POSICAODESENHO, CONCAT( NUMERO+1,'/', QTD), QTD, CODORDEM, BITOLA, LARGURA, COMPRIMENTO,DIMENSOES,NUMERO+1
    FROM CTE_Rec
    WHERE NUMERO < QTD
), CTE_ORDER AS(
	
	SELECT LEFT(PARAM,CASE WHEN CHARINDEX('-',PARAM) = 0 THEN DATALENGTH(PARAM) + 1 ELSE CHARINDEX('-',PARAM) END -1) COLLUM,CONT,RIGHT(PARAM,DATALENGTH(PARAM) - CHARINDEX('-',PARAM)) ORD,PARAM FROM CTE_ord
	
)

 
 SELECT CASE WHEN ISNULL((SELECT ORD FROM CTE_ORDER WHERE CONT=1),'ASC') = 'DESC' THEN RANK()OVER(ORDER BY ORDER1 DESC)ELSE  RANK()OVER(ORDER BY ORDER1 ASC) END AS ORD1,
 CASE WHEN ISNULL((SELECT ORD FROM CTE_order WHERE CONT=2),'ASC') = 'DESC' THEN RANK()OVER(ORDER BY ORDER1,ORDER2 DESC) ELSE  RANK()OVER(ORDER BY ORDER1,ORDER2 ASC) END AS ORD2,
 CASE WHEN ISNULL((SELECT ORD FROM CTE_order WHERE CONT=3),'ASC') = 'DESC' THEN RANK()OVER(ORDER BY ORDER1,ORDER2,ORDER3 DESC) ELSE  RANK()OVER(ORDER BY ORDER1,ORDER2,ORDER3 ASC) END AS ORD3,
 * FROM (
	SELECT DISTINCT 
	 CASE CONVERT(VARCHAR(100),(SELECT COLLUM FROM CTE_ORDER WHERE CONT=1)) 
						 WHEN 'OS' THEN CONVERT(VARCHAR(100),[OS]) COLLATE SQL_Latin1_General_CP1_CI_AI
						 WHEN 'TAG' THEN CONVERT(VARCHAR(100),TAG) COLLATE SQL_Latin1_General_CP1_CI_AI
						 WHEN 'DESENHO' THEN CONVERT(VARCHAR(100),NUMDESENHO) COLLATE SQL_Latin1_General_CP1_CI_AI
						 WHEN 'POSICAO' THEN CONVERT(VARCHAR(100),POSICAODESENHO) COLLATE SQL_Latin1_General_CP1_CI_AI
						 WHEN 'QUANTIDADE' THEN CONVERT(VARCHAR(100),QTD) COLLATE SQL_Latin1_General_CP1_CI_AI
						 WHEN 'OP' THEN CONVERT(VARCHAR(100),CODORDEM) COLLATE SQL_Latin1_General_CP1_CI_AI
						 WHEN 'BITOLA' THEN CONVERT(VARCHAR(100),BITOLA) COLLATE SQL_Latin1_General_CP1_CI_AI
						 WHEN 'LARGURA' THEN CONVERT(VARCHAR(100),LARGURA) COLLATE SQL_Latin1_General_CP1_CI_AI
						 WHEN 'COMPRIMENTO' THEN  CONVERT(VARCHAR(100),COMPRIMENTO)COLLATE SQL_Latin1_General_CP1_CI_AI
						 ELSE CONVERT(VARCHAR(100),CODORDEM) COLLATE SQL_Latin1_General_CP1_CI_AI END AS ORDER1,
					 
						 CASE CONVERT(VARCHAR(100),(SELECT COLLUM FROM CTE_ORDER WHERE CONT=2)) 
						 WHEN 'OS' THEN CONVERT(VARCHAR(100),OS) COLLATE SQL_Latin1_General_CP1_CI_AI
						 WHEN 'TAG' THEN CONVERT(VARCHAR(100),TAG) COLLATE SQL_Latin1_General_CP1_CI_AI
						 WHEN 'DESENHO' THEN CONVERT(VARCHAR(100),NUMDESENHO) COLLATE SQL_Latin1_General_CP1_CI_AI
						 WHEN 'POSICAO' THEN CONVERT(VARCHAR(100),POSICAODESENHO) COLLATE SQL_Latin1_General_CP1_CI_AI
						 WHEN 'QUANTIDADE' THEN CONVERT(VARCHAR(100),QTD) COLLATE SQL_Latin1_General_CP1_CI_AI
						 WHEN 'OP' THEN CONVERT(VARCHAR(100),CODORDEM) COLLATE SQL_Latin1_General_CP1_CI_AI
						 WHEN 'BITOLA' THEN CONVERT(VARCHAR(100),BITOLA) COLLATE SQL_Latin1_General_CP1_CI_AI
						 WHEN 'LARGURA' THEN CONVERT(VARCHAR(100),LARGURA) COLLATE SQL_Latin1_General_CP1_CI_AI
						 WHEN 'COMPRIMENTO' THEN  CONVERT(VARCHAR(100),COMPRIMENTO)COLLATE SQL_Latin1_General_CP1_CI_AI
						 ELSE CASE WHEN (SELECT COLLUM FROM CTE_ORDER WHERE CONT=1) IS NULL THEN  CONVERT(VARCHAR(100),QUANTIDADE) ELSE CONVERT(VARCHAR(100),CODORDEM) END END AS ORDER2,
					 
						 CASE CONVERT(VARCHAR(100),(SELECT COLLUM FROM CTE_ORDER WHERE CONT=3))
						 WHEN 'OS' THEN CONVERT(VARCHAR(100),OS) COLLATE SQL_Latin1_General_CP1_CI_AI
						 WHEN 'TAG' THEN CONVERT(VARCHAR(100),TAG) COLLATE SQL_Latin1_General_CP1_CI_AI
						 WHEN 'DESENHO' THEN CONVERT(VARCHAR(100),NUMDESENHO) COLLATE SQL_Latin1_General_CP1_CI_AI
						 WHEN 'POSICAO' THEN CONVERT(VARCHAR(100),POSICAODESENHO) COLLATE SQL_Latin1_General_CP1_CI_AI
						 WHEN 'QUANTIDADE' THEN CONVERT(VARCHAR(100),QTD) COLLATE SQL_Latin1_General_CP1_CI_AI
						 WHEN 'OP' THEN CONVERT(VARCHAR(100),CODORDEM) COLLATE SQL_Latin1_General_CP1_CI_AI
						 WHEN 'BITOLA' THEN CONVERT(VARCHAR(100),BITOLA) COLLATE SQL_Latin1_General_CP1_CI_AI
						 WHEN 'LARGURA' THEN CONVERT(VARCHAR(100),LARGURA) COLLATE SQL_Latin1_General_CP1_CI_AI
						 WHEN 'COMPRIMENTO' THEN  CONVERT(VARCHAR(100),COMPRIMENTO)COLLATE SQL_Latin1_General_CP1_CI_AI
						 ELSE CASE WHEN (SELECT COLLUM FROM CTE_ORDER WHERE CONT=1) IS NULL OR (SELECT COLLUM FROM CTE_ORDER WHERE CONT=2) IS NULL THEN  CONVERT(VARCHAR(100),QUANTIDADE) ELSE CONVERT(VARCHAR(100),CODORDEM) END END AS ORDER3,* FROM CTE_Rec 
						 ) R  ORDER BY ORDER1,ORD1,ORDER2,ORD2,ORDER3,ORD3,NUMERO ASC