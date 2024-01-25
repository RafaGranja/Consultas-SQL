
	-- DECLARAÇÃO DE TODAS AS VARIÁVEIS
	DECLARE @IDCRIACAO VARCHAR(500), @INDICE VARCHAR(500), @QTDETOTAL VARCHAR(500)

	PRINT 'OS: '+@OS

	-- INÍCIO DO FOR PRINCIPAL PARA OS ITENS DA ESTRUTURA (Z_CRM_ML001005)
	DECLARE ESTRUTURA CURSOR FOR 
		
		-- SELEÇÃO DE TODOS OS ITENS DA OS 
		WITH CTE AS 
	         ( 
             SELECT  *,CONVERT(VARCHAR(MAX),'1') ORD
             FROM Z_CRM_EX001005  
             WHERE OS=@OS AND CODTRFPAI=@CODTRFPAI AND EXECUCAO=@EXECUCAO AND ITEMEXCLUSIVO!=2  AND IDCRIACAO=@ID
          
             UNION ALL 
          
             SELECT  T.*,CONVERT(VARCHAR(MAX),CONCAT(CTE.ORD,'.',ROW_NUMBER() OVER(ORDER BY T.POSICAOINDICE))) ORD
             FROM CTE 
             INNER JOIN Z_CRM_EX001005 T ON T.EXECUCAO=CTE.EXECUCAO AND T.OS=CTE.OS  AND T.CODTRFPAI=CTE.CODTRFPAI AND T.IDCRIACAOPAI = CTE.IDCRIACAO AND T.CODCOLIGADA=CTE.CODCOLIGADA AND T.CODFILIAL = CTE.CODFILIAL
             WHERE T.ITEMEXCLUSIVO!=2 AND T.OS=@OS AND T.CODTRFPAI=@CODTRFPAI AND T.EXECUCAO=@EXECUCAO
          )
		SELECT IDCRIACAO,INDICE FROM CTE
		ORDER BY CAST('/' + REPLACE(ORD, '.', '/') + '/' AS HIERARCHYID)

	OPEN ESTRUTURA
	
	PRINT 'Início do FOR'

	FETCH NEXT FROM ESTRUTURA INTO @IDCRIACAO, @INDICE
					
	WHILE @@FETCH_STATUS = 0
	
	BEGIN


		UPDATE 
			P 
		SET 
			P.TOTALQTDE = CASE WHEN (P.IDCRIACAOPAI!=0) 
							   THEN CAST(REPLACE((SELECT M.TOTALQTDE FROM Z_CRM_EX001005 M WHERE M.OS=P.OS AND M.IDCRIACAO=P.IDCRIACAOPAI AND M.EXECUCAO=P.EXECUCAO AND M.CODTRFPAI=P.CODTRFPAI AND M.ITEMEXCLUSIVO!=2),',','.') AS FLOAT) * CAST(REPLACE(P.DESQTDE,',','.') AS FLOAT) 
							   ELSE CAST(REPLACE(P.DESQTDE,',','.') AS FLOAT) 
						  END, 
			P.PESOBRUTO = REPLACE( CAST( (CASE WHEN (P.IDCRIACAOPAI!=0) 
										  THEN CAST(REPLACE((SELECT M.TOTALQTDE FROM Z_CRM_EX001005 M WHERE M.OS=P.OS AND M.IDCRIACAO=P.IDCRIACAOPAI AND M.EXECUCAO=P.EXECUCAO AND M.CODTRFPAI=P.CODTRFPAI AND M.ITEMEXCLUSIVO!=2),',','.') AS FLOAT) * CAST(REPLACE(P.DESQTDE,',','.') AS FLOAT) 
										  ELSE CAST(REPLACE(P.DESQTDE,',','.') AS FLOAT) 
										  END) * ISNULL(CAST(REPLACE(P.PESOUNITARIO,',','.') AS FLOAT),0) AS DECIMAL(18,4)),'.',','), 
			P.PESOLIQUIDO = REPLACE( CAST( (CASE WHEN (P.IDCRIACAOPAI!=0) 
											THEN CAST(REPLACE((SELECT M.TOTALQTDE FROM Z_CRM_EX001005 M WHERE M.OS=P.OS AND M.IDCRIACAO=P.IDCRIACAOPAI AND M.EXECUCAO=P.EXECUCAO AND M.CODTRFPAI=P.CODTRFPAI AND M.ITEMEXCLUSIVO!=2),',','.') AS FLOAT) * CAST(REPLACE(P.DESQTDE,',','.') AS FLOAT) 
											ELSE CAST(REPLACE(P.DESQTDE,',','.') AS FLOAT) 
											END) * ISNULL(CAST(REPLACE(P.PESOUNLIQUIDO,',','.') AS FLOAT),0) AS DECIMAL(18,4)),'.',',') 		
		FROM	
			Z_CRM_EX001005 P 
		WHERE 
			P.OS=@OS AND P.EXECUCAO=@EXECUCAO AND P.CODTRFPAI=@CODTRFPAI AND P.IDCRIACAO=@IDCRIACAO

	FETCH NEXT FROM ESTRUTURA INTO @IDCRIACAO, @INDICE
	
	END

	CLOSE ESTRUTURA
	DEALLOCATE ESTRUTURA

	UPDATE 
		C
	SET 
		C.QTDEUNCOMPONENTES = CASE WHEN P.QTDEUNCOMP='DESENHO' AND C.LISTACOMPONENTES='L' THEN P.DESQTDE ELSE C.QTDEUNCOMPONENTES END, 
		C.QTDETOTALCOMPONENTES = (CAST((CASE WHEN P.QTDEUNCOMP='DESENHO' AND C.LISTACOMPONENTES='L'THEN P.DESQTDE ELSE REPLACE(C.QTDEUNCOMPONENTES,',','.') END) AS DECIMAL(18,4)))*(CAST(P.TOTALQTDE AS DECIMAL(18,4))/CAST(P.DESQTDE AS DECIMAL(18,4)))
	FROM 
		Z_CRM_EX001019 C 
		INNER JOIN Z_CRM_EX001005 P ON P.OS=C.OSCOMPONENTES AND P.IDCRIACAO=C.IDCRIACAOCOMPONENTES AND P.EXECUCAO=C.EXECUCAO
	WHERE  		
		C.OSCOMPONENTES=@OS AND P.EXECUCAO=@EXECUCAO AND P.CODTRFPAI=@CODTRFPAI AND C.SUBSTITUTOCOMPONENTES = '' AND P.IDCRIACAO=@IDCRIACAO
