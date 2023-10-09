;WITH CTE3 AS (
		SELECT CODCOLIGADA,CODFILIAL,CODORDEM,MIN(DTHRINICIAL) TEMPO FROM (
			SELECT CODCOLIGADA,CODFILIAL,CODORDEM,DTHRINICIAL  
			FROM KMAOOBRAALOC  
			WHERE EFETIVADO=0
			UNION ALL
			SELECT CODCOLIGADA,CODFILIAL,CODORDEM,DTHRINICIAL
			FROM ZMDKATVORDEMPROGRAMACAO  
			WHERE STATUS!=2
		) R 
		GROUP BY CODCOLIGADA,CODFILIAL,CODORDEM 

),CTE2 AS (

	SELECT CODCOLIGADA,CODFILIAL,CODORDEM,MAX(TEMPO) TEMPO FROM (SELECT CODCOLIGADA,CODFILIAL,CODORDEM,T,SUM(TEMPO) TEMPO FROM (
		SELECT CODCOLIGADA,CODFILIAL,CODORDEM,TEMPO,0 T 
		FROM KATVORDEM 
		UNION ALL
		SELECT CODCOLIGADA,CODFILIAL,CODORDEM,CAST(DATEDIFF(SECOND,DTHRINICIAL,DTHRFINAL) AS BIGINT)/60 TEMPO ,1 T 
		FROM KMAOOBRAALOC  
		WHERE EFETIVADO=0
		UNION ALL
		SELECT CODCOLIGADA,CODFILIAL,CODORDEM,CAST(DATEDIFF(SECOND,DTHRINICIAL,DTHRFINAL) AS BIGINT)/60 TEMPO ,2 T 
		FROM ZMDKATVORDEMPROGRAMACAO  
		WHERE STATUS!=2
	) R 
	GROUP BY CODCOLIGADA,CODFILIAL,CODORDEM,T ) R2 GROUP BY CODCOLIGADA,CODFILIAL,CODORDEM

),
CTE AS (
    
    SELECT  Z.IDCRIACAO,Z.IDCRIACAOPAI,CONCAT(Z.DESCRICAO, ' - ' COLLATE Latin1_General_CI_AS ,Z.POSICAODESENHO) DESCRICAO,Z.EXECUCAO,Z.CODTRFPAI,Z.OS,1 FILHO,
	0 PAI,Z.INDICE,Z.NIVEL,KI.CODORDEM,C3.TEMPO INICIO,DATEADD(MINUTE,C2.TEMPO,C3.TEMPO) FIM
    FROM FLUIG.DBO.Z_CRM_EX001005 Z
    LEFT JOIN FLUIG.DBO.Z_CRM_EX001005 C ON  
	Z.EXECUCAO=C.EXECUCAO AND Z.OS=C.OS  
	AND Z.CODTRFPAI=C.CODTRFPAI 
	AND Z.IDCRIACAO = C.IDCRIACAOPAI
	INNER JOIN KITEMORDEM KI ON 
	KI.CODCOLIGADA=Z.CODCOLIGADA 
	AND KI.CODFILIAL=Z.CODFILIAL 
	AND KI.CODESTRUTURA=Z.CODIGOPRD COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI 
	INNER JOIN KORDEMCOMPL KL ON 
	KL.CODCOLIGADA=KI.CODCOLIGADA 
	AND KL.CODFILIAL=KI.CODFILIAL 
	AND KL.CODORDEM=KI.CODORDEM 
	AND KL.NUMEXEC=Z.EXECUCAO 
	INNER JOIN CTE2 C2 ON
	C2.CODCOLIGADA=KI.CODCOLIGADA
	AND C2.CODFILIAL=KI.CODFILIAL
	AND C2.CODORDEM=KI.CODORDEM
	INNER JOIN CTE3 C3 ON
	C3.CODCOLIGADA=KI.CODCOLIGADA
	AND C3.CODFILIAL=KI.CODFILIAL
	AND C3.CODORDEM=KI.CODORDEM
	WHERE Z.OS='3.07829.32.001' AND Z.ITEMEXCLUSIVO!=2 AND Z.CODTRFPAI='1.14' AND Z.EXECUCAO=20 AND C.IDCRIACAO IS NULL AND KI.STATUS!=6

	UNION ALL

    SELECT Z.IDCRIACAO,Z.IDCRIACAOPAI,CONCAT(Z.DESCRICAO, ' - ' COLLATE Latin1_General_CI_AS ,Z.POSICAODESENHO) DESCRICAO,Z.EXECUCAO,Z.CODTRFPAI,Z.OS,1 FILHO,
	CASE WHEN C.PAI=1 THEN 1 ELSE 0 END AS PAI,Z.INDICE,Z.NIVEL,KI.CODORDEM,C.FIM INICIO,DATEADD(MINUTE,C2.TEMPO,C.FIM) FIM
    FROM FLUIG.DBO.Z_CRM_EX001005 Z
	INNER JOIN CTE C ON
	Z.OS=C.OS
	AND Z.CODTRFPAI=C.CODTRFPAI
	AND Z.EXECUCAO=C.EXECUCAO
	AND Z.IDCRIACAO=C.IDCRIACAOPAI
	INNER JOIN KITEMORDEM KI ON 
	KI.CODCOLIGADA=Z.CODCOLIGADA 
	AND KI.CODFILIAL=Z.CODFILIAL 
	AND KI.CODESTRUTURA=Z.CODIGOPRD COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI 
	INNER JOIN KORDEMCOMPL KL ON 
	KL.CODCOLIGADA=KI.CODCOLIGADA 
	AND KL.CODFILIAL=KI.CODFILIAL 
	AND KL.CODORDEM=KI.CODORDEM 
	AND KL.NUMEXEC=Z.EXECUCAO 
	INNER JOIN CTE2 C2 ON
	C2.CODCOLIGADA=KI.CODCOLIGADA
	AND C2.CODFILIAL=KI.CODFILIAL
	AND C2.CODORDEM=KI.CODORDEM
	INNER JOIN CTE3 C3 ON
	C3.CODCOLIGADA=KI.CODCOLIGADA
	AND C3.CODFILIAL=KI.CODFILIAL
	AND C3.CODORDEM=KI.CODORDEM
	WHERE Z.OS='3.07829.32.001' AND Z.ITEMEXCLUSIVO!=2 AND Z.CODTRFPAI='1.14' AND Z.EXECUCAO=20 AND C.PAI=0 AND KI.STATUS!=6
)


SELECT IDCRIACAO,IDCRIACAOPAI,DESCRICAO,EXECUCAO,CODTRFPAI,INDICE,OS,CODORDEM,INICIO,FIM
 FROM CTE 
 ORDER BY CAST('/' + REPLACE(INDICE, '.', '/') + '/' AS HIERARCHYID)

