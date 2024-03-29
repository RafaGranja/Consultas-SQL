;with cte as (

	SELECT Z.INDICE,K.CODCOLIGADA,K.CODFILIAL,Z.CODTRFPAI,K.CODORDEM,KI.QTDEPLANEJADA,K.DSCORDEM,KL.NUMEXEC,Z.OPSUNITARIAS,Z.DESQTDE,Z.TOTALQTDE,ROW_NUMBER()OVER (PARTITION BY DSCORDEM,NUMEXEC ORDER BY K.CODORDEM) SEQ
	FROM KORDEM K
	INNER JOIN KORDEMCOMPL KL ON
	KL.CODCOLIGADA=K.CODCOLIGADA
	AND KL.CODFILIAL=K.CODFILIAL
	AND KL.CODORDEM=K.CODORDEM
	INNER JOIN KITEMORDEM KI ON
	KI.CODCOLIGADA=K.CODCOLIGADA
	AND KI.CODFILIAL=K.CODFILIAL
	AND KI.CODORDEM=K.CODORDEM
	INNER JOIN FLUIG.DBO.Z_CRM_EX001005 Z ON
	Z.CODCOLIGADA=K.CODCOLIGADA
	AND Z.CODFILIAL=K.CODFILIAL
	AND Z.CODIGOPRD=KI.CODESTRUTURA COLLATE Latin1_General_CI_AS
	AND Z.OS=K.CODCCUSTO COLLATE Latin1_General_CI_AS
	AND Z.EXECUCAO=KL.NUMEXEC
	AND Z.ITEMEXCLUSIVO!=2
	WHERE K.CODCCUSTO='3.07853.32.003' AND K.STATUS=0 AND Z.INDICE LIKE '2%'
), CTE2 AS ( 

SELECT A.*,'INCORRETO' AVALIACAO FROM cte A WHERE TOTALQTDE != (SELECT max(SEQ) FROM cte WHERE CODCOLIGADA=A.CODCOLIGADA AND CODFILIAL=A.CODFILIAL AND DSCORDEM=A.DSCORDEM AND NUMEXEC=A.NUMEXEC) AND A.OPSUNITARIAS='SIM'
UNION ALL
SELECT B.*,'CORRETO' AVALIACAO FROM cte B WHERE B.OPSUNITARIAS=''
 )


 SELECT * FROM CTE2 WHERE AVALIACAO='INCORRETO' AND SEQ!=1
 ORDER BY CAST('/'+REPLACE(INDICE,'.','/')+'/' AS hierarchyid)