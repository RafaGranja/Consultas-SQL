;WITH CTE AS
(
    
    SELECT  CODCOLIGADA,CODFILIAL,OS,IDCRIACAO,IDCRIACAOPAI,EXECUCAO,DESCRICAO,CODIGOPRD,CODTRFPAI,NUMDESENHO,POSICAODESENHO,TOTALQTDE,
            CONVERT(VARCHAR(MAX),ROW_NUMBER() OVER(PARTITION BY EXECUCAO ORDER BY POSICAOINDICE)) INDICE
    FROM FLUIG.DBO.Z_CRM_EX001005 
    WHERE OS='3.07840.32.001' AND ITEMEXCLUSIVO!=2

    UNION ALL

    SELECT  T.CODCOLIGADA,T.CODFILIAL,T.OS,T.IDCRIACAO,T.IDCRIACAOPAI,T.EXECUCAO,T.DESCRICAO,T.CODIGOPRD,T.CODTRFPAI,T.NUMDESENHO,T.POSICAODESENHO,T.TOTALQTDE,
             CONVERT(VARCHAR(MAX),CONCAT(CTE.INDICE,'.',ROW_NUMBER() OVER(PARTITION BY T.EXECUCAO ORDER BY T.POSICAOINDICE))) INDICE
    FROM FLUIG.DBO.Z_CRM_EX001005 T
    JOIN CTE ON T.IDCRIACAOPAI = CTE.IDCRIACAO AND T.EXECUCAO=CTE.EXECUCAO AND T.OS=CTE.OS 
	WHERE T.OS='3.07840.32.001' AND T.ITEMEXCLUSIVO!=2 

)

SELECT CODCOLIGADA,CODFILIAL,REPROCESSAMENTO,DSCORDEM,CODESTRUTURA,CODORDEM,QTDEPLANEJADA,QTDEEFETIVADA,NUMEXEC,RESPONSAVEL,CODTRFPAI,
CONCAT(CODTRFPAI COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI,'.',FORMAT(NUMEXEC,'000'),'.',FORMAT(ROW_NUMBER() OVER(PARTITION BY CODTRFPAI,NUMEXEC ORDER BY INDICE),'000')) CODTRF FROM (
SELECT K.CODCOLIGADA,K.CODFILIAL,ISNULL(K.REPROCESSAMENTO,0) REPROCESSAMENTO,INDICE,K.DSCORDEM,KI.CODESTRUTURA,K.CODORDEM,KI.QTDEPLANEJADA,KI.QTDEEFETIVADA,KL.NUMEXEC,K.RESPONSAVEL,Z.CODTRFPAI
FROM CTE Z 
INNER JOIN KITEMORDEM KI ON
KI.CODCOLIGADA=Z.CODCOLIGADA
AND KI.CODFILIAL=Z.CODFILIAL
AND KI.CODESTRUTURA=Z.CODIGOPRD COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI
AND KI.CODCCUSTO=Z.OS COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI
INNER JOIN KORDEMCOMPL KL ON
KL.CODCOLIGADA=KI.CODCOLIGADA
AND KL.CODFILIAL=KI.CODFILIAL
AND KL.CODORDEM=KI.CODORDEM
AND KL.NUMEXEC=Z.EXECUCAO
INNER JOIN KORDEM K ON
K.CODCOLIGADA=KI.CODCOLIGADA
AND K.CODFILIAL=KI.CODFILIAL
AND K.CODORDEM=KI.CODORDEM
WHERE K.CODCCUSTO='3.07840.32.001' AND K.STATUS!=6 
) R
ORDER BY NUMEXEC,CAST('/' + REPLACE(INDICE, '.', '/') + '/' AS HIERARCHYID)
