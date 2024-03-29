;WITH CTE AS ( 
              
             SELECT PRJ.CODCOLIGADA,PRJ.CODFILIAL,CODTRFAUX,ISNULL(TAG,'SEM TAG') TAG,ISNULL(CELULA,'SEM CELULA') CELULA,ISNULL(C.DESCRICAO,'SEM CELULA') DESCRICAO
             FROM MPRJ(NOLOCK)  PRJ 
             INNER JOIN MTRF(NOLOCK) TRF ON PRJ.CODCOLIGADA = TRF.CODCOLIGADA AND PRJ.IDPRJ = TRF.IDPRJ 
             INNER JOIN MTRFCOMPL(NOLOCK)  TRFC ON TRF.CODCOLIGADA = TRFC.CODCOLIGADA AND TRF.IDPRJ = TRFC.IDPRJ  AND TRF.IDTRF = TRFC.IDTRF 
             INNER JOIN GCONSIST (NOLOCK)  C ON  C.CODCLIENTE = TRFC.CELULA 
             WHERE PRJ.POSICAO IN(1,4) AND C.CODTABELA='CELULA' AND C.APLICACAO = 'M' AND C.CODCOLIGADA=0 
          
        )
SELECT Z.CODCOLIGADA,Z.CODFILIAL,CODESTRUTURA,SUM(CAST(DATEDIFF(SECOND,Z.DTHRINICIAL,Z.DTHRFINAL) AS BIGINT))/3600.0 ALOCADO,C.CELULA
FROM ZMDKATVORDEMPROGRAMACAO Z 
INNER JOIN KORDEM K ON
Z.CODCOLIGADA=K.CODCOLIGADA
AND Z.CODFILIAL=K.CODFILIAL
AND Z.CODORDEM=K.CODORDEM
INNER JOIN CTE C ON C.CODCOLIGADA=Z.CODCOLIGADA AND C.CODFILIAL=Z.CODFILIAL AND C.CODTRFAUX=Z.CODORDEM
WHERE  Z.STATUS = 0
               AND K.STATUS NOT IN ( 5, 6 )
               AND Z.CODCOLIGADA = 1
               AND Z.CODFILIAL = 7
               AND Z.CODMO = '72032'
               AND CAST(Z.DTHRINICIAL AS DATE) = '2023-05-23' 
 GROUP BY Z.CODCOLIGADA,Z.CODFILIAL,CODESTRUTURA,C.CELULA