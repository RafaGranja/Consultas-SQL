
DROP TABLE #TODO
DROP TABLE #DATA
SELECT F.LOGIN,P.NUM_PROCES,SUM(DATEDIFF(SECOND,P.START_DATE,P.END_DATE))/3600 TEMPO INTO #TODO FROM TAR_PROCES P 
INNER JOIN FDN_USERTENANT F ON
P.CD_MATRICULA=F.USER_CODE
INNER JOIN PROCES_WORKFLOW W ON
W.NUM_PROCES=P.NUM_PROCES
WHERE W.COD_DEF_PROCES IN ('CADASTRO DE ESTRUTURA','EDIÇÃO DE ESTRUTURA','EDIÇÃO DE SUBCONJUNTO DE ESTRUTURA',
'EDIÇÃO DE EXECUÇÃO','CADASTRO DE SUBCONJUNTO DE ESTRUTURA','EDIÇÃO DE EXECUÇÃO DE SUBCONJUNTO')  AND W.PERIOD_ID='DEFAULT' AND LOGIN !='TI.DELP'
GROUP BY F.LOGIN,P.NUM_PROCES

SELECT MIN(P.START_DATE) INICIO,MAX(P.END_DATE) FIM INTO #DATA FROM TAR_PROCES P 
INNER JOIN FDN_USERTENANT F ON
P.CD_MATRICULA=F.USER_CODE
INNER JOIN PROCES_WORKFLOW W ON
W.NUM_PROCES=P.NUM_PROCES
WHERE W.COD_DEF_PROCES IN ('CADASTRO DE ESTRUTURA','EDIÇÃO DE ESTRUTURA','EDIÇÃO DE SUBCONJUNTO DE ESTRUTURA',
'EDIÇÃO DE EXECUÇÃO','CADASTRO DE SUBCONJUNTO DE ESTRUTURA','EDIÇÃO DE EXECUÇÃO DE SUBCONJUNTO')  AND W.PERIOD_ID='DEFAULT' AND LOGIN !='TI.DELP'


SELECT SUM(TOTAL-MAXIMO) ECONOMIA FROM (
SELECT NUM_PROCES,MAX(TEMPO) MAXIMO,SUM(TEMPO) TOTAL FROM #TODO
GROUP BY NUM_PROCES ) R

SELECT CONVERT(VARCHAR(10),INICIO,103) INICIO,CONVERT(VARCHAR(10),FIM,103) FIM FROM #DATA


SELECT SUM(TOTAL-MAXIMO)/DATEDIFF(DAY,D.INICIO,D.FIM) ECONOMIADIA FROM (
SELECT NUM_PROCES,MAX(TEMPO) MAXIMO,SUM(TEMPO) TOTAL FROM #TODO
GROUP BY NUM_PROCES ) R
INNER JOIN #DATA D ON 1=1
GROUP BY D.INICIO,D.FIM