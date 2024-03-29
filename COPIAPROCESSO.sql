DELETE FROM Z_CRM_EX001021 WHERE OSPROCESSO='3.07867.32.001' AND IDCRIACAOPROCESSO=(SELECT DISTINCT IDCRIACAO FROM Z_CRM_EX001005 WHERE OS='3.07867.32.001' AND CODTRFPAI='1.20' AND EXECUCAO=1) AND EXECUCAO=1 
INSERT INTO Z_CRM_EX001021 (ID,PRIORIDADE,IDCRIACAOPROCESSO,CODATIVIDADE,OSPROCESSO,IDPROCESSO,DESCATIVIDADE,
HABILIDADEREQUERIDA,CODHABILIDADE,CODPOSTO,DESCPOSTO,FILA,CONFIGURACAO,PROCESSAMENTO,DESAGREGACAO,
ESPERA,MOVIMENTACAO,MINUTOSGASTOS,DESCPROCESSO,DOCAPOIOATV1,DOCAPOIOATV2,DOCAPOIOATV3,DOCAPOIOATV4,
SOLICITACAOPROCESSO,FORNPARA,INTEGRADOPROCESSO,EXECUCAO) 
SELECT (SELECT VALAUTOINC FROM CORPORE.dbo.GAUTOINC WHERE CODSISTEMA = 'K' AND CODAUTOINC = 'Z_CRM_ML001021')+ROW_NUMBER()OVER(ORDER BY ID) ID,PRIORIDADE,IDCRIACAOPROCESSO,CODATIVIDADE,OSPROCESSO,IDPROCESSO,DESCATIVIDADE,
HABILIDADEREQUERIDA,CODHABILIDADE,CODPOSTO,DESCPOSTO,FILA,CONFIGURACAO,PROCESSAMENTO,DESAGREGACAO,
ESPERA,MOVIMENTACAO,MINUTOSGASTOS,DESCPROCESSO,DOCAPOIOATV1,DOCAPOIOATV2,DOCAPOIOATV3,DOCAPOIOATV4,
SOLICITACAOPROCESSO,FORNPARA,INTEGRADOPROCESSO,'1' EXECUCAO
 FROM Z_CRM_EX001021 WHERE OSPROCESSO='3.07867.32.001' AND IDCRIACAOPROCESSO=(SELECT DISTINCT IDCRIACAO FROM Z_CRM_EX001005 WHERE OS='3.07867.32.001' AND CODTRFPAI='1.20' AND EXECUCAO=1) AND EXECUCAO=2

  UPDATE CORPORE.dbo.GAUTOINC SET VALAUTOINC = (SELECT MAX(ID) FROM (SELECT ID FROM Z_CRM_ML001021 UNION ALL SELECT ID FROM Z_CRM_EX001021) AS MLS) WHERE CODCOLIGADA = 0 AND CODAUTOINC = 'Z_CRM_ML001021'

