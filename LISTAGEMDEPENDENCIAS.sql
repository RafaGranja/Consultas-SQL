SELECT DISTINCT KA.CODORDEM,KA.CODATIVIDADE,KI.CODCCUSTO,T.CODIGOPRD FROM KATVORDEM KA
INNER JOIN KATVORDEMMP K ON
K.CODCOLIGADA=KA.CODCOLIGADA
AND K.CODFILIAL=KA.CODFILIAL
AND K.CODORDEM=KA.CODORDEM
AND K.CODESTRUTURA=KA.CODESTRUTURA
AND K.CODMODELO=KA.CODMODELO
AND K.IDATIVIDADE=KA.IDATVORDEM
INNER JOIN KITEMORDEM KI ON
KI.CODCOLIGADA=KA.CODCOLIGADA
AND KI.CODFILIAL=KA.CODFILIAL
AND KI.CODORDEM=KA.CODORDEM
AND KI.CODESTRUTURA=KA.CODESTRUTURA
AND KI.CODMODELO=KA.CODMODELO
INNER JOIN FLUIG.DBO.Z_CRM_EX001005COMPL E ON
E.CODCOLIGADA=KA.CODCOLIGADA
AND E.CODFILIAL=KA.CODFILIAL
AND E.OS=KI.CODCCUSTO COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI
AND E.CODORDEM=KA.CODORDEM
INNER JOIN TPRD T ON
T.CODCOLIGADA=K.CODCOLIGADA
AND T.IDPRD=K.IDPRODUTO
LEFT JOIN TMOV TT ON
TT.CODCOLIGADA=K.CODCOLIGADA
AND CAST(TT.CAMPOLIVRE1 AS VARCHAR(30))=K.CODORDEM
AND	TT.CAMPOLIVRE2=K.IDATIVIDADE
AND TT.CODTMV='1.1.05'
LEFT JOIN TITMMOV TI ON
TI.CODCOLIGADA=TT.CODCOLIGADA
AND TI.CODFILIAL=KA.CODFILIAL
AND TI.IDPRD=KA.IDATVDEPEND
WHERE LEFT(T.CODIGOPRD,6) IN ('01.018','01.052','01.051') AND KA.STATUS NOT IN (5,6) AND KI.STATUS NOT IN (5,6) AND TI.CODCOLIGADA IS NULL AND K.EFETIVADO=0

--OP NÃO TEM CNC CADASTRADO.
SELECT KA.CODORDEM,KA.CODATIVIDADE,KI.CODCCUSTO FROM KATVORDEM KA
INNER JOIN KATVORDEMMP K ON
K.CODCOLIGADA=KA.CODCOLIGADA
AND K.CODFILIAL=KA.CODFILIAL
AND K.CODORDEM=KA.CODORDEM
AND K.CODESTRUTURA=KA.CODESTRUTURA
AND K.CODMODELO=KA.CODMODELO
AND K.IDATIVIDADE=KA.IDATVORDEM
INNER JOIN KITEMORDEM KI ON
KI.CODCOLIGADA=KA.CODCOLIGADA
AND KI.CODFILIAL=KA.CODFILIAL
AND KI.CODORDEM=KA.CODORDEM
AND KI.CODESTRUTURA=KA.CODESTRUTURA
AND KI.CODMODELO=KA.CODMODELO
INNER JOIN FLUIG.DBO.Z_CRM_EX001005COMPL E ON
E.CODCOLIGADA=KA.CODCOLIGADA
AND E.CODFILIAL=KA.CODFILIAL
AND E.OS=KI.CODCCUSTO COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI
AND E.CODORDEM=KA.CODORDEM
INNER JOIN TPRD T ON
T.CODCOLIGADA=K.CODCOLIGADA
AND T.IDPRD=K.IDPRODUTO
LEFT JOIN ZMDPLANOAPROVEITAMENTOCORTE Z 
ON Z.CODCOLIGADA=KA.CODCOLIGADA
AND Z.CODFILIAL=KA.CODFILIAL
AND Z.CODORDEM=KA.CODORDEM
AND Z.CODESTRUTURA=KA.CODESTRUTURA
AND Z.CODATIVIDADE=KA.CODATIVIDADE
WHERE (T.CODTB2FAT IN ('0181','0182','0183') OR T.CODIGOPRD LIKE '01.055%') AND Z.CODORDEM IS NULL AND KA.STATUS NOT IN (5,6) AND KI.STATUS NOT IN (5,6) AND KA.QUANTIDADE=0  AND K.EFETIVADO=0

--OP POSSUI CNC/PA CADASTRADO, MAS NÃO TEM HORAS APONTADAS.
SELECT KA.CODORDEM,KA.CODATIVIDADE,KI.CODCCUSTO,Z.NUMPLANOCORTE FROM KATVORDEM KA 
INNER JOIN KITEMORDEM KI ON
KI.CODCOLIGADA=KA.CODCOLIGADA
AND KI.CODFILIAL=KA.CODFILIAL
AND KI.CODORDEM=KA.CODORDEM
AND KI.CODESTRUTURA=KA.CODESTRUTURA
AND KI.CODMODELO=KA.CODMODELO
INNER JOIN ZMDPLANOAPROVEITAMENTOCORTE Z 
ON Z.CODCOLIGADA=KA.CODCOLIGADA
AND Z.CODFILIAL=KA.CODFILIAL
AND Z.CODORDEM=KA.CODORDEM
AND Z.CODESTRUTURA=KA.CODESTRUTURA
AND Z.CODATIVIDADE=KA.CODATIVIDADE
LEFT JOIN KMAOOBRAALOC H ON
H.CODCOLIGADA=KA.CODCOLIGADA
AND H.CODFILIAL=KA.CODFILIAL
AND H.CODORDEM=KA.CODORDEM
AND H.CODESTRUTURA=KA.CODESTRUTURA
AND H.CODMODELO=KA.CODMODELO
AND	H.IDATVORDEM=KA.IDATVORDEM
LEFT JOIN KPARADAATV HP ON
HP.CODCOLIGADA=KA.CODCOLIGADA
AND HP.CODFILIAL=KA.CODFILIAL
AND HP.CODORDEM=KA.CODORDEM
AND HP.CODESTRUTURA=KA.CODESTRUTURA
AND HP.CODMODELO=KA.CODMODELO
AND	HP.IDATVORDEM=KA.IDATVORDEM
WHERE  KA.STATUS NOT IN (5,6) AND KI.STATUS NOT IN (5,6) AND H.CODCOLIGADA IS NULL AND HP.CODCOLIGADA IS NULL

--OP POSSUI CNC/PA, MAS NÃO GEROU RA
SELECT KA.CODORDEM,KA.CODATIVIDADE,KI.CODCCUSTO,Z.NUMPLANOCORTE FROM KATVORDEM KA 
INNER JOIN KITEMORDEM KI ON
KI.CODCOLIGADA=KA.CODCOLIGADA
AND KI.CODFILIAL=KA.CODFILIAL
AND KI.CODORDEM=KA.CODORDEM
AND KI.CODESTRUTURA=KA.CODESTRUTURA
AND KI.CODMODELO=KA.CODMODELO
INNER JOIN ZMDPLANOAPROVEITAMENTOCORTE Z 
ON Z.CODCOLIGADA=KA.CODCOLIGADA
AND Z.CODFILIAL=KA.CODFILIAL
AND Z.CODORDEM=KA.CODORDEM
AND Z.CODESTRUTURA=KA.CODESTRUTURA
AND Z.CODATIVIDADE=KA.CODATIVIDADE
WHERE  KA.STATUS NOT IN (5,6) AND KI.STATUS NOT IN (5,6) AND Z.IDMOVREQ IS NULL

--OPs e PA/PC QUE FORAM GERADAS RA'S QUE NÃO BAIXOU
SELECT KA.CODORDEM,KA.CODATIVIDADE,KI.CODCCUSTO,Z.NUMPLANOCORTE FROM KATVORDEM KA 
INNER JOIN KITEMORDEM KI ON
KI.CODCOLIGADA=KA.CODCOLIGADA
AND KI.CODFILIAL=KA.CODFILIAL
AND KI.CODORDEM=KA.CODORDEM
AND KI.CODESTRUTURA=KA.CODESTRUTURA
AND KI.CODMODELO=KA.CODMODELO
INNER JOIN ZMDPLANOAPROVEITAMENTOCORTE Z 
ON Z.CODCOLIGADA=KA.CODCOLIGADA
AND Z.CODFILIAL=KA.CODFILIAL
AND Z.CODORDEM=KA.CODORDEM
AND Z.CODESTRUTURA=KA.CODESTRUTURA
AND Z.CODATIVIDADE=KA.CODATIVIDADE
INNER JOIN TMOV T ON
T.CODCOLIGADA=KA.CODCOLIGADA
AND T.CODFILIAL=KA.CODFILIAL
AND T.CAMPOLIVRE3=Z.NUMPLANOCORTE
AND T.IDMOV=Z.IDMOVREQ
WHERE  KA.STATUS NOT IN (5,6) AND KI.STATUS NOT IN (5,6) AND T.STATUS='A'

-- OP POSSUI CNC/PA, MAS A GUIA COM CNC/PA CADASTRADO NÃO FOI AVANÇADA 100%, 
SELECT KA.CODORDEM,KA.CODATIVIDADE,KI.CODCCUSTO,Z.NUMPLANOCORTE,PERCENTUAL FROM KATVORDEM KA 
INNER JOIN KITEMORDEM KI ON
KI.CODCOLIGADA=KA.CODCOLIGADA
AND KI.CODFILIAL=KA.CODFILIAL
AND KI.CODORDEM=KA.CODORDEM
AND KI.CODESTRUTURA=KA.CODESTRUTURA
AND KI.CODMODELO=KA.CODMODELO
INNER JOIN ZMDPLANOAPROVEITAMENTOCORTE Z 
ON Z.CODCOLIGADA=KA.CODCOLIGADA
AND Z.CODFILIAL=KA.CODFILIAL
AND Z.CODORDEM=KA.CODORDEM
AND Z.CODESTRUTURA=KA.CODESTRUTURA
AND Z.CODATIVIDADE=KA.CODATIVIDADE
WHERE KA.STATUS NOT IN (5,6) AND KI.STATUS NOT IN (5,6)