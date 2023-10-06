SELECT DISTINCT K.CODCOLIGADA,K.CODFILIAL,K.CODCCUSTO,K.CODORDEM,KA.IDATVORDEM,KA.CODATIVIDADE,KAA.DSCATIVIDADE FROM KORDEM K
INNER JOIN KATVORDEM KA ON 
K.CODCOLIGADA=KA.CODCOLIGADA
AND K.CODFILIAL=KA.CODFILIAL
AND K.CODORDEM=KA.CODORDEM
INNER JOIN KATVORDEMMP KM ON
KM.CODCOLIGADA=KA.CODCOLIGADA
AND KM.CODFILIAL=KA.CODFILIAL
AND KM.CODORDEM=KA.CODORDEM
AND KM.IDATIVIDADE=KA.IDATVORDEM
INNER JOIN TPRD P ON
P.CODCOLIGADA=KA.CODCOLIGADA
AND P.IDPRD=KM.IDPRODUTO
INNER JOIN KATIVIDADE KAA ON
KAA.CODCOLIGADA=KA.CODCOLIGADA
AND KAA.CODATIVIDADE=KA.CODATIVIDADE
WHERE P.CODIGOPRD LIKE '01.053%' 
ORDER BY K.CODCOLIGADA,K.CODFILIAL,K.CODCCUSTO,K.CODORDEM,KA.IDATVORDEM