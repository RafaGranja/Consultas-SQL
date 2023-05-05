SELECT Z.CODCOLIGADA,Z.CODFILIAL,K.CODCCUSTO,Z.CODORDEM,Z.CODATIVIDADE,Z.NUMPLANOCORTE,Z.CODIGOMP,Z.NUMLOTE,Z.QTDEMP,Z.QTDESUCATA,(Z.QTDEMP-Z.QTDESUCATA) QTDEPECAS,A.DSCATIVIDADE,T.DESCRICAO,CONVERT(VARCHAR(10),Z.RECCREATEDON,103) DATACRICAO FROM 
ZMDPLANOAPROVEITAMENTOCORTE Z 
INNER JOIN KORDEM K ON
K.CODCOLIGADA=Z.CODCOLIGADA
AND K.CODFILIAL=Z.CODFILIAL
AND K.CODORDEM=Z.CODORDEM
INNER JOIN KATVORDEM KA ON
KA.CODCOLIGADA=Z.CODCOLIGADA
AND KA.CODFILIAL=Z.CODFILIAL
AND KA.CODORDEM=Z.CODORDEM
AND KA.CODATIVIDADE=Z.CODATIVIDADE
INNER JOIN KATIVIDADE A ON
A.CODCOLIGADA=KA.CODCOLIGADA
AND A.CODFILIAL=KA.CODFILIAL
AND A.CODATIVIDADE=KA.CODATIVIDADE
INNER JOIN TPRODUTO T ON
T.CODCOLPRD=Z.CODCOLIGADA
AND T.CODIGOPRD=Z.CODIGOMP
LEFT JOIN ZMDKATVORDEMPROGRAMACAO ZP ON
ZP.CODCOLIGADA=Z.CODCOLIGADA
AND ZP.CODFILIAL=Z.CODFILIAL
AND ZP.CODORDEM=Z.CODORDEM
AND ZP.NUMPLANOCORTE=Z.NUMPLANOCORTE
AND ZP.IDATVORDEM=KA.IDATVORDEM
LEFT JOIN KMAOOBRAALOC KM ON
KM.CODCOLIGADA=Z.CODCOLIGADA
AND KM.CODFILIAL=Z.CODFILIAL
AND KM.CODORDEM=K.CODORDEM
AND KM.IDATVORDEM=KA.IDATVORDEM
AND KM.EFETIVADO=0
WHERE ZP.IDATVORDEMPROGRAMACAO IS NULL AND KM.CODORDEM IS NULL AND Z.RECCREATEDBY='FLUIG' AND QTDMPFINAL IS NOT NULL
ORDER BY Z.NUMPLANOCORTE,Z.CODORDEM,Z.IDATVORDEM