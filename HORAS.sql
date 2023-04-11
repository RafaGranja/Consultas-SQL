
SELECT ROUND(CAST(SUM(DATEDIFF(SECOND,KM.DTHRINICIAL,KM.DTHRFINAL)) AS float)/3600,2) HORAS, 'PRODUTIVO' TIPO FROM KMAOOBRAALOC KM
INNER JOIN KORDEM K 
ON K.CODCOLIGADA=KM.CODCOLIGADA
AND K.CODFILIAL=KM.CODFILIAL
AND K.CODORDEM=KM.CODORDEM
INNER JOIN KATVORDEM KA 
ON KA.CODCOLIGADA=KM.CODCOLIGADA
AND KA.CODFILIAL=KM.CODFILIAL
AND KA.CODORDEM=K.CODORDEM
AND KA.IDATVORDEM=KM.IDATVORDEM
WHERE KM.EFETIVADO=1 
AND K.STATUS <> 6 
AND KA.STATUS <> 6 
AND K.CODCCUSTO='3.07840.32.001'
UNION ALL
SELECT ROUND(CAST(SUM(DATEDIFF(SECOND,KM.DTHRINICIO,KM.DTHRFIM)) AS float)/3600,2) HORAS,'PARADA' TIPO FROM KPARADAATV KM
INNER JOIN KORDEM K 
ON K.CODCOLIGADA=KM.CODCOLIGADA
AND K.CODFILIAL=KM.CODFILIAL
AND K.CODORDEM=KM.CODORDEM
INNER JOIN KATVORDEM KA 
ON KA.CODCOLIGADA=KM.CODCOLIGADA
AND KA.CODFILIAL=KM.CODFILIAL
AND KA.CODORDEM=K.CODORDEM
AND KA.IDATVORDEM=KM.IDATVORDEM
WHERE 
K.STATUS <> 6 AND KA.STATUS <> 6 AND 
K.CODCCUSTO='3.07840.32.001' 
UNION ALL
SELECT SUM(HORAS), 'TOTAL' TIPO FROM (
SELECT ROUND(CAST(SUM(DATEDIFF(SECOND,KM.DTHRINICIAL,KM.DTHRFINAL)) AS float)/3600,2) HORAS FROM KMAOOBRAALOC KM
INNER JOIN KORDEM K 
ON K.CODCOLIGADA=KM.CODCOLIGADA
AND K.CODFILIAL=KM.CODFILIAL
AND K.CODORDEM=KM.CODORDEM
INNER JOIN KATVORDEM KA 
ON KA.CODCOLIGADA=KM.CODCOLIGADA
AND KA.CODFILIAL=KM.CODFILIAL
AND KA.CODORDEM=K.CODORDEM
AND KA.IDATVORDEM=KM.IDATVORDEM
WHERE KM.EFETIVADO=1 
AND K.STATUS <> 6 
AND KA.STATUS <> 6 
AND K.CODCCUSTO='3.07840.32.001'
UNION ALL
SELECT ROUND(CAST(SUM(DATEDIFF(SECOND,KM.DTHRINICIO,KM.DTHRFIM)) AS float)/3600,2) HORAS FROM KPARADAATV KM
INNER JOIN KORDEM K 
ON K.CODCOLIGADA=KM.CODCOLIGADA
AND K.CODFILIAL=KM.CODFILIAL
AND K.CODORDEM=KM.CODORDEM
INNER JOIN KATVORDEM KA 
ON KA.CODCOLIGADA=KM.CODCOLIGADA
AND KA.CODFILIAL=KM.CODFILIAL
AND KA.CODORDEM=K.CODORDEM
AND KA.IDATVORDEM=KM.IDATVORDEM
WHERE 
K.STATUS <> 6 AND KA.STATUS <> 6 AND 
K.CODCCUSTO='3.07840.32.001' ) SOMAR



--SELECT * FROM KPARADAATV