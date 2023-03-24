WITH TAB_CTE AS (
SELECT
DISTINCT
CODCCUSTO,
       TAG,
       DESCRICAO,
       CCUSTOPOSTO,
       NOMEPOSTO,
       SUM(PERCENTUAL_CONCLUIDO)OVER (PARTITION BY NOMEPOSTO,codordem) PERCENTUAL_CONCLUIDO,
       CODORDEM,
       Sum(HORAS_PREVISTAS) OVER (PARTITION BY NOMEPOSTO,codordem)  HORAS_PREVISTAS,
       Sum(HORAS_APONTADAS) OVER (PARTITION BY NOMEPOSTO, codordem) HORAS_APONTADAS
FROM   (SELECT CODCCUSTO,
               TAG,
               DESCRICAO,
               CCUSTOPOSTO,
               NOMEPOSTO,
               CODORDEM,
               CONVERT(DECIMAL(10, 2), PERCENTUAL_CONCLUIDO * 100) PERCENTUAL_CONCLUIDO,
               CONVERT(DECIMAL(10, 2), Sum(HR_PREVISTAS))          HORAS_PREVISTAS,
               CONVERT(DECIMAL(10, 2), Sum(HR_APONTADAS))          HORAS_APONTADAS
        FROM   (
        SELECT * FROM 
(
        SELECT O.CODCCUSTO,
                       CL.TAG,
                       C.CODCCUSTO                                           CCUSTOPOSTO,
                       C.NOME                                                NOMEPOSTO,
                       M.DESCRICAO,
                       A.CODCOLIGADA,
                       A.CODFILIAL,
                       A.CODORDEM,
                       A.IDATVORDEM,
                       ( A.PERCENTUAL / 100.000 )                    PERCENTUAL_CONCLUIDO,
                       Round(A.TEMPO / 60.0000, 4)                           HR_PREVISTAS,
                       ISNULL((SELECT Sum(Round(Cast(Datediff(SECOND, K.DTHRINICIOPREV, K.DTHRFINALPREV) AS NUMERIC(15, 4)) / 3600, 4))
                               FROM   KATVORDEMPROCESSO K
                               WHERE  K.CODCOLIGADA = A.CODCOLIGADA
                                      AND K.CODFILIAL = A.CODFILIAL
                                      AND K.CODORDEM = A.CODORDEM
                                      AND K.IDATVORDEM = A.IDATVORDEM
                                      AND K.EFETIVADO = 1
                                      AND K.CODORDEM IN (SELECT CODORDEM
                                                         FROM   KORDEM
                                                         WHERE  RESPONSAVEL LIKE '%;%')
                                      AND K.DTHRFINALPREV <= :DATAFINAL), 0) HR_APONTADAS
                FROM   KATVORDEM A
                       INNER JOIN KORDEM O
                               ON A.CODCOLIGADA = O.CODCOLIGADA
                                  AND A.CODFILIAL = O.CODFILIAL
                                  AND A.CODORDEM = O.CODORDEM
                       INNER JOIN MTAREFA T
                               ON A.CODCOLIGADA = T.CODCOLIGADA
                                  AND A.CODORDEM = T.CODTRFAUX
                                  AND T.CODTRFAUX IS NOT NULL
                       INNER JOIN MPRJ M
                               ON M.CODCOLIGADA = T.CODCOLIGADA
                                  AND M.IDPRJ = T.IDPRJ
                       INNER JOIN MTRFCOMPL CL
                               ON CL.CODCOLIGADA = T.CODCOLIGADA
                                  AND CL.IDPRJ = T.IDPRJ
                                  AND CL.IDTRF = T.IDTRF
                       INNER JOIN KPOSTO P
                               ON A.CODCOLIGADA = P.CODCOLIGADA
                                  AND A.CODFILIAL = P.CODFILIAL
                                  AND A.CODPOSTO = P.CODPOSTO
                       INNER JOIN GCCUSTO C
                               ON C.CODCOLIGADA = P.CODCOLIGADA
                                  AND C.CODCCUSTO = P.CODCCUSTO
                WHERE  O.CODCCUSTO = :CCUSTO
                       AND CL.TAG like :TAG
                       AND A.CODFILIAL = :FILIAL
                       AND A.CODCOLIGADA = :COLIGADA

					   group by o.CODCCUSTO, cl.tag, c.codccusto, c.nome, m.descricao, a.codcoligada, a.codfilial, a.codordem, a.idatvordem, a.tempo, A.PERCENTUAL 
                       ) TAB 

                       )                       AS HORAS
        GROUP  BY CODCCUSTO,
                  TAG,
                  CCUSTOPOSTO,
                  NOMEPOSTO,
                  DESCRICAO,
                  CODCOLIGADA,
                  CODFILIAL,
                  CODORDEM,
                  PERCENTUAL_CONCLUIDO) AS AAA

GROUP  BY NOMEPOSTO,
          CODCCUSTO,
          TAG,
          CCUSTOPOSTO,
		  HORAS_PREVISTAS,
		  HORAS_APONTADAS,
          CODORDEM,
          DESCRICAO,
          PERCENTUAL_CONCLUIDO
		  )

SELECT
DISTINCT
CODCCUSTO,DSCORDEM,
       TAG,
       DESCRICAO,
       CCUSTOPOSTO,
       NOMEPOSTO,
       SUM(PERCENTUAL_CONCLUIDO)OVER (PARTITION BY NOMEPOSTO,codordem) PERCENTUAL_CONCLUIDO,
       CODORDEM,
       Sum(HORAS_PREVISTAS) OVER (PARTITION BY NOMEPOSTO,codordem)  HORAS_PREVISTAS,
       Sum(HORAS_APONTADAS) OVER (PARTITION BY NOMEPOSTO, codordem) HORAS_APONTADAS
FROM   (SELECT CODCCUSTO,DSCORDEM,
               TAG,
               DESCRICAO,
               CCUSTOPOSTO,
               NOMEPOSTO,
               CODORDEM,
               CONVERT(DECIMAL(10, 2), PERCENTUAL_CONCLUIDO * 100) PERCENTUAL_CONCLUIDO,
               CONVERT(DECIMAL(10, 2), Sum(HR_PREVISTAS))          HORAS_PREVISTAS,
               CONVERT(DECIMAL(10, 2), Sum(HR_APONTADAS))          HORAS_APONTADAS
        FROM   (
        SELECT * FROM 
(
        SELECT O.CODCCUSTO,O.DSCORDEM,
                       CL.TAG,
                       C.CODCCUSTO                                           CCUSTOPOSTO,
                       C.NOME                                                NOMEPOSTO,
                       M.DESCRICAO,
                       A.CODCOLIGADA,
                       A.CODFILIAL,
                       A.CODORDEM,
                       A.IDATVORDEM,
                       ( A.PERCENTUAL / 100.000 )                    PERCENTUAL_CONCLUIDO,
                       Round(A.TEMPO / 60.0000, 4)                           HR_PREVISTAS,
                       ISNULL((SELECT Sum(Round(Cast(Datediff(SECOND, K.DTHRINICIOPREV, K.DTHRFINALPREV) AS NUMERIC(15, 4)) / 3600, 4))
                               FROM   KATVORDEMPROCESSO K
                               WHERE  K.CODCOLIGADA = A.CODCOLIGADA
                                      AND K.CODFILIAL = A.CODFILIAL
                                      AND K.CODORDEM = A.CODORDEM
                                      AND K.IDATVORDEM = A.IDATVORDEM
                                      AND K.EFETIVADO = 1
                                      AND K.CODORDEM IN (SELECT CODORDEM
                                                         FROM   KORDEM
                                                         WHERE  RESPONSAVEL LIKE '%;%')
                                      AND K.DTHRFINALPREV <= :DATAFINAL), 0) HR_APONTADAS
                FROM   KATVORDEM A
                       INNER JOIN KORDEM O
                               ON A.CODCOLIGADA = O.CODCOLIGADA
                                  AND A.CODFILIAL = O.CODFILIAL
                                  AND A.CODORDEM = O.CODORDEM
                       INNER JOIN MTAREFA T
                               ON A.CODCOLIGADA = T.CODCOLIGADA
                                  AND A.CODORDEM = T.CODTRFAUX
                                  AND T.CODTRFAUX IS NOT NULL
                       INNER JOIN MPRJ M
                               ON M.CODCOLIGADA = T.CODCOLIGADA
                                  AND M.IDPRJ = T.IDPRJ
                       INNER JOIN MTRFCOMPL CL
                               ON CL.CODCOLIGADA = T.CODCOLIGADA
                                  AND CL.IDPRJ = T.IDPRJ
                                  AND CL.IDTRF = T.IDTRF
                       INNER JOIN KPOSTO P
                               ON A.CODCOLIGADA = P.CODCOLIGADA
                                  AND A.CODFILIAL = P.CODFILIAL
                                  AND A.CODPOSTO = P.CODPOSTO
                       INNER JOIN GCCUSTO C
                               ON C.CODCOLIGADA = P.CODCOLIGADA
                                  AND C.CODCCUSTO = P.CODCCUSTO
                WHERE  O.CODCCUSTO = :CCUSTO
                       AND CL.TAG like :TAG
                       AND A.CODFILIAL = :FILIAL
                       AND A.CODCOLIGADA = :COLIGADA
                       AND M.REVISAO = (SELECT Max(REVISAO) FROM MPRJ where codprj = :CCUSTO)

					   group by O.DSCORDEM,o.CODCCUSTO, cl.tag, c.codccusto, c.nome, m.descricao, a.codcoligada, a.codfilial, a.codordem, a.idatvordem, a.tempo, A.PERCENTUAL 
                       ) TAB 

                       )                       AS HORAS
        GROUP  BY CODCCUSTO,
                  TAG,DSCORDEM,
                  CCUSTOPOSTO,
                  NOMEPOSTO,DSCORDEM,
                  DESCRICAO,
                  CODCOLIGADA,
                  CODFILIAL,
                  CODORDEM,
                  PERCENTUAL_CONCLUIDO) AS AAA
				    WHERE TAG IN (SELECT TCTE.TAG FROM TAB_CTE TCTE WHERE TCTE.HORAS_APONTADAS>0)
GROUP  BY NOMEPOSTO,
          CODCCUSTO,
          TAG,DSCORDEM,
          CCUSTOPOSTO,
		  HORAS_PREVISTAS,
		  HORAS_APONTADAS,
          CODORDEM,
          DESCRICAO,
          PERCENTUAL_CONCLUIDO


ORDER  BY TAG,
          CODORDEM,
          NOMEPOSTO 