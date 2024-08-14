;

WITH CTE AS (
    SELECT
        KA.CODCOLIGADA,
        KA.CODFILIAL,
        EX1.INDICE,
        KA.CODESTRUTURA,
        K.CODORDEM,
        KA.IDATVORDEM,
        KA.CODATIVIDADE,
        CASE
            WHEN PAPC.NUMPLANOCORTE IS NULL THEN 0
            ELSE CASE
                WHEN PAPC.QTDEAPONTADA IS NULL THEN CASE
                    WHEN PAPC.QUANTIDADE - KA.QUANTIDADE < 0 THEN 0
                    ELSE PAPC.QUANTIDADE - KA.QUANTIDADE
                END
                ELSE PAPC.QUANTIDADE - PAPC.QTDEAPONTADA
            END
        END AS SALDOPAPC,
        CASE
            WHEN PAPC.NUMPLANOCORTE IS NULL THEN '0'
            ELSE PAPC.NUMPLANOCORTE
        END AS PAPC,
        KMA.IDPRODUTO,
        SUM(
            CASE
                WHEN KMA.EFETIVADO = 0 THEN KMA.QUANTIDADE
                ELSE 0
            END
        ) AS PREVISTO,
        SUM(
            CASE
                WHEN KMA.EFETIVADO = 1 THEN KMA.QUANTIDADE
                ELSE 0
            END
        ) AS APONTADO,
        CODTB2FAT GRUPO,
        LEFT(P.CODIGOPRD, 6) MASKPRD,
        P.NOMEFANTASIA
    FROM
        FLUIG.DBO.Z_CRM_EX001005 (NOLOCK) EX1
        INNER JOIN KORDEM (NOLOCK) K ON K.CODCOLIGADA = EX1.CODCOLIGADA
        AND K.CODFILIAL = EX1.CODFILIAL
        AND K.CODCCUSTO = EX1.OS COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI
        INNER JOIN KATVORDEM (NOLOCK) KA ON KA.CODCOLIGADA = K.CODCOLIGADA
        AND KA.CODFILIAL = K.CODFILIAL
        AND KA.CODORDEM = K.CODORDEM
        AND KA.CODESTRUTURA = EX1.CODIGOPRD COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI
        INNER JOIN KORDEMCOMPL (NOLOCK) KL ON KL.CODCOLIGADA = KA.CODCOLIGADA
        AND KL.CODFILIAL = KA.CODFILIAL
        AND KL.CODORDEM = KA.CODORDEM
        AND KL.NUMEXEC = EX1.EXECUCAO
        INNER JOIN KATVORDEMMP (NOLOCK) KMA ON KMA.CODCOLIGADA = KA.CODCOLIGADA
        AND KMA.CODFILIAL = KA.CODFILIAL
        AND KMA.CODORDEM = KA.CODORDEM
        AND KMA.IDATIVIDADE = KA.IDATVORDEM
        INNER JOIN TPRD (NOLOCK) P ON KMA.CODCOLIGADA = P.CODCOLIGADA
        AND KMA.IDPRODUTO = P.IDPRD
        LEFT JOIN ZMDPLANOAPROVEITAMENTOCORTE (NOLOCK) PAPC ON PAPC.CODCOLIGADA = KA.CODCOLIGADA
        AND PAPC.CODFILIAL = KA.CODFILIAL
        AND PAPC.CODORDEM = KA.CODORDEM
        AND PAPC.CODATIVIDADE = KA.CODATIVIDADE
    WHERE
        EX1.ITEMEXCLUSIVO <> 2
        AND K.STATUS NOT IN (5, 6)
        AND KA.STATUS NOT IN (6)
        AND (
            K.REPROCESSAMENTO = 0
            OR K.REPROCESSAMENTO IS NULL
            OR K.REPROCESSAMENTO = ''
        )
        AND (
            CODTB2FAT != 0184
            OR CODTB2FAT IS NULL
        )
        AND LEFT(P.CODIGOPRD, 6) NOT IN ('01.053', '01.019', '01.020')
        AND (
            (
                CASE
                    WHEN PAPC.NUMPLANOCORTE IS NULL THEN '0'
                    ELSE PAPC.NUMPLANOCORTE
                END
            ) = '0'
            OR (
                CASE
                    WHEN PAPC.NUMPLANOCORTE IS NULL THEN 0
                    ELSE CASE
                        WHEN PAPC.QTDEAPONTADA IS NULL THEN CASE
                            WHEN PAPC.QUANTIDADE - KA.QUANTIDADE < 0 THEN 0
                            ELSE PAPC.QUANTIDADE - KA.QUANTIDADE
                        END
                        ELSE PAPC.QUANTIDADE - PAPC.QTDEAPONTADA
                    END
                END
            ) > 0
        )
    GROUP BY
        KA.CODCOLIGADA,
        KA.CODFILIAL,
        EX1.INDICE,
        KA.CODESTRUTURA,
        K.CODORDEM,
        KA.IDATVORDEM,
        KA.CODATIVIDADE,
        (
            CASE
                WHEN PAPC.NUMPLANOCORTE IS NULL THEN '0'
                ELSE PAPC.NUMPLANOCORTE
            END
        ),
        (
            CASE
                WHEN PAPC.NUMPLANOCORTE IS NULL THEN 0
                ELSE CASE
                    WHEN PAPC.QTDEAPONTADA IS NULL THEN CASE
                        WHEN PAPC.QUANTIDADE - KA.QUANTIDADE < 0 THEN 0
                        ELSE PAPC.QUANTIDADE - KA.QUANTIDADE
                    END
                    ELSE PAPC.QUANTIDADE - PAPC.QTDEAPONTADA
                END
            END
        ),
        KMA.IDPRODUTO,
        CODTB2FAT,
        LEFT(P.CODIGOPRD, 6),
        P.NOMEFANTASIA
),
CTE3 AS (
    SELECT
        *,
        CASE
            WHEN PREVISTO = 0 THEN 'SUBSTITUTO'
            ELSE 'PRINCIPAL'
        END AS POSICAO,
        CASE
            WHEN PREVISTO = 0 THEN ISNULL(
                (
                    SELECT
                        TOP 1 IDPRDORIGEM
                    FROM
                        KCOMPSUBSTITUTO
                    WHERE
                        IDPRD = IDPRODUTO
                        AND CODESTRUTURA = R2.CODESTRUTURA
                        AND CODATIVIDADE = R2.CODATIVIDADE
                ),
                (
                    SELECT
                        TOP 1 IDPRODUTO
                    FROM
                        KATVORDEMMP KK
                        INNER JOIN TPRODUTO T ON T.NOMEFANTASIA = R2.NOMEFANTASIA
                        AND INATIVO = 0
                        AND LEFT(CODIGOPRD, 6) = '03.023'
                        AND CODCOLPRD = R2.CODCOLIGADA
                    WHERE
                        CODCOLIGADA = R2.CODCOLIGADA
                        AND CODORDEM = R2.CODORDEM
                        AND IDATIVIDADE = R2.IDATVORDEM
                        AND EFETIVADO = 0
                )
            )
            ELSE R2.IDPRODUTO
        END AS SUBSTITUTO
    FROM
        CTE R2
)
SELECT
    INDICE,
    GRUPO,
    SUM(PREVISTO) PREVISTO,
    SUM(APONTADO) APONTADO
FROM
    CTE3
GROUP BY
    INDICE,
    GRUPO
HAVING
    (
        (SUM(PREVISTO) - SUM(APONTADO)) > 0
        AND (
            GRUPO != '0184'
            OR GRUPO IS NULL
        )
    )
    OR (
        SUM(APONTADO) <= 0
        AND GRUPO = '0184'
    )