DECLARE @OS VARCHAR(30)
DECLARE @CODTRFPAI VARCHAR(30)
DECLARE @EXECUCAO VARCHAR(100)

SET @OS='3.07840.32.001'
SET @EXECUCAO='1,2,'
SET @CODTRFPAI = '1.20'

;WITH EXECUCAO as (	

			select left(@EXECUCAO,CHARINDEX(',',@EXECUCAO)-1) param,SUBSTRING(@EXECUCAO,CHARINDEX(',',@EXECUCAO)+1,LEN(@EXECUCAO)) param1 
			union all 
			select left(param1,CHARINDEX(',',param1)-1) param,SUBSTRING(param1,CHARINDEX(',',param1)+1,LEN(param1)) param1
			from EXECUCAO  
			where len(param1)>1

)
, CTE0
     AS (SELECT *
           FROM (SELECT
        EX1.INDICE,EX1.EXECUCAO,EX1.OS,EX1.CODTRFPAI,K.CODORDEM,KA.IDATVORDEM,A.DSCATIVIDADE,K.STATUS
        STATUS_OP,
                KA.STATUS STATUS_ATV,KA.PERCENTUAL,(SELECT Count(*)
                        FROM KMAOOBRAALOC
                        WHERE CODCOLIGADA = KA.CODCOLIGADA
                              AND CODFILIAL = KA.CODFILIAL
                              AND CODORDEM = KA.CODORDEM
                              AND IDATVORDEM = KA.IDATVORDEM
                              AND EFETIVADO = 1) APONT,CASE
                                                         WHEN
                PAPC.NUMPLANOCORTE IS NULL
                                                       THEN 0
                                                         ELSE
                                                           CASE
                WHEN PAPC.QTDEAPONTADA IS
                     NULL THEN
                        CASE
                          WHEN PAPC.QUANTIDADE
                               - KA.QUANTIDADE < 0 THEN 0
                          ELSE PAPC.QUANTIDADE - KA.QUANTIDADE
                        END
                                                             ELSE
                PAPC.QUANTIDADE - PAPC.QTDEAPONTADA
                    END
                END AS SALDOPAPC,
                (SELECT Count(*)
                FROM (SELECT ID
                FROM ZMDKATVORDEMPROGRAMACAO (NOLOCK)
                WHERE CODCOLIGADA = KA.CODCOLIGADA
                AND CODFILIAL = KA.CODFILIAL
                AND CODORDEM = KA.CODORDEM
                AND IDATVORDEM = KA.IDATVORDEM
                AND STATUS IN ( 0, 1 )
                UNION
                SELECT ID
                FROM KMAOOBRAALOC (NOLOCK)
                WHERE CODCOLIGADA = KA.CODCOLIGADA
                AND CODFILIAL = KA.CODFILIAL
                AND CODORDEM = KA.CODORDEM
                AND IDATVORDEM = KA.IDATVORDEM
                AND EFETIVADO = 0) R) PROG,
                CASE
                WHEN KA.CODATIVIDADE LIKE 'WATVOO5%' THEN 1
                ELSE 0
                END AS CQ,CASE
                WHEN PAPC.NUMPLANOCORTE IS NULL THEN '0'
                ELSE PAPC.NUMPLANOCORTE
                END AS PAPC
                   FROM FLUIG.DBO.Z_CRM_EX001005 (NOLOCK) EX1
                        INNER JOIN KORDEM (NOLOCK) K
                                ON K.CODCOLIGADA = EX1.CODCOLIGADA
                                   AND K.CODFILIAL = EX1.CODFILIAL
                                   AND
                        K.CODCCUSTO = EX1.OS COLLATE
                                      SQL_LATIN1_GENERAL_CP1_CI_AI
                        INNER JOIN KATVORDEM (NOLOCK) KA
                                ON KA.CODCOLIGADA = K.CODCOLIGADA
                                   AND KA.CODFILIAL = K.CODFILIAL
                                   AND KA.CODORDEM = K.CODORDEM
                                   AND KA.CODESTRUTURA = EX1.CODIGOPRD COLLATE
                                       SQL_LATIN1_GENERAL_CP1_CI_AI
                        LEFT JOIN ZMDPLANOAPROVEITAMENTOCORTE (NOLOCK) PAPC
                               ON PAPC.CODCOLIGADA = KA.CODCOLIGADA
                                  AND PAPC.CODFILIAL = KA.CODFILIAL
                                  AND PAPC.CODORDEM = KA.CODORDEM
                                  AND PAPC.CODATIVIDADE = KA.CODATIVIDADE
                        INNER JOIN KATIVIDADE (NOLOCK) A
                                ON A.CODCOLIGADA = KA.CODCOLIGADA
                                   AND A.CODFILIAL = KA.CODFILIAL
                                   AND A.CODATIVIDADE = KA.CODATIVIDADE
                        INNER JOIN KORDEMCOMPL (NOLOCK) KL
                                ON KL.CODCOLIGADA = KA.CODCOLIGADA
                                   AND KL.CODFILIAL = KA.CODFILIAL
                                   AND KL.CODORDEM = KA.CODORDEM
                                   AND KL.NUMEXEC = EX1.EXECUCAO
                  WHERE EX1.ITEMEXCLUSIVO <> 2
                        AND OS = @OS
                        AND EX1.EXECUCAO IN (SELECT DISTINCT PARAM FROM EXECUCAO)
                        AND EX1.CODTRFPAI = @CODTRFPAI
                        AND K.STATUS NOT IN ( 5, 6 )
                        AND KA.STATUS NOT IN ( 5, 6 )
                        AND COALESCE(K.REPROCESSAMENTO, 0) = 0) R),
     CTE
     AS (SELECT EXECUCAO,OS,CODTRFPAI,CODCOLIGADA,CODFILIAL,INDICE,CODESTRUTURA,CODORDEM,
                IDATVORDEM,
                   CODATIVIDADE,SALDOPAPC,PAPC,SUBSTITUTO IDPRODUTO,GRUPO,
                MASKPRD,Sum
                (
                   PREVISTO) PREVISTO,Sum(APONTADO) APONTADO
           FROM (SELECT *,CASE
                            WHEN PREVISTO = 0 THEN 'SUBSTITUTO'
                            ELSE 'PRINCIPAL'
                          END AS POSICAO,CASE
                                           WHEN PREVISTO = 0 THEN
                                             CASE
                                               WHEN COALESCE((SELECT IDPRDORIGEM
                                                                FROM
                                                    KCOMPSUBSTITUTO
                                                               WHERE IDPRD =
                                                             IDPRODUTO
                                                                     AND
        CODESTRUTURA = R2.CODESTRUTURA
                AND
        CODATIVIDADE = R2.CODATIVIDADE), 0) = 0 THEN
        (
        SELECT
        TOP 1 IDPRODUTO
        FROM
        KATVORDEMMP
        WHERE
        CODCOLIGADA = R2.CODCOLIGADA
        AND CODORDEM = R2.CODORDEM
        AND IDATIVIDADE = R2.IDATVORDEM
        AND EFETIVADO = 0
        AND IDPRODUTO IN (SELECT IDPRD
            FROM TPRODUTO
           WHERE CODCOLPRD = R2.CODCOLIGADA
                 AND
         NOMEFANTASIA = R2.NOMEFANTASIA
                 AND INATIVO = 0
                 AND CODIGOPRD LIKE
                     '03.023%'))
        ELSE (SELECT IDPRDORIGEM
        FROM KCOMPSUBSTITUTO
        WHERE IDPRD = IDPRODUTO
        AND CODESTRUTURA = R2.CODESTRUTURA
        AND CODATIVIDADE = R2.CODATIVIDADE)
        END
        ELSE R2.IDPRODUTO
        END AS SUBSTITUTO
        FROM (SELECT EXECUCAO,OS,CODTRFPAI,CODCOLIGADA,CODFILIAL,INDICE,CODESTRUTURA,CODORDEM
                     ,
        IDATVORDEM,
        CODATIVIDADE,SALDOPAPC,PAPC,IDPRODUTO,
        Sum(PREVISTO) PREVISTO,Sum
        (APONTADO) APONTADO,CODTB2FAT GRUPO,MASKPRD,CODIGOPRD,
        NOMEFANTASIA
			FROM (SELECT
						EX1.EXECUCAO,EX1.OS,EX1.CODTRFPAI,KA.CODCOLIGADA,KA.CODFILIAL,EX1.INDICE,KA.CODESTRUTURA,K.CODORDEM,KA.IDATVORDEM,KA.CODATIVIDADE,CASE
						WHEN PAPC.NUMPLANOCORTE IS NULL THEN 0
						ELSE
						CASE
						WHEN PAPC.QTDEAPONTADA IS NULL THEN
						CASE
						WHEN PAPC.QUANTIDADE - KA.QUANTIDADE < 0 THEN 0
						ELSE PAPC.QUANTIDADE - KA.QUANTIDADE
						END
						ELSE PAPC.QUANTIDADE - PAPC.QTDEAPONTADA
						END
						END AS SALDOPAPC,CASE
						WHEN PAPC.NUMPLANOCORTE IS NULL THEN '0'
						ELSE PAPC.NUMPLANOCORTE
						END AS PAPC,KMA.IDPRODUTO,CASE
								WHEN
						KMA.EFETIVADO = 0 THEN KMA.QUANTIDADE
								ELSE 0
							  END AS PREVISTO,CASE
						WHEN KMA.EFETIVADO = 1 THEN KMA.QUANTIDADE
						ELSE 0
											  END
						AS APONTADO,(SELECT CODTB2FAT
						FROM TPRODUTODEF
						WHERE CODCOLIGADA = KA.CODCOLIGADA
						AND IDPRD = KMA.IDPRODUTO) CODTB2FAT,
						LEFT(P.CODIGOPRD, 6) MASKPRD,P.CODIGOPRD,P.NOMEFANTASIA
						FROM FLUIG.DBO.Z_CRM_EX001005 (NOLOCK) EX1
						INNER JOIN KORDEM (NOLOCK) K
						ON K.CODCOLIGADA = EX1.CODCOLIGADA
						AND K.CODFILIAL = EX1.CODFILIAL
						AND K.CODCCUSTO = EX1.OS COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI
						INNER JOIN KATVORDEM (NOLOCK) KA
						ON KA.CODCOLIGADA = K.CODCOLIGADA
						AND KA.CODFILIAL = K.CODFILIAL
						AND KA.CODORDEM = K.CODORDEM
						AND KA.CODESTRUTURA = EX1.CODIGOPRD COLLATE
						SQL_LATIN1_GENERAL_CP1_CI_AI
						LEFT JOIN ZMDPLANOAPROVEITAMENTOCORTE (NOLOCK) PAPC
						ON PAPC.CODCOLIGADA = KA.CODCOLIGADA
						AND PAPC.CODFILIAL = KA.CODFILIAL
						AND PAPC.CODORDEM = KA.CODORDEM
						AND PAPC.CODATIVIDADE = KA.CODATIVIDADE
						INNER JOIN KORDEMCOMPL (NOLOCK) KL
						ON KL.CODCOLIGADA = KA.CODCOLIGADA
						AND KL.CODFILIAL = KA.CODFILIAL
						AND KL.CODORDEM = KA.CODORDEM
						AND KL.NUMEXEC = EX1.EXECUCAO
						INNER JOIN KATVORDEMMP (NOLOCK) KMA
						ON KMA.CODCOLIGADA = KA.CODCOLIGADA
						AND KMA.CODFILIAL = KA.CODFILIAL
						AND KMA.CODORDEM = KA.CODORDEM
						AND KMA.IDATIVIDADE = KA.IDATVORDEM
						INNER JOIN TPRD (NOLOCK) P
						ON KMA.CODCOLIGADA = P.CODCOLIGADA
						AND KMA.IDPRODUTO = P.IDPRD
						WHERE EX1.ITEMEXCLUSIVO <> 2
						AND OS = @OS
						AND EX1.EXECUCAO IN (SELECT DISTINCT PARAM FROM EXECUCAO)
						AND EX1.CODTRFPAI = @CODTRFPAI
						AND K.STATUS NOT IN ( 6 )
						AND KA.STATUS NOT IN ( 6 )
						AND COALESCE(K.REPROCESSAMENTO, 0) = 0) R
						WHERE ( CODTB2FAT = 0184
						OR MASKPRD NOT IN ( '01.053', '01.019', '01.020' ) )
						AND ( PAPC = '0'
						OR SALDOPAPC > 0 )
					GROUP BY EXECUCAO,OS,CODTRFPAI,CODCOLIGADA,CODFILIAL,INDICE,CODORDEM,IDATVORDEM,SALDOPAPC,
					PAPC,IDPRODUTO,CODTB2FAT,MASKPRD,CODATIVIDADE,CODESTRUTURA,CODIGOPRD,
					NOMEFANTASIA) R2) R3
				GROUP BY EXECUCAO,OS,CODTRFPAI,CODCOLIGADA,CODFILIAL,INDICE,CODESTRUTURA,CODORDEM,
					  IDATVORDEM,CODATIVIDADE,SALDOPAPC,PAPC,SUBSTITUTO,GRUPO,MASKPRD),
     CTE2
     AS (SELECT *
           FROM CTE C
          WHERE ( ( PREVISTO - APONTADO ) > 0
                  AND ( GRUPO != '0184'
                         OR GRUPO IS NULL ) )
                 OR ( APONTADO <= 0
                      AND GRUPO = '0184' )),
     CTE3
     AS (SELECT *
           FROM (SELECT DISTINCT INDICE,K.CODORDEM,EX1.EXECUCAO,EX1.CODTRFPAI,EX1.OS
                   FROM FLUIG.DBO.Z_CRM_EX001005 (NOLOCK) EX1
                        INNER JOIN KORDEM (NOLOCK) K
                                ON K.CODCOLIGADA = EX1.CODCOLIGADA
                                   AND K.CODFILIAL = EX1.CODFILIAL
                                   AND
                        K.CODCCUSTO = EX1.OS COLLATE
                                      SQL_LATIN1_GENERAL_CP1_CI_AI
                        INNER JOIN KATVORDEM KA (NOLOCK)
                                ON KA.CODCOLIGADA = K.CODCOLIGADA
                                   AND KA.CODFILIAL = K.CODFILIAL
                                   AND KA.CODORDEM = K.CODORDEM
                                   AND KA.CODESTRUTURA = EX1.CODIGOPRD COLLATE
                                       SQL_LATIN1_GENERAL_CP1_CI_AI
                        INNER JOIN KORDEMCOMPL (NOLOCK) KL
                                ON KL.CODCOLIGADA = K.CODCOLIGADA
                                   AND KL.CODFILIAL = K.CODFILIAL
                                   AND KL.CODORDEM = K.CODORDEM
                                   AND KL.NUMEXEC = EX1.EXECUCAO
                  WHERE EX1.ITEMEXCLUSIVO <> 2
                        AND OS = @OS
                        AND EX1.EXECUCAO IN (SELECT DISTINCT PARAM FROM EXECUCAO)
                        AND EX1.CODTRFPAI = @CODTRFPAI
                        AND K.STATUS NOT IN ( 5, 6 )
                        AND COALESCE(K.REPROCESSAMENTO, 0) = 0) R),
						CTE4 AS (
SELECT *
  FROM(SELECT DISTINCT INDICE,OS,EXECUCAO,CODTRFPAI
         FROM CTE0
       UNION
       SELECT DISTINCT INDICE,OS,EXECUCAO,CODTRFPAI
         FROM CTE2
       UNION
       SELECT DISTINCT INDICE,OS,EXECUCAO,CODTRFPAI
         FROM CTE3) R )


SELECT *,concat(round(cast(PENDENTE as float)*100/cast(TOTAL as float),2),'%') PERCENTUAL FROM (
SELECT COUNT(A) TOTAL, COUNT(B) PENDENTE, OS,CODTRFPAI,EXECUCAO FROM (
SELECT Z.INDICE A,C.INDICE B,Z.OS,Z.CODTRFPAI,Z.EXECUCAO FROM FLUIG.DBO.Z_CRM_EX001005 Z 
LEFT JOIN CTE4 C ON
C.INDICE=Z.INDICE
AND C.OS=Z.OS
AND C.EXECUCAO=Z.EXECUCAO
AND C.OS=Z.OS
WHERE Z.OS=@OS AND Z.EXECUCAO IN (SELECT DISTINCT PARAM FROM EXECUCAO) AND Z.CODTRFPAI=@CODTRFPAI AND Z.ITEMEXCLUSIVO <> 2 ) R
GROUP BY OS,CODTRFPAI,EXECUCAO ) R2