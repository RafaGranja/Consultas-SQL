
SELECT
CODCCUSTO,DESCRICAO,
       TAG,
	   DSCORDEM,
       CCUSTOPOSTO,
       NOMEPOSTO,
       SUM(PERCENTUAL_CONCLUIDO)*100 PERCENTUAL_CONCLUIDO,
       CODORDEM,
       REPROCESSAMENTO,
       Sum(HORAS_PREVISTAS)  HORAS_PREVISTAS,
       Sum(HORAS_APONTADAS) HORAS_APONTADAS,
	   Sum(HORAS_PROJETADAS) HORAS_PROJETADAS
FROM   (SELECT CODCCUSTO,DESCRICAO,
               TAG,
               CCUSTOPOSTO,
               NOMEPOSTO,
               CODORDEM,
			   DSCORDEM,
			   REPROCESSAMENTO,
               CONVERT(DECIMAL(10, 2), SUM(PERCENTUAL_CONCLUIDO)/(SELECT COUNT(*) FROM KATVORDEM WHERE CODORDEM=R2.CODORDEM AND STATUS<>6)) PERCENTUAL_CONCLUIDO,
               CONVERT(DECIMAL(10, 2), Sum(HORAS_PREVISTAS))          HORAS_PREVISTAS,
               CONVERT(DECIMAL(10, 2), Sum(HORAS_APONTADAS))          HORAS_APONTADAS,
			   CONVERT(DECIMAL(10, 2), Sum(HORAS_PROJETADAS))         HORAS_PROJETADAS
        FROM   (
        SELECT
		CODCCUSTO,
		TAG,
		CCUSTOPOSTO, 
		NOMEPOSTO,
		DESCRICAO,
		CODCOLIGADA,
		CODFILIAL,
		CODORDEM,
		DSCORDEM,
		IDATVORDEM,
		REPROCESSAMENTO,
		ISNULL(PERCENTUAL_CONCLUIDO,0) PERCENTUAL_CONCLUIDO,
		HR_PREVISTAS HORAS_PREVISTAS,
		HR_APONTADAS HORAS_APONTADAS,
		CASE WHEN HR_APONTADAS = 0 THEN HR_PREVISTAS
			ELSE 
				CASE WHEN PERCENTUAL_CONCLUIDO = 0 AND HR_APONTADAS > 0 THEN 
					CASE WHEN HR_PREVISTAS > HR_APONTADAS THEN  HR_PREVISTAS 
					ELSE HR_APONTADAS 
					END
				ELSE HR_APONTADAS/(CASE WHEN PERCENTUAL_CONCLUIDO > 1.00000 THEN  1.00000 ELSE PERCENTUAL_CONCLUIDO END) 
				END
			END AS HORAS_PROJETADAS FROM 
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
                       O.REPROCESSAMENTO,
                       ISNULL(( A.PERCENTUAL / 100.000 ),0)                    PERCENTUAL_CONCLUIDO,
                       CASE WHEN ISNULL(O.REPROCESSAMENTO,0) = 0 THEN ROUND((CASE WHEN MLA.ID IS NULL THEN 
																		CASE WHEN MLX.OPSUNITARIAS='SIM' THEN
																			CAST(MLXA.PROCESSAMENTO AS FLOAT)
																		ELSE 
																			CAST(MLXA.PROCESSAMENTO AS FLOAT)*CAST(MLX.TOTALQTDE AS FLOAT)
																			END
																	ELSE 
																		CASE WHEN MLXA.ID IS NULL 
																			
																			THEN A.TEMPO
																		ELSE																		
																			CASE WHEN ML.OPSUNITARIAS='SIM' THEN
																				CAST(MLA.PROCESSAMENTO AS FLOAT)
																			ELSE 
																				CAST(MLA.PROCESSAMENTO AS FLOAT)*CAST(ML.TOTALQTDE AS FLOAT)
																				END
																		END
																	 END)/60.0000, 4) ELSE ROUND((A.TEMPO/60.0000),4) END AS HR_PREVISTAS,
                       ISNULL((SELECT Sum(Round(Cast(Datediff(SECOND, K.DTHRINICIAL, K.DTHRFINAL) AS NUMERIC(15, 4)) / 3600, 4))
                               FROM   KMAOOBRAALOC K (NOLOCK)
                               WHERE  K.CODCOLIGADA = A.CODCOLIGADA
                                      AND K.CODFILIAL = A.CODFILIAL
                                      AND K.CODORDEM = A.CODORDEM
                                      AND K.IDATVORDEM = A.IDATVORDEM
                                      AND K.EFETIVADO = 1
                                       AND K.DTHRFINAL <= :DATAFINAL), 0) HR_APONTADAS
                FROM   KATVORDEM A (NOLOCK)
                       INNER JOIN KORDEM O (NOLOCK)
                               ON A.CODCOLIGADA = O.CODCOLIGADA
                                  AND A.CODFILIAL = O.CODFILIAL
                                  AND A.CODORDEM = O.CODORDEM
                       INNER JOIN MTAREFA T (NOLOCK)
                               ON A.CODCOLIGADA = T.CODCOLIGADA
                                  AND A.CODORDEM = T.CODTRFAUX
                                  AND T.CODTRFAUX IS NOT NULL
                       INNER JOIN MPRJ M (NOLOCK)
                               ON M.CODCOLIGADA = T.CODCOLIGADA
                                  AND M.IDPRJ = T.IDPRJ
                       INNER JOIN MTRFCOMPL CL (NOLOCK)
                               ON CL.CODCOLIGADA = T.CODCOLIGADA
                                  AND CL.IDPRJ = T.IDPRJ
                                  AND CL.IDTRF = T.IDTRF
                       INNER JOIN KPOSTO P (NOLOCK)
                               ON A.CODCOLIGADA = P.CODCOLIGADA
                                  AND A.CODFILIAL = P.CODFILIAL
                                  AND A.CODPOSTO = P.CODPOSTO
                       INNER JOIN GCCUSTO C (NOLOCK)
                               ON C.CODCOLIGADA = P.CODCOLIGADA
                                  AND C.CODCCUSTO = P.CODCCUSTO
                       LEFT JOIN FLUIG.DBO.Z_CRM_ML001005 ML (NOLOCK) 
                       			ON ML.CODCOLIGADA=A.CODCOLIGADA 
                       			 AND ML.CODFILIAL=A.CODFILIAL
								 AND ML.CODIGOPRD=A.CODESTRUTURA COLLATE SQL_Latin1_General_CP1_CI_AI 
								 AND ML.OS=O.CODCCUSTO COLLATE SQL_Latin1_General_CP1_CI_AI
					   LEFT JOIN FLUIG.DBO.Z_CRM_ML001021 MLA (NOLOCK) 
								 ON MLA.IDCRIACAOPROCESSO=ML.IDCRIACAO 
								 AND MLA.CODATIVIDADE=A.CODATIVIDADE COLLATE SQL_Latin1_General_CP1_CI_AI AND MLA.OSPROCESSO=ML.OS
					   LEFT JOIN FLUIG.DBO.Z_CRM_EX001005 MLX (NOLOCK) 
								 ON MLX.CODCOLIGADA=A.CODCOLIGADA AND MLX.CODFILIAL=A.CODFILIAL
								 AND MLX.CODIGOPRD=A.CODESTRUTURA COLLATE SQL_Latin1_General_CP1_CI_AI 
								 AND MLX.ITEMEXCLUSIVO<>2 AND MLX.OS=O.CODCCUSTO COLLATE SQL_Latin1_General_CP1_CI_AI
								 AND MLX.EXECUCAO=(SELECT NUMEXEC FROM KORDEMCOMPL WHERE CODCOLIGADA=O.CODCOLIGADA AND CODFILIAL=O.CODFILIAL AND CODORDEM=O.CODORDEM)
						LEFT JOIN FLUIG.DBO.Z_CRM_EX001021 MLXA (NOLOCK) 
							 	 ON MLXA.IDCRIACAOPROCESSO=MLX.IDCRIACAO 
							 	 AND MLXA.CODATIVIDADE=A.CODATIVIDADE COLLATE SQL_Latin1_General_CP1_CI_AI
							  	 AND MLXA.EXECUCAO=MLX.EXECUCAO AND MLXA.OSPROCESSO=MLX.OS
                WHERE  O.CODCCUSTO = :CCUSTO
                       AND CL.TAG like :TAG
                       AND A.CODFILIAL = :FILIAL
                       AND A.CODCOLIGADA = :COLIGADA
                       AND A.STATUS<>6
					   AND M.REVISAO = (SELECT Max(REVISAO) FROM MPRJ where codprj = :CCUSTO) ) R ) R2
					   group by CODCCUSTO,DESCRICAO,
				   TAG,
				   DESCRICAO,
				   CCUSTOPOSTO,
				   NOMEPOSTO,
				   CODORDEM,
				   DSCORDEM,
				   REPROCESSAMENTO
						   ) TAB 
        GROUP  BY CODCCUSTO,DESCRICAO,
				   TAG,
				   DSCORDEM,
				   CCUSTOPOSTO,
				   NOMEPOSTO,
				   CODORDEM,
				   REPROCESSAMENTO


ORDER  BY TAG,
          CODORDEM,
          NOMEPOSTO 