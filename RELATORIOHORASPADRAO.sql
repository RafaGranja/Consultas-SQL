
with cte as (
SELECT 
CASE
WHEN NOMEPOSTO LIKE 'Prepara%'			 THEN 1
WHEN NOMEPOSTO = 'Pré-Montagem'			 THEN 2
WHEN NOMEPOSTO = 'Montagem Mecânica'	 THEN 3
WHEN NOMEPOSTO = 'Solda Robotizada'		 THEN 4
WHEN NOMEPOSTO = 'Solda Convencional '	 THEN 5
WHEN NOMEPOSTO = 'Forno'				 THEN 6
WHEN NOMEPOSTO = 'Linha Automatizada '	 THEN 7
WHEN NOMEPOSTO = 'Acabamento/Pintura'	 THEN 8
WHEN NOMEPOSTO = 'Usinagem Pesada'		 THEN 9
WHEN NOMEPOSTO = 'Ferramentaria'		 THEN 10
WHEN NOMEPOSTO = 'Controle de Qualidade' THEN 11
END AS ORDEM,
CODCCUSTO,
CCUSTOPOSTO, 
NOMEPOSTO,
DESCRICAO,
CONVERT(decimal(10, 2),SUM(HORAS_PREVISTAS)) HORAS_PREVISTAS,
CONVERT(decimal(10, 2),SUM(HR_PREVISTA_SEM_RETRABALHO)) HR_PREVISTA_SEM_RETRABALHO,
CONVERT(decimal(10, 2),SUM(RETRABALHO_HR_PREVISTA)) RETRABALHO_HR_PREVISTA,
CONVERT(decimal(10, 2),SUM(HORAS_APONTADAS))  HORAS_APONTADAS,
CONVERT(decimal(10, 2),SUM(HR_APONTADAS_SEM_RETRABALHO)) HR_APONTADAS_SEM_RETRABALHO,
CONVERT(decimal(10, 2),SUM(RETRABALHO_HR_APONTADA)) RETRABALHO_HR_APONTADA,
CONVERT(decimal(10, 2),SUM(HORAS_PROJETADAS+RETRABALHO_HR_APONTADA)) HORAS_PROJETADAS
FROM
	(SELECT CODCCUSTO,
		TAG,
		CCUSTOPOSTO, 
		NOMEPOSTO,
		DESCRICAO,
		CODCOLIGADA,
		CODFILIAL,
		CODORDEM,
		SUM(HORAS_PREVISTAS) HORAS_PREVISTAS,
		SUM(HR_PREVISTA_SEM_RETRABALHO) HR_PREVISTA_SEM_RETRABALHO,
		SUM(RETRABALHO_HR_PREVISTA) RETRABALHO_HR_PREVISTA,
		SUM(HORAS_APONTADAS) HORAS_APONTADAS,
		SUM(HR_APONTADAS_SEM_RETRABALHO) HR_APONTADAS_SEM_RETRABALHO,
		SUM(RETRABALHO_HR_APONTADA) RETRABALHO_HR_APONTADA,
		SUM(HORAS_PROJETADAS) HORAS_PROJETADAS
		FROM (
		SELECT
		CODCCUSTO,
		TAG,
		CCUSTOPOSTO, 
		NOMEPOSTO,
		DESCRICAO,
		CODCOLIGADA,
		CODFILIAL,
		CODORDEM,
		IDATVORDEM,
		HR_PREVISTAS HORAS_PREVISTAS,
		HR_PREVISTA_SEM_RETRABALHO HR_PREVISTA_SEM_RETRABALHO,
		RETRABALHO_HR_PREVISTA RETRABALHO_HR_PREVISTA,
		HR_APONTADAS HORAS_APONTADAS,
		HR_APONTADAS_SEM_RETRABALHO HR_APONTADAS_SEM_RETRABALHO,
		RETRABALHO_HR_APONTADA RETRABALHO_HR_APONTADA,
		CASE WHEN HR_APONTADAS_SEM_RETRABALHO = 0 OR PERCENTUAL_CONCLUIDO = 0 THEN HR_PREVISTA_SEM_RETRABALHO
			ELSE 
				CASE WHEN PERCENTUAL_CONCLUIDO = 0 AND HR_APONTADAS_SEM_RETRABALHO > 0 THEN 
					CASE WHEN HR_PREVISTA_SEM_RETRABALHO > HR_APONTADAS_SEM_RETRABALHO THEN  HR_PREVISTA_SEM_RETRABALHO 
					ELSE HR_APONTADAS_SEM_RETRABALHO 
					END
				ELSE HR_APONTADAS_SEM_RETRABALHO/(CASE WHEN PERCENTUAL_CONCLUIDO > 1.00000 THEN  1.00000 ELSE PERCENTUAL_CONCLUIDO END) 
				END
			END AS HORAS_PROJETADAS
		
/*CASE WHEN (SUM(PERCENTUAL_CONCLUIDO)/COUNT(IDATVORDEM)) = 0   THEN SUM(HR_PREVISTAS) ELSE case when (SUM(PERCENTUAL_CONCLUIDO)/COUNT(IDATVORDEM)) >1.0000 then SUM(HR_APONTADAS) else SUM(HR_APONTADAS)/ (SUM(PERCENTUAL_CONCLUIDO)/COUNT(IDATVORDEM)) END end HORAS_PROJETADAS
CASE WHEN (SUM(HR_APONTADAS)=0 OR SUM(PERCENTUAL_CONCLUIDO) = 0) THEN SUM(HR_PREVISTAS)	 ELSE (SUM(HR_APONTADAS) / SUM(PERCENTUAL_CONCLUIDO))   END HORAS_PROJETADAS*/
		FROM (
			SELECT   
			CL.TAG,
			O.CODCCUSTO,
			C.CODCCUSTO CCUSTOPOSTO, 
			C.NOME NOMEPOSTO,
			M.DESCRICAO, 
			A.CODCOLIGADA, 
			A.CODFILIAL, 
			A.CODORDEM, 
			A.IDATVORDEM,
			(A.PERCENTUAL/100.000) PERCENTUAL_CONCLUIDO,
			CASE WHEN ISNULL(O.REPROCESSAMENTO,0) = 0 THEN ROUND((CASE WHEN MLA.ID IS NULL THEN 
																		CASE WHEN MLX.OPSUNITARIAS='SIM' THEN
																			CAST(MLXA.PROCESSAMENTO AS FLOAT)
																		ELSE 
																			CAST(MLXA.PROCESSAMENTO AS FLOAT)*CAST(MLX.TOTALQTDE AS FLOAT)
																			END
																	ELSE
																		CASE WHEN ML.OPSUNITARIAS='SIM' THEN
																			CAST(MLA.PROCESSAMENTO AS FLOAT)
																		ELSE 
																			CAST(MLA.PROCESSAMENTO AS FLOAT)*CAST(ML.TOTALQTDE AS FLOAT)
																			END
																	 END)/60.0000, 4) ELSE ROUND((A.TEMPO/60.0000),4) END AS HR_PREVISTAS,
			CASE WHEN ISNULL(O.REPROCESSAMENTO,0) = 0 THEN ROUND((CASE WHEN MLA.ID IS NULL THEN 
																		CASE WHEN MLX.OPSUNITARIAS='SIM' THEN
																			CAST(MLXA.PROCESSAMENTO AS FLOAT)
																		ELSE 
																			CAST(MLXA.PROCESSAMENTO AS FLOAT)*CAST(MLX.TOTALQTDE AS FLOAT)
																			END
																	ELSE
																		CASE WHEN ML.OPSUNITARIAS='SIM' THEN
																			CAST(MLA.PROCESSAMENTO AS FLOAT)
																		ELSE 
																			CAST(MLA.PROCESSAMENTO AS FLOAT)*CAST(ML.TOTALQTDE AS FLOAT)
																			END
																	 END)/60.0000, 4) ELSE 0 END AS HR_PREVISTA_SEM_RETRABALHO,
			CASE WHEN O.REPROCESSAMENTO =1 THEN ROUND(A.TEMPO/60.0000, 4) ELSE 0 END AS RETRABALHO_HR_PREVISTA,


			ISNULL((  SELECT SUM(ROUND(CAST(DATEDIFF(SECOND,K.DTHRINICIAL,K.DTHRFINAL) AS numeric(15,4))/3600,4))
				FROM KMAOOBRAALOC K 
				WHERE K.CODCOLIGADA = A.CODCOLIGADA
				AND	K.CODFILIAL = A.CODFILIAL
				AND	K.CODORDEM = A.CODORDEM
				AND	K.IDATVORDEM = A.IDATVORDEM
				AND	K.EFETIVADO = 1
				),0) HR_APONTADAS,

				ISNULL((  SELECT SUM(ROUND(CAST(DATEDIFF(SECOND,K.DTHRINICIAL,K.DTHRFINAL) AS numeric(15,4))/3600,4))
				FROM KMAOOBRAALOC K 
				WHERE K.CODCOLIGADA = A.CODCOLIGADA
				AND	K.CODFILIAL = A.CODFILIAL
				AND	K.CODORDEM = A.CODORDEM
				AND	K.IDATVORDEM = A.IDATVORDEM
				AND	K.EFETIVADO = 1
				AND ISNULL(O.REPROCESSAMENTO,0) = 0
				),0) HR_APONTADAS_SEM_RETRABALHO,

				ISNULL((  SELECT SUM(ROUND(CAST(DATEDIFF(SECOND,K.DTHRINICIAL,K.DTHRFINAL) AS numeric(15,4))/3600,4))
				FROM KMAOOBRAALOC K 
				WHERE K.CODCOLIGADA = A.CODCOLIGADA
				AND	K.CODFILIAL = A.CODFILIAL
				AND	K.CODORDEM = A.CODORDEM
				AND	K.IDATVORDEM = A.IDATVORDEM
				AND	K.EFETIVADO = 1
				AND O.REPROCESSAMENTO =1
				),0) RETRABALHO_HR_APONTADA

			FROM KATVORDEM A
				INNER JOIN KORDEM O ON A.CODCOLIGADA = O.CODCOLIGADA AND A.CODFILIAL = O.CODFILIAL AND A.CODORDEM = O.CODORDEM
				INNER JOIN MTAREFA T ON A.CODCOLIGADA = T.CODCOLIGADA AND A.CODORDEM = T.CODTRFAUX
				INNER JOIN MPRJ M ON M.CODCOLIGADA = T.CODCOLIGADA AND M.IDPRJ = T.IDPRJ 
				INNER JOIN MTRFCOMPL CL ON CL.CODCOLIGADA = T.CODCOLIGADA AND CL.IDPRJ = T.IDPRJ AND CL.IDTRF = T.IDTRF
				INNER JOIN KPOSTO P ON A.CODCOLIGADA = P.CODCOLIGADA AND A.CODFILIAL = P.CODFILIAL AND A.CODPOSTO = P.CODPOSTO
				INNER JOIN GCCUSTO C ON C.CODCOLIGADA = P.CODCOLIGADA AND C.CODCCUSTO = P.CODCCUSTO
				INNER JOIN KATIVIDADE KA ON A.CODCOLIGADA = KA.CODCOLIGADA AND A.CODFILIAL = KA.CODFILIAL AND A.CODATIVIDADE = KA.CODATIVIDADE
				LEFT JOIN FLUIG.DBO.Z_CRM_ML001005 ML (NOLOCK) ON ML.CODCOLIGADA=A.CODCOLIGADA AND ML.CODFILIAL=A.CODFILIAL 
					AND ML.CODIGOPRD=A.CODESTRUTURA COLLATE SQL_Latin1_General_CP1_CI_AI AND ML.OS=O.CODCCUSTO COLLATE SQL_Latin1_General_CP1_CI_AI
				LEFT JOIN FLUIG.DBO.Z_CRM_ML001021 MLA (NOLOCK) ON MLA.IDCRIACAOPROCESSO=ML.IDCRIACAO AND MLA.CODATIVIDADE=A.CODATIVIDADE COLLATE SQL_Latin1_General_CP1_CI_AI AND MLA.OSPROCESSO=ML.OS
				INNER JOIN FLUIG.DBO.Z_CRM_EX001005 MLX (NOLOCK) ON MLX.CODCOLIGADA=A.CODCOLIGADA AND MLX.CODFILIAL=A.CODFILIAL 
					AND MLX.CODIGOPRD=A.CODESTRUTURA COLLATE SQL_Latin1_General_CP1_CI_AI AND MLX.ITEMEXCLUSIVO<>2 AND MLX.OS=O.CODCCUSTO COLLATE SQL_Latin1_General_CP1_CI_AI 
					AND MLX.EXECUCAO=(SELECT NUMEXEC FROM KORDEMCOMPL WHERE CODCOLIGADA=O.CODCOLIGADA AND CODFILIAL=O.CODFILIAL AND CODORDEM=O.CODORDEM)
				INNER JOIN FLUIG.DBO.Z_CRM_EX001021 MLXA (NOLOCK) ON MLXA.IDCRIACAOPROCESSO=MLX.IDCRIACAO AND MLXA.CODATIVIDADE=A.CODATIVIDADE COLLATE SQL_Latin1_General_CP1_CI_AI 
				AND MLXA.EXECUCAO=MLX.EXECUCAO AND MLXA.OSPROCESSO=MLX.OS
			WHERE O.CODCCUSTO = '3.07840.32.001'
			AND C.NOME LIKE '%'
			AND KA.DSCATIVIDADE LIKE '%'
			AND O.DSCORDEM LIKE '%'
			AND CL.TAG >= 'CT08'
			AND CL.TAG <= 'CT08'
			AND A.CODFILIAL = 7
			AND A.STATUS <> 6
			AND M.REVISAO = (SELECT Max(REVISAO) FROM MPRJ where codprj = '3.07840.32.001') 

		)AS PROJETADO) AS HORAS
		GROUP BY 
		CODCCUSTO,
		CCUSTOPOSTO, 
		NOMEPOSTO,
		TAG,
		DESCRICAO,
		CODCOLIGADA,
		CODFILIAL,
		CODORDEM
 ) CONTROLE_HORAS
GROUP BY 
CODCCUSTO,
CCUSTOPOSTO,
NOMEPOSTO,
DESCRICAO )

select *  from cte
