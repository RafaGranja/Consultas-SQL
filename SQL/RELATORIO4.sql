SELECT CODCCUSTO,RESPONSAVEL,MATRICULA_RESP,DSCORDEM,DTAPONTAMENTO,TAG,NOME_ATIVIDADE,CCUSTOPOSTO,NOMEPOSTO,DESCRICAO,CODORDEM,IDATVORDEM,PERCENTUAL_CONCLUIDO,convert(decimal(10,2),ROUND(SUM(HR_APONTADAS),2)) HR_APONTADAS FROM (
SELECT O.CODCCUSTO,( SELECT DISTINCT NOME FROM PPESSOA WHERE CODIGO = (SELECT DISTINCT CODPESSOA FROM KMAOOBRA WHERE CODMO=W.CODMO)) RESPONSAVEL, W.CODMO MATRICULA_RESP,O.DSCORDEM,
convert(varchar,W.DTHRINICIAL,103) DTAPONTAMENTO,
               CL.TAG,
               D.DSCATIVIDADE										 NOME_ATIVIDADE,
               C.CODCCUSTO                                           CCUSTOPOSTO,
               C.NOME                                                NOMEPOSTO,
               M.DESCRICAO,
               A.CODORDEM,
               W.IDATVORDEM,
               A.PERCENTUAL 			                             PERCENTUAL_CONCLUIDO,
             
               Cast(Datediff(SECOND, W.DTHRINICIAL, W.DTHRFINAL) AS NUMERIC(15, 4)) / 3600 HR_APONTADAS
        FROM   KATVORDEM A
               INNER JOIN KORDEM O
                     (NOLOCK)  ON A.CODCOLIGADA = O.CODCOLIGADA
                          AND A.CODFILIAL = O.CODFILIAL
                          AND A.CODORDEM = O.CODORDEM
               INNER JOIN MTAREFA T
                     (NOLOCK)  ON A.CODCOLIGADA = T.CODCOLIGADA
                          AND A.CODORDEM = T.CODTRFAUX
						  AND T.CODTRFAUX IS NOT NULL
            INNER JOIN MPRJ M
                    (NOLOCK)   ON M.CODCOLIGADA = T.CODCOLIGADA
                          AND M.IDPRJ = T.IDPRJ
               INNER JOIN MTRFCOMPL CL
                      (NOLOCK) ON CL.CODCOLIGADA = T.CODCOLIGADA
                          AND CL.IDPRJ = T.IDPRJ
                          AND CL.IDTRF = T.IDTRF
               INNER JOIN KPOSTO P
                   (NOLOCK)    ON A.CODCOLIGADA = P.CODCOLIGADA
                          AND A.CODFILIAL = P.CODFILIAL
                          AND A.CODPOSTO = P.CODPOSTO
               INNER JOIN GCCUSTO C
                       ON C.CODCOLIGADA = P.CODCOLIGADA
                          AND C.CODCCUSTO = P.CODCCUSTO
       		   INNER JOIN KATIVIDADE D
					(NOLOCK)   ON D.CODCOLIGADA = A.CODCOLIGADA
						  AND D.CODFILIAL = A.CODFILIAL
						  AND D.CODATIVIDADE = A.CODATIVIDADE
			   INNER JOIN KMAOOBRAALOC W
				(NOLOCK)	   ON A.CODCOLIGADA = W.CODCOLIGADA
						  AND A.CODFILIAL = W.CODFILIAL
						  AND A.CODORDEM = W.CODORDEM
						  AND A.CODESTRUTURA = W.CODESTRUTURA
						  AND A.CODMODELO = W.CODMODELO 
						  AND A.IDATVORDEM = W.IDATVORDEM
     WHERE  O.CODCCUSTO = :CCUSTO
               AND CL.TAG like :TAG
			    AND W.DTHRINICIAL <= :DATAFINAL
				   AND A.CODFILIAL = :FILIAL
				   AND A.CODCOLIGADA = :COLIGADA
				   AND W.EFETIVADO = 1
			AND M.REVISAO = (SELECT Max(REVISAO) FROM MPRJ where codprj = :CCUSTO)) R
				  
GROUP BY
CODCCUSTO,RESPONSAVEL,MATRICULA_RESP,DSCORDEM,DTAPONTAMENTO,TAG,NOME_ATIVIDADE,CCUSTOPOSTO,NOMEPOSTO,DESCRICAO,CODORDEM,IDATVORDEM,PERCENTUAL_CONCLUIDO
ORDER BY TAG,CODORDEM,IDATVORDEM 
				  



/*

        WHERE  O.CODCCUSTO = :CCUSTO
               AND CL.TAG like :TAG
			    AND Q.DTHRFINALPREV <= :DATAFINAL
				   AND A.CODFILIAL = :FILIAL
				   AND W.EFETIVADO = 1
			  */
		