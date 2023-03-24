declare @os varchar(100)
declare @exec int 

set @os = '3.07842.32.001'
set @exec = 3

;with CTE_rec  as (
SELECT DISTINCT R2.CODCCUSTO,
               R2.CODESTRUTURA,
               R2.CODORDEM,
               R2.DSCORDEM,
			   KEL.CAMDESENHO,
			   KEL.POSICAO,
			   KEL.BITOLA,
			   KEL.LARGURA,
			   KEL.COMPRIMENTO,
			   KEL.PESOBRUTO,
			   KEL.PESOLIQUIDO,
			   --R2.STATUS,
               R2.IDATVORDEM,
			   R2.CODATIVIDADE,
               ATV.DSCATIVIDADE,
               CODIGOPRD,
               CASE
                 WHEN K.CODORDEM IS NULL THEN 'SEM LOTE'
                 ELSE K.CODORDEM
               END                        AS LOTE_PREVISTO,
               CASE
                 WHEN TL.NUMLOTE IS NULL THEN 'SEM LOTE'
                 ELSE TL.NUMLOTE
               END                        AS LOTE,
               CODUNDCONTROLE,
               NOMEFANTASIA,
               CASE
                 WHEN NUMPLANOCORTE IS NULL THEN 'SEM PLANO'
                 ELSE NUMPLANOCORTE
               END                        AS PLANOCORTE,
               POSICAO_COMP,NUMEXEC
			   
        FROM   (SELECT DISTINCT KM1.IDATVORDEMMATERIAPRIMA,
                                KA.CODESTRUTURA,
                                K.CODCOLIGADA,
                                K.CODFILIAL,
                                K.CODCCUSTO,
                                K.CODORDEM,
                                K.DSCORDEM,
								K.STATUS,
                                KAL.IDATVORDEM,
                                KA.CODATIVIDADE,
                                T.CODIGOPRD,
                                L.IDLOTE       IDLOTE,
                                TP.CODUNDCONTROLE,
                                T.NOMEFANTASIA,
                                PAPC.NUMPLANOCORTE,
                                KA.STATUS      STATUS_ATIVIDADE,
                                K.STATUS       STATUS_OP,
                                KM1.IDPRODUTO,
                                KM1.QUANTIDADE QUANTIDADE_PREVISTA,
                                KM2.QUANTIDADE QUANTIDADE_EFETIVADA,
                                'PRINCIPAL'    POSICAO_COMP
                FROM   KORDEM K
                       LEFT JOIN KATVORDEM KA
                              ON KA.CODCOLIGADA = K.CODCOLIGADA
                                 AND KA.CODFILIAL = K.CODFILIAL
                                 AND KA.CODORDEM = K.CODORDEM
                       LEFT JOIN KORDEMCOMPL KOL
                              ON KOL.CODCOLIGADA = K.CODCOLIGADA
                                 AND KOL.CODFILIAL = K.CODFILIAL
                                 AND KOL.CODORDEM = K.CODORDEM
                       INNER JOIN KATVORDEMMP KM1
                               ON KM1.CODCOLIGADA = K.CODCOLIGADA
                                  AND KM1.CODFILIAL = K.CODFILIAL
                                  AND KM1.CODORDEM = K.CODORDEM
                                  AND KM1.IDATIVIDADE = KA.IDATVORDEM
                                  AND KM1.EFETIVADO = 0
                       LEFT JOIN KATVORDEMMP KM2
                              ON KM2.CODCOLIGADA = K.CODCOLIGADA
                                 AND KM2.CODFILIAL = K.CODFILIAL
                                 AND KM2.CODORDEM = K.CODORDEM
                                 AND KM2.IDATIVIDADE = KA.IDATVORDEM
                                 AND KM2.IDPRODUTO = KM1.IDPRODUTO
                                 AND KM2.EFETIVADO = 1
                       LEFT JOIN KATVORDEMMPLOTE L
                              ON L.CODCOLIGADA = K.CODCOLIGADA
                                 AND L.CODFILIAL = K.CODFILIAL
                                 AND L.IDATVORDEMMATERIAPRIMA = KM2.IDATVORDEMMATERIAPRIMA
                       LEFT JOIN KATVORDEMCOMPL KAL
                              ON KAL.CODCOLIGADA = K.CODCOLIGADA
                                 AND KAL.CODFILIAL = K.CODFILIAL
                                 AND KAL.CODORDEM = K.CODORDEM
                                 AND KAL.IDATVORDEM = KA.IDATVORDEM
                       LEFT JOIN TPRODUTO T
                              ON T.IDPRD = KM1.IDPRODUTO
                       LEFT JOIN TPRD TP
                              ON TP.IDPRD = T.IDPRD
                       LEFT JOIN ZMDPLANOAPROVEITAMENTOCORTE PAPC
                              ON PAPC.CODCOLIGADA = K.CODCOLIGADA
                                 AND PAPC.CODFILIAL = K.CODFILIAL
                                 AND PAPC.CODORDEM = K.CODORDEM
                                 AND PAPC.IDATVORDEM = KM1.IDATIVIDADE
                                 AND PAPC.CODIGOMP = T.CODIGOPRD
                WHERE  K.CODCCUSTO = @os
                       AND KOL.NUMEXEC = @exec
                       AND KA.STATUS != 6
                       AND K.STATUS = 5

                UNION ALL
                SELECT IDATVORDEMMATERIAPRIMA,
                       CODESTRUTURA,
                       CODCOLIGADA,
                       CODFILIAL,
                       CODCCUSTO,
                       CODORDEM,
                       DSCORDEM,
					   STATUS,
                       IDATVORDEM,
                       CODATIVIDADE,
                       CODIGOPRD,
                       IDLOTE,
                       CODUNDCONTROLE,
                       NOMEFANTASIA,
                       NUMPLANOCORTE,
                       STATUS_ATIVIDADE,
                       STATUS_OP,
                       IDPRODUTO,
                       QUANTIDADE_PREVISTA,
                       QUANTIDADE_EFETIVADA,
                       POSIÇÃO
                FROM   (SELECT DISTINCT KM2.IDATVORDEMMATERIAPRIMA,
                                        KA.CODESTRUTURA,
                                        K.CODCOLIGADA,
                                        K.CODFILIAL,
                                        K.CODCCUSTO,
                                        K.CODORDEM,
                                        K.DSCORDEM,
										K.STATUS,
                                        KAL.IDATVORDEM,
                                        KA.CODATIVIDADE,
                                        T.CODIGOPRD,
                                        L.IDLOTE,
                                        TP.CODUNDCONTROLE,
                                        T.NOMEFANTASIA,
                                        PAPC.NUMPLANOCORTE,
                                        KA.STATUS                                   STATUS_ATIVIDADE,
                                        K.STATUS                                    STATUS_OP,
                                        KM2.IDPRODUTO,
                                        KM3.QUANTIDADE                              QUANTIDADE_PREVISTA,
                                        KM2.QUANTIDADE                              QUANTIDADE_EFETIVADA,
                                        CONCAT('SUBSTITUTO DO ', KCOMP.IDPRDORIGEM) POSIÇÃO
                        FROM   KORDEM K
                               LEFT JOIN KATVORDEM KA
                                      ON KA.CODCOLIGADA = K.CODCOLIGADA
                                         AND KA.CODFILIAL = K.CODFILIAL
                                         AND KA.CODORDEM = K.CODORDEM
                               LEFT JOIN KORDEMCOMPL KOL
                                      ON KOL.CODCOLIGADA = K.CODCOLIGADA
                                         AND KOL.CODFILIAL = K.CODFILIAL
                                         AND KOL.CODORDEM = K.CODORDEM
                               LEFT JOIN KATVORDEMMP KM1
                                      ON KM1.CODCOLIGADA = K.CODCOLIGADA
                                         AND KM1.CODFILIAL = K.CODFILIAL
                                         AND KM1.CODORDEM = K.CODORDEM
                                         AND KM1.IDATIVIDADE = KA.IDATVORDEM
                                         AND KM1.EFETIVADO = 0
                               INNER JOIN KATVORDEMMP KM2
                                       ON KM2.CODCOLIGADA = K.CODCOLIGADA
                                          AND KM2.CODFILIAL = K.CODFILIAL
                                          AND KM2.CODORDEM = K.CODORDEM
                                          AND KM2.IDATIVIDADE = KA.IDATVORDEM
                                          AND KM2.IDPRODUTO != KM1.IDPRODUTO
                                          AND KM2.EFETIVADO = 1
                               LEFT JOIN KATVORDEMMPLOTE L
                                      ON L.CODCOLIGADA = K.CODCOLIGADA
                                         AND L.CODFILIAL = K.CODFILIAL
                                         AND L.IDATVORDEMMATERIAPRIMA = KM2.IDATVORDEMMATERIAPRIMA
                               LEFT JOIN KATVORDEMMP KM3
                                      ON KM3.CODCOLIGADA = K.CODCOLIGADA
                                         AND KM3.CODFILIAL = K.CODFILIAL
                                         AND KM3.CODORDEM = K.CODORDEM
                                         AND KM3.IDATIVIDADE = KM2.IDATIVIDADE
                                         AND KM3.IDPRODUTO = KM2.IDPRODUTO
                                         AND KM3.EFETIVADO = 0
                               INNER JOIN KATVORDEMMPLOTE KML
                                       ON KML.CODCOLIGADA = KM1.CODCOLIGADA
                                          AND KML.CODFILIAL = KM1.CODFILIAL
                               LEFT JOIN KATVORDEMCOMPL KAL
                                      ON KAL.CODCOLIGADA = K.CODCOLIGADA
                                         AND KAL.CODFILIAL = K.CODFILIAL
                                         AND KAL.CODORDEM = K.CODORDEM
                                         AND KAL.IDATVORDEM = KA.IDATVORDEM
                               LEFT JOIN TPRODUTO T
                                      ON T.IDPRD = KM2.IDPRODUTO
                               LEFT JOIN TPRD TP
                                      ON TP.IDPRD = T.IDPRD
                               LEFT JOIN ZMDPLANOAPROVEITAMENTOCORTE PAPC
                                      ON PAPC.CODCOLIGADA = K.CODCOLIGADA
                                         AND PAPC.CODFILIAL = K.CODFILIAL
                                         AND PAPC.CODORDEM = K.CODORDEM
                                         AND PAPC.IDATVORDEM = KM1.IDATIVIDADE
                                         AND PAPC.CODIGOMP = T.CODIGOPRD
                               LEFT JOIN KCOMPSUBSTITUTO KCOMP
                                      ON KCOMP.CODCOLIGADA = K.CODCOLIGADA
                                         AND KCOMP.CODFILIAL = K.CODFILIAL
                                         AND KCOMP.CODESTRUTURA = KA.CODESTRUTURA
                                         AND KCOMP.IDPRD = KM2.IDPRODUTO
                                         AND KCOMP.CODATIVIDADE = KA.CODATIVIDADE
                        WHERE  K.CODCCUSTO = @os
                               AND KOL.NUMEXEC = @exec
                               AND KM2.IDPRODUTO = (SELECT IDPRD
                                                    FROM   TLOTEPRD
                                                    WHERE  IDLOTE = KML.IDLOTE)
                               AND KM1.IDPRODUTO != (SELECT IDPRD
                                                     FROM   TLOTEPRD
                                                     WHERE  IDLOTE = KML.IDLOTE)
                               AND KA.STATUS != 6
                               AND K.STATUS = 5
                               AND KM3.QUANTIDADE IS NULL) R) R2
			   LEFT JOIN KESTRUTURACOMPL KEL 
					  ON KEL.CODESTRUTURA=R2.CODESTRUTURA
					  AND KEL.CODCOLIGADA=R2.CODCOLIGADA
               LEFT JOIN TLOTEPRD TL
                      ON TL.CODCOLIGADA = R2.CODCOLIGADA
                         AND TL.IDLOTE = R2.IDLOTE
               LEFT JOIN KSTATUS KS1
                      ON KS1.CODCOLIGADA = R2.CODCOLIGADA
                         AND KS1.CODSTATUS = R2.STATUS_ATIVIDADE
               LEFT JOIN KSTATUS KS2
                      ON KS2.CODCOLIGADA = R2.CODCOLIGADA
                         AND KS2.CODSTATUS = R2.STATUS_OP
               LEFT JOIN KATIVIDADE ATV
                      ON ATV.CODCOLIGADA = R2.CODCOLIGADA
                         AND ATV.CODFILIAL = R2.CODFILIAL
                         AND ATV.CODATIVIDADE = R2.CODATIVIDADE
			   LEFT JOIN KORDEM K 
			   		  ON K.CODCOLIGADA=R2.CODCOLIGADA
						 AND K.CODFILIAL=R2.CODFILIAL
						 AND SUBSTRING(K.RESPONSAVEL,0,15)=R2.CODIGOPRD
						 AND K.CODORDEM IN (SELECT CODORDEM FROM KORDEMCOMPL WHERE NUMEXEC=1)
						 AND K.STATUS!=6
			   LEFT JOIN KORDEMCOMPL KL 
			          ON KL.CODCOLIGADA=K.CODCOLIGADA
				     	AND KL.CODFILIAL=K.CODFILIAL
			            AND KL.CODORDEM=K.CODORDEM
		WHERE ( KL.NUMEXEC IS NOT NULL OR SUBSTRING(CODIGOPRD,0,7)!='03.023' ) 
		AND R2.STATUS=5
				  )

select C.CODCCUSTO,	
C.CODESTRUTURA,
C.CODORDEM,
C.DSCORDEM,
C.CAMDESENHO,
C.POSICAO,
C.BITOLA,
C.LARGURA,
C.COMPRIMENTO,
C.PESOBRUTO,
C.PESOLIQUIDO,
C.IDATVORDEM,
C.DSCATIVIDADE,
C.CODIGOPRD,
CASE WHEN LOTE IS NULL AND PLANOCORTE IS NOT NULL THEN (SELECT DISTINCT NUMLOTE FROM ZMDPLANOAPROVEITAMENTOCORTE WHERE NUMPLANOCORTE=NUMPLANOCORTE) ELSE LOTE END AS LOTE,
C.CODUNDCONTROLE,
C.NOMEFANTASIA,
C.PLANOCORTE
from CTE_rec C
WHERE (LOTE!= 'SEM LOTE' AND PLANOCORTE!='SEM PLANO' AND C.CODIGOPRD LIKE '01.018%') OR (CONCAT(CODORDEM,' - ',CODATIVIDADE) NOT IN (SELECT CONCAT(CODORDEM,' - ',CODATIVIDADE) FROM ZMDPLANOAPROVEITAMENTOCORTE))
ORDER BY CAMDESENHO,CODESTRUTURA,CODORDEM,IDATVORDEM






