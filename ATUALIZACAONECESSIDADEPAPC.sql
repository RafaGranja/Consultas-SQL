CREATE TABLE Z_DELP_PEDIDORELAC(
	CODCOLIGADA INT,
	CODFILIAL INT,
	OS VARCHAR(30),
	OP VARCHAR(30),
	CODATIVIDADE VARCHAR(30),
	NSEQPEDIDO INT,
	NUMPLANOCORTE VARCHAR(30),
	QTDPLANO INT,
	NSEQPLANOCORTE INT
)

SELECT CODCOLIGADA,CODFILIAL,OS,OP,CODATIVIDADE,NSEQPEDIDO,NUMPLANOCORTE,QTDPLANO,NSEQPLANOCORTE
					    FROM   (SELECT DISTINCT Z.CODCOLIGADA, 
                            Z.CODFILIAL, 
                            K.CODCCUSTO  OS, 
                            N.NUMDESENHO, 
                            N.CODIGOPRD 
                            CODESTRUTURA, 
                            N.EXECUCAO, 
                            K.CODORDEM OP, 
                            A.DSCATIVIDADE, 
                            Z.CODATIVIDADE, NUMPLANOCORTE,Z3.QUANTIDADE QTDPLANO,NSEQPLANOCORTE,
                            KA.IDATVORDEM, 
                            N.POSICAODESENHO POSICAO, 
                            N.DESCRICAO  ITEM, 
                            N.MATERIAL, 
                            ( Z.QUANTIDADE ) - ISNULL(Z3.QUANTIDADE, 0) 
                            QUANTIDADE, 
                            Z.QTDATENDIDA, 
                            KA.QTPREVISTA 
                            QTDEPLANEJADA, 
                            Z.QUANTIDADE 
                            QTDORIGINAL, 
                            Z.SEMANANECESSIDADE, 
                            Z.SEMANAREPROGRAMACAO, 
                            Z.PRIORIDADEREPROGRAMACAO, 
                            Z.STATUS STATUSNECESSIDADE, 
                            Z.RECCREATEDBY, 
                            Z.RECCREATEDON, 
                            ( KA.QTPREVISTA - Z.QUANTIDADE ) SALDOED, 
                            Z.QUANTIDADE 
                            QUANTIDADEED, 
                            N.BITOLA, 
                            CONVERT(VARCHAR(10), Z.DATANECESSIDADE, 103) 
                            DATANECESSIDADE, 
                            Z.PRIORIDADE, 
                            Z.RECMODIFIEDBY, 
                            Z.PAPC, 
                            Z.COMPLEMENTO, 
                            Z.OBS, 
                            CONVERT(VARCHAR(10), Z.DATAREPROGRAMACAO, 103) 
                                   DATAREPROGRAMACAO, 
                            ( Z.QUANTIDADE - ISNULL(Z.QTDATENDIDA, 0) )    SALDO, 
                            Z.NSEQPEDIDO, 
                            N.PESOBRUTO, 
                            N.PESOLIQUIDO,CASE WHEN K.RESPONSAVEL LIKE '%RETRABALHO%' THEN 1 ELSE 0 END AS RETRABALHO, Z.PENDENCIA
					 		FROM FLUIG.DBO.Z_DELP_NECESSIDADEPAPC Z  
					 			INNER JOIN KORDEM K ON  
						 			K.CODCOLIGADA=Z.CODCOLIGADA  
						 			AND K.CODFILIAL=Z.CODFILIAL  
						 			AND K.CODORDEM=Z.CODORDEM  
						 			INNER JOIN KORDEMCOMPL KC ON  
						 			K.CODCOLIGADA = KC.CODCOLIGADA  
						 			AND K.CODFILIAL = KC.CODFILIAL  
						 			AND K.CODORDEM = KC.CODORDEM  
						 	       INNER JOIN KATVORDEM KA ON  
				         			KA.CODCOLIGADA=Z.CODCOLIGADA  
						 			AND KA.CODFILIAL=Z.CODFILIAL  
						 		AND KA.CODORDEM=Z.CODORDEM  
						 			AND KA.CODATIVIDADE=Z.CODATIVIDADE  
						 			INNER JOIN FLUIG.DBO.Z_CRM_EX001005 N ON  
						 		N.OS = K.CODCCUSTO COLLATE LATIN1_GENERAL_CI_AS  
						 			AND N.CODIGOPRD = Z.CODESTRUTURA COLLATE LATIN1_GENERAL_CI_AS  
						 			AND N.EXECUCAO=KC.NUMEXEC  
						 			AND N.ITEMEXCLUSIVO<>2  
						 	       AND N.CODCOLIGADA=KA.CODCOLIGADA  
				         	       AND N.CODFILIAL=KA.CODFILIAL  
				         		INNER JOIN KATIVIDADE A ON  
						 			A.CODCOLIGADA=Z.CODCOLIGADA  
						 			AND A.CODATIVIDADE=Z.CODATIVIDADE  
						 	 	    AND A.CODFILIAL=Z.CODFILIAL  
                           INNER JOIN FLUIG.DBO.Z_CRM_EX001005COMPL FTAG 
                         	ON FTAG.CODCOLIGADA=K.CODCOLIGADA 
                        	AND FTAG.CODFILIAL=K.CODFILIAL 
                        	AND FTAG.OS=N.OS 
                        	AND FTAG.EXECUCAO=N.EXECUCAO 
                        	AND FTAG.IDCRIACAO=N.IDCRIACAO 
						 LEFT JOIN (  
								   SELECT SUM(A.QUANTIDADE) QUANTIDADE,A.CODCOLIGADA,A.CODFILIAL,A.CODORDEM,A.CODATIVIDADE ,A.NUMPLANOCORTE,A.NSEQPLANOCORTE
								   FROM ZMDPLANOAPROVEITAMENTOCORTE A  
								   INNER JOIN FLUIG.DBO.Z_DELP_NECESSIDADEPAPC B  
								    ON A.CODCOLIGADA = B.CODCOLIGADA  
					                         AND A.CODFILIAL = B.CODFILIAL  
					                         AND A.CODORDEM = B.CODORDEM  
					                         AND A.CODATIVIDADE = B.CODATIVIDADE  
											 AND B.LOGNECESSIDADE LIKE CONCAT('%COM O PLANO ',A.NUMPLANOCORTE,'%')  
								   GROUP BY A.CODCOLIGADA,A.CODFILIAL,A.CODORDEM,A.CODATIVIDADE,A.NUMPLANOCORTE,A.NSEQPLANOCORTE)AS  Z3  
					                      ON Z3.CODCOLIGADA = Z.CODCOLIGADA  
					                         AND Z3.CODFILIAL = Z.CODFILIAL  
					                         AND Z3.CODORDEM = Z.CODORDEM  
					                         AND Z3.CODATIVIDADE = Z.CODATIVIDADE  
    		 WHERE NUMPLANOCORTE IS NOT NULL) R  
			 ORDER BY OP,CODATIVIDADE,NUMPLANOCORTE