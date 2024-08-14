 WITH COMP AS (
 SELECT SUM(CASE WHEN IDSTATUS=6 THEN (TC.SALDOFISICO1 +  
	TC.SALDOFISICO2 +  
	TC.SALDOFISICO3 +  
	TC.SALDOFISICO4 +  
	TC.SALDOFISICO5 +  
	TC.SALDOFISICO6 +  
	TC.SALDOFISICO7 +  
	TC.SALDOFISICO8 +  
	TC.SALDOFISICO9 + 
	TC.SALDOFISICO10) ELSE 0 END) SALDOLIBERADO,
	SUM(TC.SALDOFISICO1 +  
	TC.SALDOFISICO2 +  
	TC.SALDOFISICO3 +  
	TC.SALDOFISICO4 +  
	TC.SALDOFISICO5 +  
	TC.SALDOFISICO6 +  
	TC.SALDOFISICO7 +  
	TC.SALDOFISICO8 +  
	TC.SALDOFISICO9 + 
	TC.SALDOFISICO10) SALDO,
	SUM(CASE WHEN CODLOC=23 THEN (TC.SALDOFISICO1 +  
	TC.SALDOFISICO2 +  
	TC.SALDOFISICO3 +  
	TC.SALDOFISICO4 +  
	TC.SALDOFISICO5 +  
	TC.SALDOFISICO6 +  
	TC.SALDOFISICO7 +  
	TC.SALDOFISICO8 +  
	TC.SALDOFISICO9 + 
	TC.SALDOFISICO10) ELSE 0 END) SALDO23,
	SUM(CASE WHEN CODLOC=23 AND IDSTATUS=6  THEN (TC.SALDOFISICO1 +  
	TC.SALDOFISICO2 +  
	TC.SALDOFISICO3 +  
	TC.SALDOFISICO4 +  
	TC.SALDOFISICO5 +  
	TC.SALDOFISICO6 +  
	TC.SALDOFISICO7 +  
	TC.SALDOFISICO8 +  
	TC.SALDOFISICO9 + 
	TC.SALDOFISICO10) ELSE 0 END) SALDO23LIBERADO
	,TC.IDPRD,TC.CODFILIAL,TC.CODCOLIGADA FROM TLOTEPRDLOC (NOLOCK) TC 
	INNER JOIN TLOTEPRD (NOLOCK) TL ON 
	TL.CODCOLIGADA=TC.CODCOLIGADA 
	AND TL.IDLOTE=TC.IDLOTE 
	AND TL.IDPRD=TC.IDPRD  
	GROUP BY TC.IDPRD,TC.CODFILIAL,TC.CODCOLIGADA 
 ), RA AS (
 SELECT MIN(T.NUMEROMOV) NUMEROMOV,T.CODCOLIGADA,T.CODFILIAL,T.CAMPOLIVRE1,T.CAMPOLIVRE2,T.CAMPOLIVRE3 
FROM TMOV T (NOLOCK) INNER JOIN TITMMOV TI ON TI.CODCOLIGADA=T.CODCOLIGADA AND TI.IDMOV=T.IDMOV
WHERE T.CODTMV='1.1.05' AND T.STATUS='A' 
GROUP BY T.CODCOLIGADA,T.CODFILIAL,T.CAMPOLIVRE1,T.CAMPOLIVRE2,T.CAMPOLIVRE3 
				    
				    ),CTE AS ( 
				     	SELECT EXECUCAO,CODCOLIGADA,CODFILIAL,INDICE,STATUS_ATV,STATUS_OP,CODESTRUTURA,CODORDEM,IDATVORDEM,CODATIVIDADE,SALDOPAPC,PAPC,SUBSTITUTO IDPRODUTO,GRUPO,MASKPRD,SUM(PREVISTO) PREVISTO,SUM(APONTADO) APONTADO,NOMEFANTASIA FROM ( 
				     		SELECT EXECUCAO,CODCOLIGADA,CODFILIAL,INDICE,STATUS_ATV,STATUS_OP,CODESTRUTURA,CODORDEM,IDATVORDEM,CODATIVIDADE,SALDOPAPC,PAPC,GRUPO,MASKPRD,PREVISTO,APONTADO,NOMEFANTASIA
				     		,CASE WHEN PREVISTO = 0 THEN 'SUBSTITUTO' ELSE 'PRINCIPAL' END AS POSICAO, 
				     		CASE 
								WHEN PREVISTO = 0 THEN  ISNULL( 
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
											INNER JOIN TPRODUTO T ON 
											T.NOMEFANTASIA=R2.NOMEFANTASIA 
											AND INATIVO = 0 
											AND LEFT(CODIGOPRD, 6) = '03.023' 
											AND CODCOLPRD = R2.CODCOLIGADA 
										WHERE 
											CODCOLIGADA = R2.CODCOLIGADA 
											AND CODORDEM = R2.CODORDEM 
											AND IDATIVIDADE = R2.IDATVORDEM 
											AND EFETIVADO = 0 
									)) 
								ELSE R2.IDPRODUTO 
							END AS SUBSTITUTO 
				     		 FROM ( 
				     				SELECT 
										EX1.EXECUCAO, 
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
										SUM(CASE 
											WHEN KMA.EFETIVADO = 0 THEN KMA.QUANTIDADE 
											ELSE 0 
										END) AS PREVISTO, 
										SUM(CASE 
											WHEN KMA.EFETIVADO = 1 THEN KMA.QUANTIDADE 
											ELSE 0 
										END) AS APONTADO, 
										PP.CODTB2FAT GRUPO, 
										LEFT(P.CODIGOPRD, 6) MASKPRD, 
										P.CODIGOPRD, 
										P.NOMEFANTASIA, 
										KA.STATUS STATUS_ATV, 
										K.STATUS STATUS_OP 
									FROM 
										FLUIG.DBO.Z_CRM_EX001005 (NOLOCK) EX1 
										INNER JOIN KITEMORDEM (NOLOCK) KI ON KI.CODCOLIGADA = EX1.CODCOLIGADA 
										AND KI.CODFILIAL = EX1.CODFILIAL 
										AND KI.CODCCUSTO = EX1.OS COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI 
										AND KI.CODESTRUTURA = EX1.CODIGOPRD COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI 
										INNER JOIN KORDEMCOMPL (NOLOCK) KL ON 
										KL.CODCOLIGADA = KI.CODCOLIGADA 
										AND KL.CODFILIAL = KI.CODFILIAL 
										AND KL.CODORDEM = KI.CODORDEM 
										AND KL.NUMEXEC = EX1.EXECUCAO 
										INNER JOIN KORDEM (NOLOCK) K ON 
										K.CODCOLIGADA = KI.CODCOLIGADA 
										AND K.CODFILIAL = KI.CODFILIAL 
										AND K.CODORDEM = KI.CODORDEM 
										INNER JOIN KATVORDEM (NOLOCK) KA ON
										KA.CODCOLIGADA = KI.CODCOLIGADA 
										AND KA.CODFILIAL = KI.CODFILIAL 
										AND KA.CODORDEM = KI.CODORDEM 
										AND KA.CODESTRUTURA=KI.CODESTRUTURA 
										AND KA.CODMODELO=KI.CODMODELO 
										INNER JOIN KATVORDEMMP (NOLOCK) KMA ON 
										KMA.CODCOLIGADA = KA.CODCOLIGADA 
										AND KMA.CODFILIAL = KA.CODFILIAL 
										AND KMA.CODORDEM = KA.CODORDEM 
										AND KMA.CODESTRUTURA=KI.CODESTRUTURA 
										AND KMA.CODMODELO=KI.CODMODELO 
										AND KMA.IDATIVIDADE = KA.IDATVORDEM 
										INNER JOIN TPRODUTO (NOLOCK) P ON 
										P.CODCOLPRD=KI.CODCOLIGADA
										AND KMA.IDPRODUTO = P.IDPRD 
										INNER JOIN TPRODUTODEF (NOLOCK) PP ON 
										PP.CODCOLIGADA=P.CODCOLPRD
										AND P.IDPRD = PP.IDPRD 
										LEFT JOIN ZMDPLANOAPROVEITAMENTOCORTE (NOLOCK) PAPC ON PAPC.CODCOLIGADA = KA.CODCOLIGADA 
										AND PAPC.CODFILIAL = KA.CODFILIAL 
										AND PAPC.CODORDEM = KA.CODORDEM 
										AND PAPC.CODATIVIDADE = KA.CODATIVIDADE 
				     				WHERE EX1.ITEMEXCLUSIVO<>2  
				     				 AND K.CODCOLIGADA=1 AND K.CODFILIAL=7 AND K.CODORDEM='26739/21' AND K.STATUS NOT IN (5,6) AND KA.STATUS NOT IN (6) AND ISNULL(K.REPROCESSAMENTO,0)=0 
				     				AND ( CODTB2FAT != 0184 OR CODTB2FAT IS NULL ) AND LEFT(P.CODIGOPRD, 6) NOT IN ('01.053','01.019','01.020') AND  
				     				( (CASE 
											WHEN PAPC.NUMPLANOCORTE IS NULL THEN '0' 
											ELSE PAPC.NUMPLANOCORTE 
										END)='0' OR (CASE 
											WHEN PAPC.NUMPLANOCORTE IS NULL THEN 0 
											ELSE CASE 
												WHEN PAPC.QTDEAPONTADA IS NULL THEN CASE 
													WHEN PAPC.QUANTIDADE - KA.QUANTIDADE < 0 THEN 0 
													ELSE PAPC.QUANTIDADE - KA.QUANTIDADE 
												END 
												ELSE PAPC.QUANTIDADE - PAPC.QTDEAPONTADA 
											END 
										END)>0) 
				     				GROUP BY EX1.EXECUCAO, 
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
										END, 
										CASE 
											WHEN PAPC.NUMPLANOCORTE IS NULL THEN '0' 
											ELSE PAPC.NUMPLANOCORTE 
										END, 
										KMA.IDPRODUTO, 
										PP.CODTB2FAT, 
										LEFT(P.CODIGOPRD, 6), 
										P.CODIGOPRD, 
										P.NOMEFANTASIA, 
										KA.STATUS, 
										K.STATUS 
				     			) R2  
				     		) R3 GROUP BY EXECUCAO,CODCOLIGADA,CODFILIAL,INDICE,CODESTRUTURA,CODORDEM,IDATVORDEM,CODATIVIDADE,SALDOPAPC,PAPC,SUBSTITUTO,GRUPO,MASKPRD,STATUS_ATV,STATUS_OP,NOMEFANTASIA ) 
				     , CTE2 AS ( 

					 SELECT CODCOLIGADA,CODFILIAL,INDICE,CODESTRUTURA,CODORDEM,IDATVORDEM,CODATIVIDADE,SALDOPAPC,PAPC,IDPRODUTO,NOMEFANTASIA,GRUPO,MASKPRD,PREVISTO,APONTADO,STATUS_ATV,STATUS_OP,CODIGOPRD, 
				     				CASE WHEN GRUPO='0184'  
				     					THEN  
				     						CASE WHEN SALDO23 > 0 THEN 1 ELSE 0 END 
				     					ELSE 
				     						CASE WHEN SALDO23 >= R.PREVISTO THEN 1 ELSE 0 END 
				     				END AS SALDO23, 
				     				CASE WHEN GRUPO='0184'  
				     					THEN  
				     						CASE WHEN SALDO23LIBERADO > 0 THEN 1 ELSE 0 END 
				     					ELSE 
				     						CASE WHEN SALDO23LIBERADO >= R.PREVISTO THEN 1 ELSE 0 END  
				     				END AS SALDO23LIBERADO, 
				     				CASE WHEN GRUPO='0184'  
				     					THEN  
				     						CASE WHEN SALDOTOTAL > 0 THEN 1 ELSE 0 END  
				     					ELSE 
				     						CASE WHEN SALDOTOTAL >= R.PREVISTO THEN 1 ELSE 0 END  
				     				END AS SALDOTOTAL, 
				     				CASE WHEN GRUPO='0184'  
				     					THEN  
				     						CASE WHEN SALDOTOTALLIBERADO > 0 THEN 1 ELSE 0 END 
				     					ELSE 
				     						CASE WHEN SALDOTOTALLIBERADO >= R.PREVISTO THEN 1 ELSE 0 END 
				     				END AS SALDOTOTALLIBERADO,RAABERTA,RAPAPC FROM( 

				     				 SELECT C.CODCOLIGADA,C.CODFILIAL,INDICE,CODESTRUTURA,CODORDEM,IDATVORDEM,CODATIVIDADE,SALDOPAPC,PAPC,IDPRODUTO,T.NOMEFANTASIA,GRUPO,MASKPRD,PREVISTO,APONTADO,STATUS_ATV,STATUS_OP,
									 SUM(ISNULL(CP.SALDO23,0)) SALDO23, 
				     				SUM(ISNULL(CP.SALDO23LIBERADO,0)) SALDO23LIBERADO , 
				     			ISNULL(RA.NUMEROMOV,'0') RAABERTA,
								ISNULL((CASE WHEN RA.CAMPOLIVRE3=C.PAPC THEN RA.NUMEROMOV ELSE '0' END) ,'0') RAPAPC, 
				     			SUM(ISNULL(CP.SALDO,0)) SALDOTOTAL, 
				     			SUM(ISNULL(CP.SALDOLIBERADO,0)) SALDOTOTALLIBERADO,
								T.CODIGOPRD 
								FROM CTE C 
								INNER JOIN TPRODUTO T ON
								T.CODCOLPRD=C.CODCOLIGADA
								AND T.IDPRD=C.IDPRODUTO
								AND INATIVO=0
								LEFT JOIN 
								COMP CP ON 
								CP.CODFILIAL=C.CODFILIAL 
								AND CP.CODCOLIGADA=C.CODCOLIGADA
								AND CP.IDPRD IN (
									SELECT DISTINCT IDPRD FROM TPRODUTO WHERE CODCOLPRD=C.CODCOLIGADA AND NOMEFANTASIA = C.NOMEFANTASIA AND INATIVO=0 AND LEFT(CODIGOPRD,6)='03.023'
									UNION ALL 
									SELECT DISTINCT IDPRD FROM KCOMPSUBSTITUTO (NOLOCK) WHERE IDPRDORIGEM=C.IDPRODUTO AND CODESTRUTURA=C.CODESTRUTURA AND CODATIVIDADE=C.CODATIVIDADE
									UNION ALL
									SELECT C.IDPRODUTO
									)
								LEFT JOIN RA ON
								RA.CODCOLIGADA=C.CODCOLIGADA
								AND RA.CODFILIAL=C.CODFILIAL  
				     			AND ((RA.CAMPOLIVRE1=C.CODORDEM AND RA.CAMPOLIVRE2=C.IDATVORDEM) OR RA.CAMPOLIVRE3=C.PAPC )


							
				     							WHERE ((PREVISTO - APONTADO) > 0 AND (GRUPO!='0184' OR GRUPO IS NULL))  OR (APONTADO <= 0 AND GRUPO='0184') 
												GROUP BY C.CODCOLIGADA,C.CODFILIAL,INDICE,CODESTRUTURA,CODORDEM,IDATVORDEM,CODATIVIDADE,SALDOPAPC,PAPC,IDPRODUTO,T.NOMEFANTASIA,GRUPO,MASKPRD,PREVISTO,APONTADO,STATUS_ATV,STATUS_OP,
												RA.NUMEROMOV,RA.CAMPOLIVRE3,T.CODIGOPRD 
												) R ) 




				     SELECT C.INDICE,C.CODORDEM,KS2.DSCSTATUS STATUS_OP,C.IDATVORDEM,KA.DSCATIVIDADE,KS.DSCSTATUS STATUS_ATV,C.CODIGOPRD,C.NOMEFANTASIA NOMEFANTASIA,CASE WHEN C.GRUPO='0184'  
				     			THEN  
				     				CASE WHEN C.APONTADO > 0  
				     					THEN  
				     						'OK' 
				     					ELSE  
				     						CASE WHEN C.SALDO23 = 1  
				     							THEN  
				     								CASE WHEN C.SALDO23LIBERADO = 1   
				     									THEN  
				     									CONCAT('Apontar Componente ',C.CODIGOPRD,' ou substituto ') 
				     									ELSE  
				     									'Liberar lotes do local 23 para apontamento do componente principal ou substituto  ' 
				     								END 
				     							ELSE  
				     								CASE WHEN C.RAABERTA != '0'  
				     									THEN  
				     									CONCAT('Atender RA ',C.RAABERTA) 
				     									ELSE  
				     										CASE WHEN C.SALDOTOTAL = 1  
				     											THEN  
				     												CASE WHEN C.SALDOTOTALLIBERADO = 1  
				     													THEN  
				     													'Gerar RA para componente principal ou substituto ' 
				     													ELSE  
				     													'Liberar estoque fora do local 23 para posterior geração de RA ' 
				     												END 
				     											ELSE 
				     											'Pendente entrada de nota, ou chegada do material ' 
				     										END 
				     								END 
				     						END 
				     				END 
				     			ELSE  
				     				CASE WHEN C.PAPC !='0' 
				     					THEN 
				     						CASE WHEN C.SALDOPAPC > 0 
				     							THEN  
				    								CASE WHEN C.RAABERTA = '0' AND C.RAPAPC = '0' 
     													THEN  
    				                                    CONCAT('Gerar RA do plano ',C.PAPC ) 
    													ELSE 
    														CASE WHEN C.RAABERTA != '0' AND  C.RAPAPC != '0'  
    															THEN +
    															 	CONCAT('Atender RA ',C.RAPAPC ) 
    				                                           ELSE 
    																CASE WHEN C.SALDO23 = 1  
				     														THEN  
				     															CASE WHEN C.SALDO23LIBERADO = 1   
				     																THEN   
				     																CONCAT('Apontar plano ',C.PAPC,' para completar baixa do material cadastrado') 
				     																ELSE  
				     																'Liberar lote do local 23 para apontamento do plano ' 
				     															END 
				     														ELSE  
				     															CASE WHEN C.SALDOTOTAL = 1  
				    																THEN  
				    																	CASE WHEN C.SALDOTOTALLIBERADO = 1  
				    																		THEN  
				    																			CONCAT('Houve erro na traneferência de material, consulte a RA ',C.RAPAPC) 
				    																		ELSE  
				    																			'Entrar em contato com o suporte' 
				    																	END 
				    																ELSE 
				    																'Pendente entrada de nota, ou chegada do material ' 
				     															END 
				     													END 
    				                                        END 
    													END  
				     							ELSE 
				     							'OK' 
				     						END 
				     					ELSE  
				     						CASE WHEN C.MASKPRD IN ('01.053','01.019','01.020')  
				     							THEN  
				     							'OK' 
				     							ELSE 
				     								CASE WHEN C.APONTADO >= C.PREVISTO 
				     									THEN  
				     										'OK' 
				     									ELSE  
				     										CASE WHEN C.MASKPRD IN ('03.023','04.024')  
				     											THEN  
				     												CASE WHEN C.SALDOTOTAL = 1  
				     													THEN  
				     													CONCAT('Apontar Componente ',C.CODIGOPRD,' ou substituto') 
				     													ELSE 
				     													CONCAT('Subir saldo do produto ',C.CODIGOPRD ) 
				      
				     												END 
				     											ELSE  
				     													CASE WHEN C.SALDO23 = 1  
				     														THEN  
				     															CASE WHEN C.SALDO23LIBERADO = 1   
				     																THEN   
				     																CONCAT('Apontar Componente ',C.CODIGOPRD,' ou substituto') 
				     																ELSE  
				     																'Liberar lotes do local 23 para apontamento do componente principal ou substituto ' 
				     															END 
				     														ELSE  
				     															CASE WHEN C.RAABERTA != '0'  
				     																THEN  
				     																CONCAT('Atender RA ',C.RAABERTA ) 
				     																ELSE  
				     																	CASE WHEN C.SALDOTOTAL = 1  
				     																		THEN  
				     																			CASE WHEN C.SALDOTOTALLIBERADO = 1  
				     																				THEN  
				     																				'Gerar RA ' 
				     																				ELSE  
				     																				'Liberar estoque fora do local 23 para posterior geração de RA ' 
				     																			END 
				     																		ELSE 
				     																		'Pendente entrada de nota, ou chegada do material ' 
				     																	END 
				     															END 
				     													END 
				     											END 
				     								END 
				     						END 
				     				END 
				     		END AS ACAO,C.PAPC NUMPLANOCORTE, CONCAT(CODIGOPRD , ' - ' , NOMEFANTASIA) COMPONENTE 
				     FROM CTE2 C  
				     INNER JOIN KSTATUS (NOLOCK) KS ON 
				     KS.CODCOLIGADA=C.CODCOLIGADA 
				     AND KS.CODSTATUS=C.STATUS_ATV 
				     INNER JOIN KSTATUS (NOLOCK) KS2 ON 
				     KS2.CODCOLIGADA=C.CODCOLIGADA 
				     AND KS2.CODSTATUS=C.STATUS_OP 
				     INNER JOIN KATIVIDADE (NOLOCK) KA ON 
				     KA.CODCOLIGADA=C.CODCOLIGADA 
				     AND KA.CODFILIAL=C.CODFILIAL 
				     AND KA.CODATIVIDADE=C.CODATIVIDADE 
				     WHERE C.INDICE IS NOT NULL 
					 ORDER BY CODORDEM,IDATVORDEM