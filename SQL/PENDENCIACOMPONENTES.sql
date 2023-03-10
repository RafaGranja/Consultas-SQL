WITH CTE AS (
	SELECT EXECUCAO,CODCOLIGADA,CODFILIAL,INDICE,CODESTRUTURA,CODORDEM,IDATVORDEM,CODATIVIDADE,SALDOPAPC,PAPC,SUBSTITUTO IDPRODUTO,GRUPO,MASKPRD,CODIGOPRD,SUM(PREVISTO) PREVISTO,SUM(APONTADO) APONTADO FROM (
		SELECT *
		,CASE WHEN PREVISTO = 0 THEN 'SUBSTITUTO' ELSE 'PRINCIPAL' END AS POSICAO,
		CASE WHEN PREVISTO = 0 THEN (SELECT IDPRDORIGEM FROM KCOMPSUBSTITUTO WHERE IDPRD=IDPRODUTO AND CODESTRUTURA=R2.CODESTRUTURA AND CODATIVIDADE=R2.CODATIVIDADE) ELSE R2.IDPRODUTO END AS SUBSTITUTO
		 FROM (
			SELECT EXECUCAO,CODCOLIGADA,CODFILIAL,INDICE,CODESTRUTURA,CODORDEM,IDATVORDEM,CODATIVIDADE,SALDOPAPC,PAPC,IDPRODUTO,SUM(PREVISTO) PREVISTO,SUM(APONTADO) APONTADO,CODTB2FAT GRUPO,MASKPRD,CODIGOPRD FROM (
				SELECT EX1.EXECUCAO,KA.CODCOLIGADA,KA.CODFILIAL,EX1.INDICE,KA.CODESTRUTURA,K.CODORDEM,KA.IDATVORDEM,KA.CODATIVIDADE,CASE WHEN PAPC.NUMPLANOCORTE IS NULL 
					THEN 0 
					ELSE 
						CASE WHEN PAPC.QTDEAPONTADA IS NULL 
							THEN  CASE WHEN PAPC.QUANTIDADE-KA.QUANTIDADE < 0  
										THEN 0
										ELSE PAPC.QUANTIDADE-KA.QUANTIDADE
										END
							ELSE PAPC.QUANTIDADE-PAPC.QTDEAPONTADA
							END			
					END AS SALDOPAPC,
				CASE WHEN PAPC.NUMPLANOCORTE IS NULL THEN '0' ELSE PAPC.NUMPLANOCORTE END AS PAPC,
				KMA.IDPRODUTO,
				CASE WHEN KMA.EFETIVADO=0 THEN KMA.QUANTIDADE ELSE 0 END AS PREVISTO,
				CASE WHEN KMA.EFETIVADO=1 THEN KMA.QUANTIDADE ELSE 0 END AS APONTADO,
				(SELECT CODTB2FAT FROM TPRODUTODEF WHERE CODCOLIGADA=KA.CODCOLIGADA AND IDPRD=KMA.IDPRODUTO) CODTB2FAT,
				LEFT(P.CODIGOPRD,6) MASKPRD,P.CODIGOPRD
				FROM FLUIG.DBO.Z_CRM_EX001005 (NOLOCK) EX1
					INNER JOIN KORDEM (NOLOCK) K ON 
					K.CODCOLIGADA=EX1.CODCOLIGADA
					AND K.CODFILIAL=EX1.CODFILIAL
					AND K.CODCCUSTO=EX1.OS COLLATE SQL_Latin1_General_CP1_CI_AI
					INNER JOIN KATVORDEM (NOLOCK) KA ON
					KA.CODCOLIGADA=K.CODCOLIGADA
					AND KA.CODFILIAL=K.CODFILIAL
					AND KA.CODORDEM=K.CODORDEM
					AND KA.CODESTRUTURA=EX1.CODIGOPRD COLLATE SQL_Latin1_General_CP1_CI_AI
					LEFT JOIN ZMDPLANOAPROVEITAMENTOCORTE (NOLOCK) PAPC ON
					PAPC.CODCOLIGADA=KA.CODCOLIGADA
					AND PAPC.CODFILIAL=KA.CODFILIAL
					AND PAPC.CODORDEM=KA.CODORDEM
					AND PAPC.CODATIVIDADE=KA.CODATIVIDADE
					INNER JOIN KORDEMCOMPL (NOLOCK) KL ON
					KL.CODCOLIGADA=KA.CODCOLIGADA
					AND KL.CODFILIAL=KA.CODFILIAL
					AND KL.CODORDEM=KA.CODORDEM
					AND KL.NUMEXEC=EX1.EXECUCAO
					INNER JOIN KATVORDEMMP (NOLOCK) KMA ON
					KMA.CODCOLIGADA=KA.CODCOLIGADA
					AND KMA.CODFILIAL=KA.CODFILIAL
					AND KMA.CODORDEM=KA.CODORDEM
					AND KMA.IDATIVIDADE=KA.IDATVORDEM
					INNER JOIN TPRD (NOLOCK) P ON         
					KMA.CODCOLIGADA = P.CODCOLIGADA 
					AND KMA.IDPRODUTO = P.IDPRD
				WHERE EX1.ITEMEXCLUSIVO<>2 
				AND OS='3.07847.32.001' AND EX1.EXECUCAO=4 AND K.STATUS NOT IN (6) AND KA.STATUS NOT IN (6) AND COALESCE(K.REPROCESSAMENTO,0)=0
				) R 
				WHERE (CODTB2FAT = 0184 OR MASKPRD NOT IN ('01.053','01.019','01.020')) AND 
				( PAPC='0' OR SALDOPAPC>0)
				GROUP BY EXECUCAO,CODCOLIGADA,CODFILIAL,INDICE,CODORDEM,IDATVORDEM,SALDOPAPC,PAPC,IDPRODUTO,CODTB2FAT,MASKPRD,CODATIVIDADE,CODESTRUTURA,CODIGOPRD
			) R2 
		) R3 GROUP BY EXECUCAO,CODCOLIGADA,CODFILIAL,INDICE,CODESTRUTURA,CODORDEM,IDATVORDEM,CODATIVIDADE,SALDOPAPC,PAPC,SUBSTITUTO,GRUPO,MASKPRD,CODIGOPRD )
, CTE2 AS ( SELECT *,CASE WHEN ( SELECT SUM(SALDO) FROM (SELECT (TC.SALDOFISICO1 + 
						TC.SALDOFISICO2 + 
						TC.SALDOFISICO3 + 
						TC.SALDOFISICO4 + 
						TC.SALDOFISICO5 + 
						TC.SALDOFISICO6 + 
						TC.SALDOFISICO7 + 
						TC.SALDOFISICO8 + 
						TC.SALDOFISICO9 +
						TC.SALDOFISICO10) SALDO,TC.IDLOTE,CODLOC,TC.IDPRD,TC.CODFILIAL,TC.CODCOLIGADA,TL.IDSTATUS,TL.NUMLOTE FROM TLOTEPRDLOC (NOLOCK) TC
						INNER JOIN TLOTEPRD (NOLOCK) TL ON
						TL.CODCOLIGADA=TC.CODCOLIGADA
						AND TL.IDLOTE=TC.IDLOTE
						AND TL.IDPRD=TC.IDPRD 
						) S WHERE(IDPRD=C.IDPRODUTO OR IDPRD IN (SELECT DISTINCT IDPRD FROM KCOMPSUBSTITUTO (NOLOCK) WHERE IDPRDORIGEM=C.IDPRODUTO AND CODESTRUTURA=C.CODESTRUTURA AND CODATIVIDADE=C.CODATIVIDADE))
						 AND CODFILIAL=C.CODFILIAL AND CODCOLIGADA=C.CODCOLIGADA AND CODLOC=23 ) >= C.PREVISTO THEN 1 ELSE 0 END AS SALDO23,
			CASE WHEN ( SELECT SUM(SALDO) FROM (SELECT (TC.SALDOFISICO1 + 
						TC.SALDOFISICO2 + 
						TC.SALDOFISICO3 + 
						TC.SALDOFISICO4 + 
						TC.SALDOFISICO5 + 
						TC.SALDOFISICO6 + 
						TC.SALDOFISICO7 + 
						TC.SALDOFISICO8 + 
						TC.SALDOFISICO9 +
						TC.SALDOFISICO10) SALDO,TC.IDLOTE,CODLOC,TC.IDPRD,TC.CODFILIAL,TC.CODCOLIGADA,TL.IDSTATUS,TL.NUMLOTE FROM TLOTEPRDLOC (NOLOCK) TC
						INNER JOIN TLOTEPRD (NOLOCK) TL ON
						TL.CODCOLIGADA=TC.CODCOLIGADA
						AND TL.IDLOTE=TC.IDLOTE
						AND TL.IDPRD=TC.IDPRD 
						) S WHERE (IDPRD=C.IDPRODUTO OR IDPRD IN (SELECT DISTINCT IDPRD FROM KCOMPSUBSTITUTO (NOLOCK) WHERE IDPRDORIGEM=C.IDPRODUTO AND CODESTRUTURA=C.CODESTRUTURA AND CODATIVIDADE=C.CODATIVIDADE))
						 AND CODFILIAL=C.CODFILIAL AND CODCOLIGADA=C.CODCOLIGADA AND CODLOC=23 AND IDSTATUS=6 ) >= C.PREVISTO THEN 1 ELSE 0 END AS SALDO23LIBERADO,
			CASE WHEN (SELECT TOP 1 NUMEROMOV FROM TMOV (NOLOCK) WHERE CODTMV='1.1.05' AND CODCOLIGADA=C.CODCOLIGADA AND STATUS='A' AND CODFILIAL=C.CODFILIAL 
						AND ((CAMPOLIVRE1=C.CODORDEM AND CAMPOLIVRE2=C.IDATVORDEM) OR CAMPOLIVRE3=C.PAPC )
						AND (IDPRODUTO = C.IDPRODUTO OR IDPRODUTO IN (SELECT DISTINCT IDPRD FROM KCOMPSUBSTITUTO (NOLOCK) WHERE IDPRDORIGEM=C.IDPRODUTO AND CODESTRUTURA=C.CODESTRUTURA AND CODATIVIDADE=C.CODATIVIDADE))) IS NOT NULL
				 THEN (SELECT TOP 1 NUMEROMOV FROM TMOV (NOLOCK) WHERE CODTMV='1.1.05' AND CODCOLIGADA=C.CODCOLIGADA AND STATUS='A' AND CODFILIAL=C.CODFILIAL 
						AND ((CAMPOLIVRE1=C.CODORDEM AND CAMPOLIVRE2=C.IDATVORDEM) OR CAMPOLIVRE3=C.PAPC ) 
						AND (IDPRODUTO = C.IDPRODUTO OR IDPRODUTO IN (SELECT DISTINCT IDPRD FROM KCOMPSUBSTITUTO (NOLOCK) WHERE IDPRDORIGEM=C.IDPRODUTO AND CODESTRUTURA=C.CODESTRUTURA AND CODATIVIDADE=C.CODATIVIDADE)))
				ELSE '0'
			END AS RAABERTA,
			CASE WHEN ( SELECT SUM(SALDO) FROM (SELECT (TC.SALDOFISICO1 + 
						TC.SALDOFISICO2 + 
						TC.SALDOFISICO3 + 
						TC.SALDOFISICO4 + 
						TC.SALDOFISICO5 + 
						TC.SALDOFISICO6 + 
						TC.SALDOFISICO7 + 
						TC.SALDOFISICO8 + 
						TC.SALDOFISICO9 +
						TC.SALDOFISICO10) SALDO,TC.IDLOTE,CODLOC,TC.IDPRD,TC.CODFILIAL,TC.CODCOLIGADA,TL.IDSTATUS,TL.NUMLOTE FROM TLOTEPRDLOC (NOLOCK) TC
						INNER JOIN TLOTEPRD (NOLOCK) TL ON
						TL.CODCOLIGADA=TC.CODCOLIGADA
						AND TL.IDLOTE=TC.IDLOTE
						AND TL.IDPRD=TC.IDPRD 
						) S WHERE (IDPRD=C.IDPRODUTO OR IDPRD IN (SELECT DISTINCT IDPRD FROM KCOMPSUBSTITUTO (NOLOCK) WHERE IDPRDORIGEM=C.IDPRODUTO AND CODESTRUTURA=C.CODESTRUTURA AND CODATIVIDADE=C.CODATIVIDADE))
							 AND CODFILIAL=C.CODFILIAL AND CODCOLIGADA=C.CODCOLIGADA AND CODLOC!=23) >= C.PREVISTO THEN 1 ELSE 0 END AS SALDOTOTAL,
			CASE WHEN ( SELECT SUM(SALDO) FROM (SELECT (TC.SALDOFISICO1 + 
						TC.SALDOFISICO2 + 
						TC.SALDOFISICO3 + 
						TC.SALDOFISICO4 + 
						TC.SALDOFISICO5 + 
						TC.SALDOFISICO6 + 
						TC.SALDOFISICO7 + 
						TC.SALDOFISICO8 + 
						TC.SALDOFISICO9 +
						TC.SALDOFISICO10) SALDO,TC.IDLOTE,CODLOC,TC.IDPRD,TC.CODFILIAL,TC.CODCOLIGADA,TL.IDSTATUS,TL.NUMLOTE FROM TLOTEPRDLOC (NOLOCK) TC
						INNER JOIN TLOTEPRD (NOLOCK) TL ON
						TL.CODCOLIGADA=TC.CODCOLIGADA
						AND TL.IDLOTE=TC.IDLOTE
						AND TL.IDPRD=TC.IDPRD 
						) S WHERE (IDPRD=C.IDPRODUTO OR IDPRD IN (SELECT DISTINCT IDPRD FROM KCOMPSUBSTITUTO (NOLOCK) WHERE IDPRDORIGEM=C.IDPRODUTO AND CODESTRUTURA=C.CODESTRUTURA AND CODATIVIDADE=C.CODATIVIDADE))
							AND CODFILIAL=C.CODFILIAL AND CODCOLIGADA=C.CODCOLIGADA AND CODLOC!=23 AND IDSTATUS=6) >= C.PREVISTO THEN 1 ELSE 0 END AS SALDOTOTALLIBERADO
																						 FROM CTE C
							WHERE ((PREVISTO - APONTADO) > 0 AND (GRUPO!='0184' OR GRUPO IS NULL))  OR (APONTADO <= 0 AND GRUPO='0184'))

SELECT C.INDICE,C.CODORDEM,C.IDATVORDEM,C.IDPRODUTO,C.CODIGOPRD,GRUPO,MASKPRD,CASE WHEN C.GRUPO='0184' -- GRUPO
			THEN -- ? DO GRUPO 0184
				CASE WHEN C.APONTADO > 0 -- TEM APONTAMENTO
					THEN -- EST? OK
						'OK'
					ELSE -- PENDENTE
						CASE WHEN C.SALDO23 = 1 -- POSSUI SALDO SUFICIENTE NO 23
							THEN  -- POSSUI SALDO SUFICIENTE NO 23
								CASE WHEN C.SALDO23LIBERADO = 1  -- POSSUI SALDO LIBERADO SUFICIENTE NO 23
									THEN -- POSSUI SALDO LIBERADO SUFICIENTE NO 23
									'Apontar Componente ou substituto'
									ELSE -- N?O POSSUI SALDO LIBERADO SUFICIENTE NO 23
									'Liberar lotes do local 23'
								END
							ELSE -- N?O POSSUI SALDO SUFICIENTE NO 23
								CASE WHEN C.RAABERTA != '0' -- POSSUI RA ABERTA
									THEN -- POSSUI RA ABERTA
									CONCAT('Atender RA ',C.RAABERTA)
									ELSE -- N?O POSSUI RA ABERTA
										CASE WHEN C.SALDOTOTAL = 1 -- POSSUI SALDO SUFICIENTE FORA DO 23
											THEN -- POSSUI SALDO SUFICIENTE FORA DO 23
												CASE WHEN C.SALDOTOTALLIBERADO = 1 -- POSSUI SALDO SUFICIENTE FORA DO 23 LIBERADO
													THEN -- POSSUI SALDO SUFICIENTE FORA DO 23 LIBERADO
													'Gerar RA'
													ELSE -- N?O POSSUI SALDO SUFICIENTE FORA DO 23 LIBERADO
													'Liberar estoque'
												END
											ELSE
											'Pendente entrada de nota, ou chegada do material'
										END
								END
						END
				END
			ELSE 
				CASE WHEN C.PAPC !='0' -- ATIVIDADE DE PLANO
					THEN -- ? ATIVIDADE DE PLANO
						CASE WHEN C.SALDOPAPC > 0 -- PLANO APONTADO
							THEN -- PLANO N?O APONTADO
							CONCAT('Apontar plano ',C.PAPC)
							ELSE -- PLANO APONTADO
							'OK'
						END
					ELSE -- N?O ? ATIVIDADE DE PLANO
						CASE WHEN C.MASKPRD IN ('01.053','01.019','01.020') -- CODIGO ERRADO
							THEN -- DESCARTA CODIGO
							'OK'
							ELSE
								CASE WHEN C.APONTADO >= C.PREVISTO -- APONTADO MAIOR QUE PREVISTO
									THEN -- APONTADO MAIOR QUE PREVISTO
										'OK'
									ELSE -- APONTADO MENOR QUE PREVISTO
										CASE WHEN C.MASKPRD IN ('03.023','04.024') -- ? PRODUTO ACABADO OU SEMIACABADO
											THEN  -- ? PRODUTO ACABADO OU SEMIACABADO
												CASE WHEN C.SALDOTOTAL = 1 -- POSSUI SALDO SUFICIENTE FORA DO 23
													THEN -- POSSUI SALDO SUFICIENTE FORA DO 23
													'Apontar Componente ou substituto'
													ELSE
													CONCAT('Subir saldo do produto ',C.CODIGOPRD)

												END
											--'Subir saldo da OP '
											ELSE -- N?O ? PRODUTO ACABADO OU SEMIACABADO
													CASE WHEN C.SALDO23 = 1 -- SALDO SUFICIENTE NO LOCAL DE ESTOQUE 23
														THEN -- SALDO SUFICIENTE NO LOCAL DE ESTOQUE 23
															CASE WHEN C.SALDO23LIBERADO = 1 -- SALDO SUFICIENTE LIBERADO NO LOCAL DE ESTOQUE 23 
																THEN  -- SALDO SUFICIENTE LIBERADO NO LOCAL DE ESTOQUE 23 
																'Apontar Componente ou substituto'
																ELSE -- SALDO INSUFICIENTE LIBERADO NO LOCAL DE ESTOQUE 23 
																'Liberar lotes do local 23'
															END
														ELSE -- SEM SALDO SUFICIENTE NO LOCAL DE ESTOQUE 23
															CASE WHEN C.RAABERTA != '0' -- POSSUI RA ABERTA
																THEN -- POSSUI RA ABERTA
																CONCAT('Atender RA ',C.RAABERTA)
																ELSE -- N?O POSSUI RA ABERTA
																	CASE WHEN C.SALDOTOTAL = 1 -- POSSUI SALDO SUFICIENTE FORA DO 23
																		THEN -- POSSUI SALDO SUFICIENTE FORA DO 23
																			CASE WHEN C.SALDOTOTALLIBERADO = 1 -- POSSUI SALDO SUFICIENTE FORA DO 23 LIBERADO
																				THEN -- POSSUI SALDO SUFICIENTE FORA DO 23 LIBERADO
																				'Gerar RA'
																				ELSE -- N?O POSSUI SALDO SUFICIENTE FORA DO 23 LIBERADO
																				'Liberar estoque'
																			END
																		ELSE
																		'Pendente entrada de nota, ou chegada do material'
																	END
															END
													END
											END
								END
						END
				END
		END AS A??O
FROM CTE2 C 
ORDER BY LEN(INDICE) DESC,CAST('/'+REPLACE(INDICE,'.','/')+'/' AS hierarchyid),CODORDEM,IDATVORDEM