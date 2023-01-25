SELECT	 	
ATV.CODCOLIGADA, ATV.CODFILIAL, 	
ISNULL((	SELECT TOP 1 AATVD.DSCATIVIDADE 		
FROM  KATVESTRUTURA AATVE 			
INNER JOIN KATIVIDADE AATVD ON AATVE.CODCOLIGADA = AATVD.CODCOLIGADA AND AATVE.CODFILIAL = AATVD.CODFILIAL AND AATVE.CODATIVIDADE = AATVD.CODATIVIDADE 		
WHERE	AATVE.CODCOLIGADA = ATV.CODCOLIGADA 		
AND		AATVE.CODFILIAL = ATV.CODFILIAL 		
AND		AATVE.CODESTRUTURA = ATV.CODESTRUTURA 		
AND		AATVE.PRIORIDADE < ATVE.PRIORIDADE 		
ORDER BY AATVE.PRIORIDADE DESC),'-') ATV_ANTERIOR, 	P.CODIGOPRD, P.IDPRD, P.DESCRICAO DSCITEM, ATV.QTPREVISTA QTDEPLANEJADA, ATV.QUANTIDADE APONTADO, 
ATV.QTPREVISTA-ATV.QUANTIDADE SALDO, ATV.CODPOSTO, P.CODUNDCONTROLE, ATV.CODORDEM OP,ATV.IDATVORDEM, ATVE.PRIORIDADE, 	
(SELECT 		CUSTO 	 FROM 		KCUSTOPOSTO (NOLOCK) X 	 WHERE 		X.CODCOLIGADA = ATV.CODCOLIGADA AND X.CODFILIAL = ATV.CODFILIAL 		
AND X.CODPOSTO = ATV.CODPOSTO AND CAST(DTINICIAL AS DATE) <= CAST(GETDATE() AS DATE) AND CAST(DTFINAL AS DATE) >= CAST(GETDATE() AS DATE) 	) CUSTO_POSTO, 	
ATVD.DSCATIVIDADE, ATVD.CODATIVIDADE, TRFC.CELULA, TRFC.CELULA+'-'+C.DESCRICAO CELULAR,  	
ATVE.CODESTRUTURA, ATVE.CODATIVIDADE,TRF.IDPRJ, PRJ.CODPRJ OS, 	TRF.FOLGACALC, 	ATV.PERCENTUAL AVANCO_REALIZADO, 	
Z.CODSUCATA, Z.QTDESUCATA, Z.QUANTIDADE QTDEPLANO, Z.QTDEAPONTADA, 	
ISNULL(( 		SELECT 			SUM(ISNULL(QTDEAPONTADA,0)) 		
					FROM ZMDPLANOAPROVEITAMENTOCORTE 		
					WHERE NUMPLANOCORTE = Z.NUMPLANOCORTE 		
					AND CODCOLIGADA = ATV.CODCOLIGADA AND CODFILIAL = ATV.CODFILIAL  	),0) TEM_APONTAMENTO, 		
					ISNULL((	SELECT TOP 1 AATVD.DSCATIVIDADE 		FROM  KATVORDEMCOMPL AATVE 			
					INNER JOIN KATVORDEM ATVOD ON AATVE.CODCOLIGADA = ATVOD.CODCOLIGADA AND AATVE.CODFILIAL = ATVOD.CODFILIAL AND AATVE.IDATVORDEM = ATVOD.IDATVORDEM 			
					INNER JOIN KATIVIDADE AATVD ON AATVE.CODCOLIGADA = AATVD.CODCOLIGADA AND AATVE.CODFILIAL = AATVD.CODFILIAL AND AATVD.CODATIVIDADE = ATVOD.CODATIVIDADE 		
				WHERE	AATVE.CODCOLIGADA = ATV.CODCOLIGADA 		
				AND		AATVE.CODFILIAL = ATV.CODFILIAL 		
				AND		AATVE.CODESTRUTURA = ATV.CODESTRUTURA  		
				AND		AATVE.PRIORIDADE > ATVOC.PRIORIDADE 		
				ORDER BY AATVE.PRIORIDADE),'ULTIMA') ATV_POSTERIOR, TRF.CODTRF
				FROM KATVORDEM ATV 	
LEFT JOIN KATVORDEMCOMPL ATVOC ON ATV.CODCOLIGADA = ATVOC.CODCOLIGADA AND ATV.CODFILIAL = ATVOC.CODFILIAL AND ATV.CODORDEM = ATVOC.CODORDEM AND ATV.IDATVORDEM = ATVOC.IDATVORDEM 	--PRIORIDADE
LEFT JOIN KATVESTRUTURA ATVE ON ATV.CODCOLIGADA = ATVE.CODCOLIGADA AND ATV.CODFILIAL = ATVE.CODFILIAL AND ATV.CODESTRUTURA = ATVE.CODESTRUTURA AND ATV.CODATIVIDADE = ATVE.CODATIVIDADE --CODESTRUTURA
LEFT JOIN KATIVIDADE ATVD ON ATV.CODCOLIGADA = ATVD.CODCOLIGADA AND ATV.CODFILIAL = ATVD.CODFILIAL AND ATV.CODATIVIDADE = ATVD.CODATIVIDADE --CODATIVIDADE
INNER JOIN TPRD P ON ATV.CODCOLIGADA = P.CODCOLIGADA AND ATV.CODESTRUTURA = P.CODIGOPRD 	
LEFT JOIN MTRF TRF ON TRF.CODCOLIGADA = ATV.CODCOLIGADA AND TRF.CODTRFAUX = ATV.CODORDEM 	
LEFT JOIN MPRJ PRJ ON PRJ.CODCOLIGADA = TRF.CODCOLIGADA AND PRJ.IDPRJ = TRF.IDPRJ AND PRJ.POSICAO IN (1,4) 	-- OS
LEFT JOIN MTRFCOMPL TRFC ON TRF.CODCOLIGADA = TRFC.CODCOLIGADA AND TRF.IDPRJ = TRFC.IDPRJ  AND TRF.IDTRF = TRFC.IDTRF 	--CELULA
LEFT JOIN GCONSIST C ON C.CODTABELA='CELULA' AND C.APLICACAO = 'M' AND C.CODINTERNO = TRFC.CELULA 	
INNER JOIN ZMDPLANOAPROVEITAMENTOCORTE Z ON ATV.CODCOLIGADA = Z.CODCOLIGADA AND ATV.CODFILIAL = Z.CODFILIAL AND ATV.CODORDEM = Z.CODORDEM AND ATV.IDATVORDEM = Z.IDATVORDEM 
WHERE		ATV.CODCOLIGADA = 1 	AND		ATV.CODFILIAL = 7 	AND		Z.NUMPLANOCORTE = 'CNC004092' ORDER BY 	ATV.CODORDEM, ATVE.PRIORIDADE


--SELECT * FROM KESTRUTURA WHERE CODESTRUTURA='03.023.0139148'

--SELECT * FROM KORDEMCOMPL WHERE CODORDEM='42806/22'

--SELECT * FROM KATVORDEM WHERE CODORDEM='42806/22'

--SELECT * FROM ZMDPLANOAPROVEITAMENTOCORTE WHERE CODORDEM='42806/22'

--SELECT * FROM KORDEM WHERE CODORDEM='42806/22'

--SELECT * FROM KORDEM WHERE CODORDEM='24382/21'


select * from KESTRUTURA where CODESTRUTURA='03.023.0139148'


select * from MTRF where IDPRJ=1370 