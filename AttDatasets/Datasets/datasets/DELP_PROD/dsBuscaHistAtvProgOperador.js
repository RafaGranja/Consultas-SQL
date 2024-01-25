// BUSCA TODO O HISTÓRICO DAS ATIVIDADES QUE FORAM PROGRAMADAS PARA UM DETERMINADO OPERADOR
function createDataset(fields, constraints, sortFields) {

	var dsBuscaHistAtvProgOperador = DatasetBuilder.newDataset();
    var dataSource = "/jdbc/FluigDSRM";
    //var dataSource = "/jdbc/FluigDSRM";
    var ic = new javax.naming.InitialContext();
    var ds = ic.lookup(dataSource);
    var created = false;
    
    var codmo = "" 
    var codFilial = ""
    var codOrdem = ""
    var dataDe = ""
    var dataAte = ""
    var recursoEq = ""
    var ordenarPor = " ORDER BY 	CAST(A.DTHRINICIAL AS DATE), A.CODMO, A.CODORDEM, ATVE.PRIORIDADE "
    	
    log.info("Entrei no dataset dsBuscaHistAtvProgOperador")
    	
	if (constraints != null) {
    	
        for (var i = 0; i < constraints.length; i++) {
        	
        	if (constraints[i].fieldName == "CODMO"){
        		
        		codmo = constraints[i].initialValue
        		
        	}
			if (constraints[i].fieldName == "CODFILIAL"){
			        		
				codFilial = constraints[i].initialValue
        		
        	}
			if (constraints[i].fieldName == "CODORDEM"){
				
				codOrdem = constraints[i].initialValue
				
			}
			if (constraints[i].fieldName == "DATA_DE"){
				
				dataDe = constraints[i].initialValue
				
			}
			if (constraints[i].fieldName == "DATA_ATE"){
				
				dataAte = constraints[i].initialValue
				
			}
			if (constraints[i].fieldName == "ORDENAR_POR"){
					
				ordenarPor = " ORDER BY 	A.CODORDEM, A.IDATVORDEM "
				
			}
			
        }
        
    }
    
    var myQuery =   "  	WITH ANTERIOR AS ( "+
					" 	SELECT AATVE.CODCOLIGADA,AATVE.CODFILIAL,AATVE.CODESTRUTURA,AATVD.DSCATIVIDADE, AATVE.PRIORIDADE,MAX(AATVE2.PRIORIDADE ) ANTERIOR "+
					" 							FROM  KATVESTRUTURA AATVE  "+
					" 								INNER JOIN KATIVIDADE AATVD ON AATVE.CODCOLIGADA = AATVD.CODCOLIGADA AND AATVE.CODFILIAL = AATVD.CODFILIAL AND AATVE.CODATIVIDADE = AATVD.CODATIVIDADE  "+
					" 								LEFT JOIN KATVESTRUTURA AATVE2 ON "+
					" 								AATVE.CODCOLIGADA=AATVE2.CODCOLIGADA  "+
					" 								AND AATVE.CODFILIAL=AATVE2.CODFILIAL "+
					" 								AND AATVE.CODESTRUTURA=AATVE2.CODESTRUTURA "+
					" 								AND	AATVE2.PRIORIDADE < AATVE.PRIORIDADE   "+
					" 								WHERE (AATVE.CODESTRUTURA LIKE '03.023%' OR AATVE.CODESTRUTURA LIKE '04.024%') "+
					" 								GROUP BY AATVE.CODCOLIGADA,AATVE.CODFILIAL,AATVE.CODESTRUTURA,AATVD.DSCATIVIDADE,AATVE.PRIORIDADE "+
					" 							 "+
					" 	), TAP AS ( "+
					" 		SELECT  CODCOLIGADA,CODFILIAL,CODORDEM,IDATVORDEM,CODMO, "+
					" 		SUM(ROUND(CAST(DATEDIFF(minute,DTHRINICIAL,DTHRFINAL)AS FLOAT)/60,4))HORAS  "+
					" 		FROM KMAOOBRAALOC WHERE EFETIVADO=1 "+
					" 		GROUP BY CODCOLIGADA,CODFILIAL,CODORDEM,IDATVORDEM,CODMO "+
					" 	) "+
					" 	,ULTIMA AS ( "+
					" 	SELECT MAX(PRIORIDADE) PRIORIDADE,ATVOD.CODORDEM,ATVOD.CODFILIAL,ATVOD.CODCOLIGADA,ATVOD.IDATVORDEM "+
					" 									FROM   KATVORDEMCOMPL AATVE "+
					" 										INNER JOIN KATVORDEM ATVOD "+
					" 												ON AATVE.CODCOLIGADA = "+
					" 													ATVOD.CODCOLIGADA "+
					" 													AND AATVE.CODFILIAL = "+
					" 														ATVOD.CODFILIAL "+
					" 													AND AATVE.IDATVORDEM = "+
					" 														ATVOD.IDATVORDEM "+
					" 													AND "+
					" 										AATVE.CODORDEM = ATVOD.CODORDEM "+
					" 										INNER JOIN KATIVIDADE AATVD "+
					" 												ON AATVE.CODCOLIGADA = "+
					" 													AATVD.CODCOLIGADA "+
					" 													AND AATVE.CODFILIAL = "+
					" 														AATVD.CODFILIAL "+
					" 													AND AATVD.CODATIVIDADE = "+
					" 														ATVOD.CODATIVIDADE "+
					" 									WHERE  AATVE.CODCOLIGADA = 1 AND ATVOD.STATUS <> 6 AND ( ATVOD.CODESTRUTURA LIKE '03.023%' OR ATVOD.CODESTRUTURA LIKE '04.024%') "+
					" 									GROUP BY ATVOD.CODORDEM,ATVOD.CODFILIAL,ATVOD.CODCOLIGADA,ATVOD.IDATVORDEM ) "+
					" , "+
					" CTE AS (  "+
					"                "+
					"              SELECT PRJ.CODCOLIGADA,PRJ.CODFILIAL,PRJ.IDPRJ,PRJ.CODPRJ,'' FOLGACALC,TRF.CODTRFPAI,TRF.CODORDEM CODTRFAUX,ISNULL(TRF.TAG,'SEM TAG') TAG,CAST(ISNULL(CAST(TRF.CODCELULA AS VARCHAR(20)),'SEM CELULA') AS VARCHAR(20)) CELULA,ISNULL(CAST(TRF.DESCCELULA AS VARCHAR(20)),'SEM CELULA') DESCRICAO "+
					"              FROM MPRJ(NOLOCK)  PRJ  "+
					"              INNER JOIN FLUIG.DBO.Z_CRM_EX001005COMPL(NOLOCK) TRF ON PRJ.CODCOLIGADA = TRF.CODCOLIGADA AND PRJ.CODFILIAL=TRF.CODFILIAL AND PRJ.CODPRJ = TRF.OS COLLATE SQL_Latin1_General_CP1_CI_AI  "+
					"              WHERE PRJ.POSICAO IN(1,4) "+
					"            "+
					"         ) "+
					" 	SELECT * "+
					" 	FROM   (SELECT A.CODCOLIGADA, "+
					" 				A.CODFILIAL, "+
					" 				A.IDATVORDEMPROGRAMACAO, "+
					" 				M.CODMO + ' - ' + F.NOME                                  RECURSO "+
					" 				, "+
					" 				T.DESCRICAO "+
					" 				HORARIO, "+
					" 				ISNULL(POS.ANTERIOR, '-') "+
					" 				ATV_ANTERIOR, "+
					" 				P.CODIGOPRD, "+
					" 				P.IDPRD, "+
					" 				P.DESCRICAO                                               DSCITEM "+
					" 				, "+
					" 				ATV.CODPOSTO, "+
					" 				P.CODUNDCONTROLE, "+
					" 				ATV.CODORDEM                                              OP, "+
					" 				ATV.IDATVORDEM, "+
					" 				ATVC.PRIORIDADE, "+
					" 				ATV.PERCENTUAL "+
					" 				AVANCO_ATUAL, "+
					" 				X.CUSTO "+
					" 				CUSTO_POSTO, "+
					" 				ATVD.DSCATIVIDADE, "+
					" 				C.CELULA, "+
					" 				CONCAT(C.CELULA,'-',C.DESCRICAO)                           CELULAR "+
					" 				, "+
					" 				QTDHORAS "+
					" 				ALOCADO, "+
					" 				CAST(A.DTHRINICIAL AS DATE) "+
					" 						DATA_PROGRAMADA "+
					" 						, "+
					" 				CAST(A.DTHRFINAL AS DATE) "+
					" 						DATA_PROGRAMADAFINAL, "+
					" 				ATV.CODESTRUTURA, "+
					" 				ATV.CODATIVIDADE, "+
					" 				C.IDPRJ, "+
					" 				C.CODPRJ                                                OS, "+
					" 				KOC.NUMEXEC "+
					" 				EXECUCAO, "+
					" 				AC.DETALHE, "+
					" 				C.FOLGACALC, "+
					" 				ISNULL(KI.QTDEEFETIVADA, 0) "+
					" 				APONTADO, "+
					" 				KI.QTDEPLANEJADA, "+
					" 				( KI.QTDEPLANEJADA - ISNULL(KI.QTDEEFETIVADA, 0) )        SALDO, "+
					" 				KI.DTHRINICIALPREV "+
					" 						INICIO_PLANEJADO, "+
					" 				KI.DTHRFINALPREV "+
					" 						FINAL_PLANEJADO "+
					" 						, "+
					" 				KO.REPROCESSAMENTO "+
					" 						RETRABALHO, "+
					" 				KO.STATUS "+
					" 				STATUS_OP, "+
					" 				ATV.PERCENTUAL "+
					" 						AVANCO_REALIZADO, "+
					" 				ATV.STATUS "+
					" 				STATUS_ATV, "+
					" 				ATV.QUANTIDADE "+
					" 				QTDE_ATV, "+
					" 				ATV.QTPREVISTA "+
					" 				QTDE_PREV, "+
					" 				( ISNULL(ATV.QTPREVISTA, 0) - ISNULL(ATV.QUANTIDADE, 0) ) "+
					" 				QTDE_SALDO, "+
					" 				ISNULL(TAP.HORAS, 0) "+
					" 						TEM_APONTAMENTO "+
					" 						, "+
					" 				( CASE "+
					" 					WHEN ISNULL((ULT.CODORDEM), 'ULTIMA') = "+
					" 							'ULTIMA' "+
					" 							AND ISNULL(ZEX.OS, 'SIM') "+
					" 								<> "+
					" 								'SIM' THEN "+
					" 					'ULTIMA' "+
					" 					ELSE ATVD.DSCATIVIDADE "+
					" 					END ) "+
					" 				ATV_POSTERIOR "+
					" 			FROM   ZMDKATVORDEMPROGRAMACAO A "+
					" 				INNER JOIN KMAOOBRA M "+
					" 						ON A.CODCOLIGADA = M.CODCOLIGADA "+
					" 							AND A.CODMO = M.CODMO "+
					" 							AND A.CODFILIAL = M.CODFILIAL "+
					" 				LEFT JOIN PFUNC F "+
					" 						ON A.CODCOLIGADA = F.CODCOLIGADA "+
					" 							AND F.CODPESSOA = M.CODPESSOA "+
					" 							AND F.CODSITUACAO = 'A' "+
					" 				LEFT JOIN AHORARIO T "+
					" 						ON F.CODCOLIGADA = T.CODCOLIGADA "+
					" 							AND F.CODHORARIO = T.CODIGO "+
					" 				INNER JOIN KATVORDEM ATV "+
					" 						ON ATV.CODCOLIGADA = A.CODCOLIGADA "+
					" 							AND ATV.CODFILIAL = A.CODFILIAL "+
					" 							AND ATV.CODORDEM = A.CODORDEM "+
					" 							AND ATV.IDATVORDEM = A.IDATVORDEM "+
					" 				INNER JOIN KATVORDEMCOMPL ATVC "+
					" 						ON ATV.CODCOLIGADA = ATVC.CODCOLIGADA "+
					" 							AND ATV.CODFILIAL = ATVC.CODFILIAL "+
					" 							AND ATV.CODORDEM = ATVC.CODORDEM "+
					" 							AND ATV.IDATVORDEM = ATVC.IDATVORDEM "+
					" 				INNER JOIN KITEMORDEM KI "+
					" 						ON A.CODCOLIGADA = KI.CODCOLIGADA "+
					" 							AND A.CODFILIAL = KI.CODFILIAL "+
					" 							AND A.CODORDEM = KI.CODORDEM "+
					" 							AND A.CODESTRUTURA = KI.CODESTRUTURA "+
					" 				INNER JOIN KORDEM KO "+
					" 						ON ATV.CODCOLIGADA = KO.CODCOLIGADA "+
					" 							AND ATV.CODFILIAL = KO.CODFILIAL "+
					" 							AND ATV.CODORDEM = KO.CODORDEM "+
					" 				INNER JOIN KORDEMCOMPL KOC "+
					" 						ON ATV.CODCOLIGADA = KOC.CODCOLIGADA "+
					" 							AND ATV.CODFILIAL = KOC.CODFILIAL "+
					" 							AND ATV.CODORDEM = KOC.CODORDEM "+
					" 				INNER JOIN KATIVIDADE ATVD "+
					" 						ON ATV.CODCOLIGADA = ATVD.CODCOLIGADA "+
					" 							AND ATV.CODFILIAL = ATVD.CODFILIAL "+
					" 							AND ATV.CODATIVIDADE = ATVD.CODATIVIDADE "+
					" 				INNER JOIN TPRD P "+
					" 						ON ATV.CODCOLIGADA = P.CODCOLIGADA "+
					" 							AND ATV.CODESTRUTURA = P.CODIGOPRD "+
					" 				LEFT JOIN KATVESTRUTURACOMPL AC "+
					" 						ON A.CODCOLIGADA = AC.CODCOLIGADA "+
					" 							AND A.CODFILIAL = AC.CODFILIAL "+
					" 							AND ATV.CODESTRUTURA = AC.CODESTRUTURA "+
					" 							AND ATV.CODATIVIDADE = AC.CODATIVIDADE "+
					" 				INNER JOIN CTE C ON C.CODCOLIGADA=A.CODCOLIGADA AND C.CODFILIAL=A.CODFILIAL AND C.CODTRFAUX=A.CODORDEM "+
					" 				LEFT JOIN FLUIG.DBO.Z_CRM_EXPROJETOS ZEX ON  "+
					" 					ZEX.CODCOLIGADA = KOC.CODCOLIGADA "+
					" 						AND ZEX.CODFILIAL = KOC.CODFILIAL "+
					" 						AND ZEX.OS = KO.CODCCUSTO COLLATE Latin1_General_CI_AS "+
					" 						AND ZEX.EXECUCAO = KOC.NUMEXEC "+
					" 						AND ZEX.CODTRFPAI = LEFT(C.CODTRFPAI,4) COLLATE Latin1_General_CI_AS "+
					" 				LEFT JOIN ZMDPLANOAPROVEITAMENTOCORTE Z "+
					" 						ON Z.CODCOLIGADA = A.CODCOLIGADA "+
					" 							AND Z.CODFILIAL = A.CODFILIAL "+
					" 							AND Z.CODORDEM = ATV.CODORDEM "+
					" 							AND Z.CODATIVIDADE = ATV.CODATIVIDADE "+
					" 					LEFT JOIN  KCUSTOPOSTO (NOLOCK) X "+
					" 					ON  X.CODCOLIGADA = ATV.CODCOLIGADA "+
					" 						AND X.CODFILIAL = ATV.CODFILIAL "+
					" 						AND X.CODPOSTO = ATV.CODPOSTO "+
					" 						AND X.DTINICIAL <= CAST(GETDATE() AS DATE) "+
					" 						AND X.DTFINAL >= CAST(GETDATE() AS DATE) "+
					" 					LEFT JOIN ANTERIOR POS ON POS.CODCOLIGADA = A.CODCOLIGADA AND	POS.CODFILIAL = A.CODFILIAL AND POS.CODESTRUTURA = A.CODESTRUTURA AND ATVC.PRIORIDADE=POS.PRIORIDADE "+
					" 					LEFT JOIN TAP TAP ON TAP.CODCOLIGADA=A.CODCOLIGADA AND TAP.CODFILIAL=A.CODFILIAL AND TAP.CODORDEM=A.CODORDEM AND TAP.IDATVORDEM=A.IDATVORDEM AND TAP.CODMO=A.CODMO "+
					" 					LEFT JOIN ULTIMA ULT ON ULT.CODCOLIGADA=A.CODCOLIGADA AND ULT.CODFILIAL=A.CODFILIAL AND ULT.CODORDEM=A.CODORDEM AND ULT.IDATVORDEM=A.IDATVORDEM AND ATVC.PRIORIDADE=ULT.PRIORIDADE "+
					"	WHERE	"+
					"		A.STATUS = 0 "+
					"		AND KO.STATUS NOT IN (5,6) "+
					"		AND		A.CODCOLIGADA = 1 "+
					"		AND		A.CODFILIAL = "+codFilial+" "+
					"		AND		A.CODMO = '"+codmo+"' "+
					"		AND 	CAST(A.DTHRINICIAL AS DATE) = '"+dataDe+"' "+
					"		AND		Z.NUMPLANOCORTE IS NULL "+
					"	) AS Z "+
					"	ORDER BY "+
					"		CAST(Z.DATA_PROGRAMADA AS DATE), Z.RECURSO, Z.OP, Z.PRIORIDADE  "



    log.info("dsBucaHistAtvProgOperador: " + myQuery);
    
    try {
    	
        var conn = ds.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery(myQuery);
        var columnCount = rs.getMetaData().getColumnCount();
        
        while (rs.next()) {
        	
            if (!created) {
            	
                for (var i = 1; i <= columnCount; i++) {
                	
                	dsBuscaHistAtvProgOperador.addColumn(rs.getMetaData().getColumnName(i));
                	
                }
                
                created = true;
                
            }
            
            var Arr = new Array();
            
            for (var i = 1; i <= columnCount; i++) {
            	
                var obj = rs.getObject(rs.getMetaData().getColumnName(i));
                
                if (null != obj) {
                	
                    Arr[i - 1] = rs.getObject(rs.getMetaData().getColumnName(i)).toString();
                    
                } else {
                	
                    Arr[i - 1] = "null";
                    
                }
                
            }
            
            dsBuscaHistAtvProgOperador.addRow(Arr);
            
        }
        
    } catch (e) {
        
    	log.error("ERRO==============> " + e.message);
    
    } finally {
    	
        if (stmt != null) {
        	
            stmt.close();
            
        }
        
        if (conn != null) {
        	
            conn.close();
            
        }
        
    }
    
    return dsBuscaHistAtvProgOperador;
	
}