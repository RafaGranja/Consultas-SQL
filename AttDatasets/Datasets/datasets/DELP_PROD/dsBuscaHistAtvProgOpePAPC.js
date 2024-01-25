// BUSCA TODO O HISTÓRICO DAS ATIVIDADES QUE FORAM PROGRAMADAS PARA UM DETERMINADO OPERADOR
function createDataset(fields, constraints, sortFields) {

	var dsBuscaHistAtvProgOpePAPC = DatasetBuilder.newDataset();
    var dataSource = "/jdbc/FluigDSRM";
    var ic = new javax.naming.InitialContext();
    var ds = ic.lookup(dataSource);
    var created = false;
    
    var codmo = "" 
    var codFilial = ""
    var numPlanoCorte = ""
    var dataDe = ""
    var dataAte = ""
    	
    log.info("Entrei no dataset dsBuscaHistAtvProgOpePAPC")
    	
	if (constraints != null) {
    	
        for (var i = 0; i < constraints.length; i++) {
        	
        	if (constraints[i].fieldName == "CODMO"){
        		
        		codmo = constraints[i].initialValue
        		
        	}
			if (constraints[i].fieldName == "CODFILIAL"){
			        		
				codFilial = constraints[i].initialValue
        		
        	}
			if (constraints[i].fieldName == "NUMPLANOCORTE"){
				
				numPlanoCorte = constraints[i].initialValue
				
			}
			if (constraints[i].fieldName == "DATA_DE"){
				
				dataDe = constraints[i].initialValue
				
			}
			if (constraints[i].fieldName == "DATA_ATE"){
				
				dataAte = constraints[i].initialValue
				
			}
        	
        }
        
    }
    
    var myQuery = 	"SELECT	 "+
    				"	ATV.CODCOLIGADA, ATV.CODFILIAL, "+
					"	ISNULL((	SELECT TOP 1 AATVD.DSCATIVIDADE "+
					"		FROM  KATVESTRUTURA AATVE "+
					"			INNER JOIN KATIVIDADE AATVD ON AATVE.CODCOLIGADA = AATVD.CODCOLIGADA AND AATVE.CODFILIAL = AATVD.CODFILIAL AND AATVE.CODATIVIDADE = AATVD.CODATIVIDADE "+
					"		WHERE	AATVE.CODCOLIGADA = ATV.CODCOLIGADA "+
					"		AND		AATVE.CODFILIAL = ATV.CODFILIAL "+
					"		AND		AATVE.CODESTRUTURA = ATV.CODESTRUTURA "+ 
					"		AND		AATVE.PRIORIDADE < ATVOC.PRIORIDADE "+
					"		ORDER BY AATVE.PRIORIDADE DESC),'-') ATV_ANTERIOR, "+
					"	P.CODIGOPRD, P.IDPRD, P.DESCRICAO DSCITEM, ATV.QTPREVISTA QTDEPLANEJADA, ATV.QUANTIDADE APONTADO, ATV.QTPREVISTA-ATV.QUANTIDADE SALDO, ATV.CODPOSTO, P.CODUNDCONTROLE, ATV.CODORDEM OP,ATV.IDATVORDEM, ATVOC.PRIORIDADE, "+
					"	(SELECT "+
					"		CUSTO "+
					"	 FROM "+
					"		KCUSTOPOSTO (NOLOCK) X "+
					"	 WHERE "+
					"		X.CODCOLIGADA = ATV.CODCOLIGADA AND X.CODFILIAL = ATV.CODFILIAL "+
					"		AND X.CODPOSTO = ATV.CODPOSTO AND CAST(DTINICIAL AS DATE) <= CAST(GETDATE() AS DATE) AND CAST(DTFINAL AS DATE) >= CAST(GETDATE() AS DATE) "+
					"	) CUSTO_POSTO, "+
					"	ATVD.DSCATIVIDADE, ATVD.CODATIVIDADE, TRF.CODCELULA CELULA, CONCAT(TRF.CODCELULA,'-',TRF.DESCCELULA) CELULAR,  "+
					"	ATV.CODESTRUTURA, ATV.CODATIVIDADE,PRJ.IDPRJ, PRJ.CODPRJ OS, " +
					"	null FOLGACALC, "+ 
					"	ATV.PERCENTUAL AVANCO_REALIZADO, "+
					"	Z.CODSUCATA, Z.QTDESUCATA, Z.QUANTIDADE QTDEPLANO, Z.QTDEAPONTADA, Z.QTDMPFINAL, "+
					"	ISNULL(( "+
					"		SELECT "+
					"			SUM(ISNULL(QTDEAPONTADA,0)) "+
					"		FROM "+
					"			ZMDPLANOAPROVEITAMENTOCORTE "+
					"		WHERE  "+
					"			NUMPLANOCORTE = Z.NUMPLANOCORTE "+
					"			AND CODCOLIGADA = ATV.CODCOLIGADA AND CODFILIAL = ATV.CODFILIAL  "+
					"	),0) TEM_APONTAMENTO, "+
					"	(CASE WHEN ISNULL((SELECT "+
					"			TOP 1 AATVD.DSCATIVIDADE "+
					"		FROM  "+
					"			KATVORDEMCOMPL AATVE "+
					"			INNER JOIN KATVORDEM ATVOD ON AATVE.CODCOLIGADA = ATVOD.CODCOLIGADA AND AATVE.CODFILIAL = ATVOD.CODFILIAL AND AATVE.IDATVORDEM = ATVOD.IDATVORDEM AND AATVE.CODORDEM=ATVOD.CODORDEM "+
					"			INNER JOIN KATIVIDADE AATVD ON AATVE.CODCOLIGADA = AATVD.CODCOLIGADA AND AATVE.CODFILIAL = AATVD.CODFILIAL AND AATVD.CODATIVIDADE = ATVOD.CODATIVIDADE "+
					"		WHERE	"+
					"			AATVE.CODCOLIGADA = 1 "+
					"			AND		AATVE.CODFILIAL = ATV.CODFILIAL "+
					"			AND		AATVE.CODESTRUTURA = ATV.CODESTRUTURA "+ 
					"			AND		AATVE.CODORDEM = ATV.CODORDEM 	"+
					"			AND		AATVE.PRIORIDADE > ATVOC.PRIORIDADE "+ 
					"		ORDER BY "+
					"			AATVE.PRIORIDADE),'ULTIMA') = 'ULTIMA' AND ISNULL((SELECT OS FROM FLUIG.DBO.Z_CRM_EXPROJETOS WHERE CODCOLIGADA=EX.CODCOLIGADA AND CODFILIAL=EX.CODFILIAL AND OS=EX.OS AND EXECUCAO=EX.EXECUCAO AND CODTRFPAI=EX.CODTRFPAI),'SIM') <> 'SIM' THEN 'ULTIMA' ELSE ATVD.DSCATIVIDADE END) ATV_POSTERIOR "+     
					" FROM  "+
					"	KATVORDEM ATV "+
					"	INNER JOIN KATVORDEMCOMPL ATVOC ON ATV.CODCOLIGADA = ATVOC.CODCOLIGADA AND ATV.CODFILIAL = ATVOC.CODFILIAL AND ATV.CODORDEM = ATVOC.CODORDEM AND ATV.IDATVORDEM = ATVOC.IDATVORDEM "+
					"	INNER JOIN KORDEMCOMPL KOC ON ATV.CODCOLIGADA = KOC.CODCOLIGADA AND ATV.CODFILIAL = KOC.CODFILIAL AND ATV.CODORDEM = KOC.CODORDEM "+
					"	INNER JOIN KATIVIDADE ATVD ON ATV.CODCOLIGADA = ATVD.CODCOLIGADA AND ATV.CODFILIAL = ATVD.CODFILIAL AND ATV.CODATIVIDADE = ATVD.CODATIVIDADE "+
					"	INNER JOIN TPRD P ON ATV.CODCOLIGADA = P.CODCOLIGADA AND ATV.CODESTRUTURA = P.CODIGOPRD "+
					"	INNER JOIN FLUIG.DBO.Z_CRM_EX001005COMPL TRF ON TRF.CODCOLIGADA = ATV.CODCOLIGADA AND TRF.CODFILIAL = ATV.CODFILIAL AND TRF.CODORDEM = ATV.CODORDEM COLLATE SQL_Latin1_General_CP1_CI_AI "+
					"	INNER JOIN MPRJ PRJ ON PRJ.CODCOLIGADA = TRF.CODCOLIGADA AND PRJ.CODPRJ = TRF.OS COLLATE SQL_Latin1_General_CP1_CI_AI AND PRJ.POSICAO IN (1,4) "+
					"	INNER JOIN ZMDPLANOAPROVEITAMENTOCORTE Z ON ATV.CODCOLIGADA = Z.CODCOLIGADA AND ATV.CODFILIAL = Z.CODFILIAL AND ATV.CODORDEM = Z.CODORDEM AND ATV.CODATIVIDADE = Z.CODATIVIDADE "+
					"	LEFT JOIN FLUIG.DBO.Z_CRM_EX001005 EX (NOLOCK) ON ATV.CODCOLIGADA = EX.CODCOLIGADA AND ATV.CODFILIAL = EX.CODFILIAL AND ATV.CODESTRUTURA = EX.CODIGOPRD COLLATE SQL_Latin1_General_CP1_CI_AI AND KOC.NUMEXEC = EX.EXECUCAO "+
					"WHERE	"+
					"	ATV.CODCOLIGADA = 1 "+
					"	AND	ATV.CODFILIAL = "+codFilial+" "+ 
					"	AND	Z.NUMPLANOCORTE = '"+numPlanoCorte+"' "+
					"ORDER BY "+
					"	ATV.CODORDEM, ATVOC.PRIORIDADE "

	    
    
    log.info("QUERY dsBuscaHistAtvProgOpePAPC: " + myQuery);
    
    try {
    	
        var conn = ds.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery(myQuery);
        var columnCount = rs.getMetaData().getColumnCount();
        
        while (rs.next()) {
        	
            if (!created) {
            	
                for (var i = 1; i <= columnCount; i++) {
                	
                	dsBuscaHistAtvProgOpePAPC.addColumn(rs.getMetaData().getColumnName(i));
                	
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
            
            dsBuscaHistAtvProgOpePAPC.addRow(Arr);
            
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
    
    return dsBuscaHistAtvProgOpePAPC;
	
}