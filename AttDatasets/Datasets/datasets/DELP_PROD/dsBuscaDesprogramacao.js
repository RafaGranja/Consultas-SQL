// BUSCA TODO O HISTÓRICO DAS ATIVIDADES QUE FORAM PROGRAMADAS PARA UM DETERMINADO OPERADOR
function createDataset(fields, constraints, sortFields) {

	var dsBuscaDesprogramacao = DatasetBuilder.newDataset();
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
    	
    log.info("Entrei no dataset dsBuscaDesprogramacao")
    	
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
    
    var myQuery =   "  ;WITH CTE AS (  "+
					" 			SELECT PRJ.CODCOLIGADA,PRJ.CODFILIAL,PRJ.IDPRJ,PRJ.CODPRJ,TRF.CODORDEM CODTRFAUX,ISNULL(TRF.TAG,'SEM TAG') TAG,ISNULL(CAST(TRF.CODCELULA AS VARCHAR(20)),'SEM CELULA') CELULA,ISNULL(CAST(TRF.DESCCELULA AS VARCHAR(20)),'SEM CELULA') DESCRICAO "+
					" 			FROM MPRJ(NOLOCK)  PRJ  "+
					" 			INNER JOIN FLUIG.DBO.Z_CRM_EX001005COMPL (NOLOCK) TRF ON PRJ.CODCOLIGADA = TRF.CODCOLIGADA AND PRJ.CODCCUSTO = TRF.OS COLLATE SQL_Latin1_General_CP1_CI_AI "+
					" 			WHERE PRJ.POSICAO IN(1,4)   "+
					" 		 "+
					" 	) "+
					" 	SELECT Z.CODCOLIGADA,Z.CODFILIAL,Z.CODESTRUTURA,Z.CODORDEM OP,C.IDPRJ,C.CODPRJ OS,Z.IDATVORDEM,KA.CODATIVIDADE,KS.DSCATIVIDADE,KL.PRIORIDADE,Z.IDATVORDEMPROGRAMACAO,K.REPROCESSAMENTO RETRABALHO "+
					" 	,Z.QTDHORAS ALOCADO,C.CELULA,C.CELULA+'-'+C.DESCRICAO CELULAR  "+
					" 	FROM ZMDKATVORDEMPROGRAMACAO Z  "+
					" 	INNER JOIN KORDEM K ON "+
					" 	Z.CODCOLIGADA=K.CODCOLIGADA "+
					" 	AND Z.CODFILIAL=K.CODFILIAL "+
					" 	AND Z.CODORDEM=K.CODORDEM "+
					" 	INNER JOIN KATVORDEM KA ON "+
					" 	KA.CODCOLIGADA=K.CODCOLIGADA  "+
					" 	AND KA.CODFILIAL=K.CODFILIAL "+
					" 	AND KA.CODORDEM=K.CODORDEM  "+
					" 	AND KA.IDATVORDEM=Z.IDATVORDEM "+
					" 	INNER JOIN KATVORDEMCOMPL KL ON "+
					" 	KL.CODCOLIGADA=KA.CODCOLIGADA "+
					" 	AND KL.CODFILIAL=KA.CODFILIAL "+
					" 	AND KL.CODORDEM=KA.CODORDEM "+
					" 	AND KL.IDATVORDEM=KA.IDATVORDEM "+
					" 	INNER JOIN KATIVIDADE KS ON "+
					" 	KS.CODCOLIGADA=KA.CODCOLIGADA "+
					" 	AND KS.CODFILIAL=KA.CODFILIAL "+
					" 	AND KS.CODATIVIDADE=KA.CODATIVIDADE "+
					" 	INNER JOIN CTE C ON C.CODCOLIGADA=Z.CODCOLIGADA AND C.CODFILIAL=Z.CODFILIAL AND C.CODTRFAUX=Z.CODORDEM "+
					"		WHERE  Z.STATUS = 0 "+
					"		AND K.STATUS NOT IN (5,6) "+
					"		AND KA.STATUS NOT IN (6) "+
					"		AND		Z.CODCOLIGADA = 1 "+
					"		AND		Z.CODFILIAL = "+codFilial+" "+
					"		AND		Z.CODMO = '"+codmo+"' "+
					"		AND 	CAST(Z.DTHRINICIAL AS DATE) = '"+dataDe+"' "+
					"		AND		( Z.NUMPLANOCORTE IS NULL OR Z.NUMPLANOCORTE='' )  "
					"	ORDER BY "+
					"		CAST(Z.DTHRINICIAL AS DATE), Z.CODMO, Z.CODORDEM, Z.PRIORIDADE  "

					



    log.info("dsBucaHistAtvProgOperador: " + myQuery);
    
    try {
    	
        var conn = ds.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery(myQuery);
        var columnCount = rs.getMetaData().getColumnCount();
        
        while (rs.next()) {
        	
            if (!created) {
            	
                for (var i = 1; i <= columnCount; i++) {
                	
                	dsBuscaDesprogramacao.addColumn(rs.getMetaData().getColumnName(i));
                	
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
            
            dsBuscaDesprogramacao.addRow(Arr);
            
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
    
    return dsBuscaDesprogramacao;
	
}