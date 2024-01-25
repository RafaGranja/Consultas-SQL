// CONSULTA PRINCIPAL COM TODAS AS INFORMAÇÕES NECESSÁRIAS DE ACORDO COM OS FILTROS INFORMADOS NO FORMULÁRIO
function createDataset(fields, constraints, sortFields) {

	var dsPlanejamentoGeralOS = DatasetBuilder.newDataset();
    var dataSource = "/jdbc/FluigDSRM";
    var ic = new javax.naming.InitialContext();
    var ds = ic.lookup(dataSource);
    var created = false;
    
    var celula = ""
    var numOS = ""
    var codOrdem = ""
    var codAtividade = ""
    var inferior = ""
    var idprd = ""
    var codigoPrd = ""
    var alocacao = ""
    var status = ""
    var codPosto = ""
    var dataInicioPrev = ""
	var dataFinalPrev = ""
    var orderBy = ""
    var folgaDe = ""
    var folgaAte = ""
    var nivelDe = ""
    var nivelAte = ""
    var filial = ""
    	
        
    log.info("Entrei no Dataset dsPlanejamentoGeralOS")
    log.info("Vou colher as constraints")	
    if (constraints != null) {
    	
        for (var i = 0; i < constraints.length; i++) {
        	
        	if (constraints[i].fieldName == "CELULA") {
            	
        		// SE CONTEÚDO NÃO É NULO OU VAZIO
            	if(!(constraints[i].initialValue=="" || constraints[i].initialValue==null || constraints[i].initialValue==undefined)){
            		
            		celula = " AND CELULA='"+constraints[i].initialValue+"'"
                		
            	}
            	
            }

            if (constraints[i].fieldName == "CODPRJ") {
            	
            	if(!(constraints[i].initialValue=="")){
            		
            		numOS = " AND OS='"+constraints[i].initialValue+"'"
            	
            	}
            	
            }
            
            if (constraints[i].fieldName == "FILIAL") {
            	
            	if(!(constraints[i].initialValue=="")){
            		
            		filial = " CODFILIAL='"+constraints[i].initialValue+"'"
            	
            	}
            	
            }
            
            if (constraints[i].fieldName == "CODORDEM") {
            	
            	var ordemAux = constraints[i].initialValue
            	
            	// SE CONTEÚDO NÃO É NULO OU VAZIO
            	if(!(ordemAux=="" || ordemAux==null || ordemAux==undefined)){
            		
            		// SE É MAIS DE UMA ORDEM
            		if(ordemAux.indexOf(";")!=-1){
                		
            			log.info("ordemAux: "+ordemAux)
            			
            			ordemAux = ordemAux.split(";")
                		
            			// PERCORRE TODAS AS ORDENS
            			for(var k=0;k<ordemAux.length;k++){
            				
            				// SE É O PRIMEIRO ITEM
            				if(k==0){
            					
            					codOrdem = " AND (OP='"+ordemAux[k]+"' "
            						
            				} else {
            					// SE NÃO
            					
            					codOrdem = codOrdem + " OR OP='"+ordemAux[k]+"' "
            					
            				}
            				
            			}
            			
            			codOrdem = codOrdem + " ) "
            			
                		
                	}else{
                		// SE É SOMENTE UMA ORDEM
                		
                		codOrdem = " AND OP='"+ordemAux+"' "
                		
                	}
            		
            	}
            	
            	
            	/*if(!(constraints[i].initialValue=="")){
            		
            		codOrdem = " AND OP='"+constraints[i].initialValue+"'"
            		
            	}*/
            	
            }
            
            if (constraints[i].fieldName == "IDPRD") {
            	
            	var idprdAux = constraints[i].initialValue
            	
            	// SE CONTEÚDO NÃO É NULO OU VAZIO
            	if(!(idprdAux=="" || idprdAux==null || idprdAux==undefined)){
            		
            		// SE É MAIS DE UMA ORDEM
            		if(idprdAux.indexOf(";")!=-1){
                		
            			log.info("idprdAux: "+idprdAux)
            			
            			idprdAux = idprdAux.split(";")
                		
            			log.info("idprdAux.length: "+idprdAux.length)
            			
            			// PERCORRE TODAS AS ORDENS
            			for(var j=0; j<idprdAux.length; j++){
            				
            				// SE É O PRIMEIRO ITEM
            				if(j==0){
            					
            					log.info("primeiro item")
            					
            					idprd = " AND (IDPRD='"+idprdAux[j]+"' "
            						
            				} else {
            					// SE NÃO
            					
            					log.info("mais um item")
            					
            					idprd = idprd + " OR IDPRD='"+idprdAux[j]+"' "
            					
            				}
            				
            			}
            			
            			idprd = idprd + " ) "
            			
                	} else {
                		// SE É SOMENTE UMA ORDEM
                		
                		idprd = " AND IDPRD='"+idprdAux+"' "
                		
                	}
            		
            	}
            	
            	/*if(!(constraints[i].initialValue=="")){
            		
            		codOrdem = " AND OP='"+constraints[i].initialValue+"'"
            		
            	}*/
            	
            }
            
            if (constraints[i].fieldName == "CODATIVIDADE") {
            	
            	if(!(constraints[i].initialValue=="")){
            		
            		codAtividade = " AND CODATIVIDADE='"+constraints[i].initialValue+"'"
            		
            	}
            	
            }
            
            if (constraints[i].fieldName == "INFERIOR") {
            	
            	if(!(constraints[i].initialValue=="")){
            		
            		inferior = " AND INDICE_FLUIG LIKE '"+constraints[i].initialValue+"%'"
            		
            	}
            	
            }
            /*
            if (constraints[i].fieldName == "IDPRD") {
            	
            	if(!(constraints[i].initialValue=="")){
            		
            		idprd = " AND IDPRD='"+constraints[i].initialValue+"'"
            		
            	}
            	
            }*/
            
			if (constraints[i].fieldName == "CODIGOPRD") {
				
				if(!(constraints[i].initialValue=="")){
					
					codigoPrd = " AND CODIGOPRD='"+constraints[i].initialValue+"'"	
					
				}
				
			}
			
			if (constraints[i].fieldName == "ALOCACAO") {
            	
				if(!(constraints[i].initialValue=="")){
					
					alocacao = "'"+constraints[i].initialValue+"'"	
					
				}
            	
            }

            if (constraints[i].fieldName == "CODSTATUS") {
            	
            	if(!(constraints[i].initialValue=="")){
            		
            		status = " AND CODSTATUS='"+constraints[i].initialValue+"'"	
            		
            	}
            	
            }
            
            if (constraints[i].fieldName == "CODPOSTO") {
            	
            	if(!(constraints[i].initialValue=="")){
            		
            		codPosto = " AND CODPOSTO='"+constraints[i].initialValue+"'"	
            		
            	}
            	
            }
            
			if (constraints[i].fieldName == "DTHRINICIALPREV") {
			            	
				if(!(constraints[i].initialValue=="")){
					
					var dataDe = constraints[i].initialValue.split(";")[0]
					var dataAte = constraints[i].initialValue.split(";")[1]
					
					//dataInicioPrev = " AND ( (CAST(INICIO_PLANEJADO AS DATE) BETWEEN '"+dataDe+"' AND '"+dataAte+"') OR (CAST(FINAL_PLANEJADO AS DATE) BETWEEN '"+dataDe+"' AND '"+dataAte+"'))"
					
					//dataInicioPrev = " AND ( ('"+dataDe+"' BETWEEN CAST(INICIO_PLANEJADO AS DATE) AND CAST(FINAL_PLANEJADO AS DATE)) OR ('"+dataAte+"' BETWEEN CAST(INICIO_PLANEJADO AS DATE) AND CAST(FINAL_PLANEJADO AS DATE) )) "
					
					//dataInicioPrev = " AND ( (CAST(I.DTHRINICIALPREV AS DATE) BETWEEN '"+constraints[i].initialValue+"'"	
					
					dataInicioPrev = " AND ( ( ('"+dataDe+"' BETWEEN CAST(INICIO_PLANEJADO AS DATE) AND CAST(FINAL_PLANEJADO AS DATE) ) OR ('"+dataAte+"' BETWEEN CAST(INICIO_PLANEJADO AS DATE) AND CAST(FINAL_PLANEJADO AS DATE) ) ) OR ( (CAST(INICIO_PLANEJADO AS DATE)  BETWEEN '"+dataDe+"' AND '"+dataAte+"') OR (CAST(FINAL_PLANEJADO AS DATE)  BETWEEN '"+dataDe+"' AND '"+dataAte+"') ) )"
					
				}
				
            }
			
			/*if (constraints[i].fieldName == "DTHRFINALPREV") {
				
				if(!(constraints[i].initialValue=="")){
					
					dataFinalPrev = " AND CAST(I.DTHRFINALPREV AS DATE)<='"+constraints[i].initialValue+"'"	
					
				}
				
			}*/
            
            /*if (constraints[i].fieldName == "DTHRINICIALPREV") {
            	
				if(!(constraints[i].initialValue=="")){
					
					dataInicioPrev = " AND '"+constraints[i].initialValue+"' BETWEEN CAST(I.DTHRINICIALPREV AS DATE) AND CAST(I.DTHRFINALPREV AS DATE) "	
					
				}
				
            }*/
            
            if (constraints[i].fieldName == "ORDERBY") {
            	
            	if(!(constraints[i].initialValue=="")){
            		
            		orderBy = "ORDER BY "+constraints[i].initialValue	
            		
            	} else {
            		
            		orderBy = "ORDER BY OS, PRIORIDADE"
            		
            	}
            	
            }
            
            if (constraints[i].fieldName == "FOLGADE") {
            	
            	if(!(constraints[i].initialValue=="")){
            		
            		folgaDe = " AND FOLGA>='"+constraints[i].initialValue+"'"	
            		
            	}
            	
            }
            
            if (constraints[i].fieldName == "FOLGAATE") {
            	
            	if(!(constraints[i].initialValue=="")){
            		
            		folgaAte = " AND FOLGA<='"+constraints[i].initialValue+"'"	
            		
            	}
            	
            }
            
            if (constraints[i].fieldName == "NIVELDE") {
            	
            	if(!(constraints[i].initialValue=="")){
            		
            		nivelDe = " AND LEN(REPLACE(INDICE_FLUIG, '.', ''))>='"+constraints[i].initialValue+"'"	
            		
            	}
            	
            }
            
            if (constraints[i].fieldName == "NIVELATE") {
            	
            	if(!(constraints[i].initialValue=="")){
            		
            		nivelAte = " AND LEN(REPLACE(INDICE_FLUIG, '.', ''))<='"+constraints[i].initialValue+"'"	
            		
            	}
            	
            }
            
        }
        
    }
    
    log.info("Vou construir a query")	
    
    var myQuery = 	"SELECT  CELULA, TAG, OS, IDPRJ, OP, "+
					"	CODCOLIGADA, CODFILIAL, CODESTRUTURA, "+
					"	CODIGOPRD, IDPRD, "+
					"	EXECUCAO, "+
					"	DSCITEM, CODATIVIDADE, DSCATIVIDADE, REPROCESSAMENTO,  	"+
					"	STATUS, STATUS_ATV, "+
					"	PRIORIDADE, CODPOSTO, DSCPOSTO, CODSTATUS, STATUS_OP, CARGA_PREV, NUMFLUIG, 	"+	
					"	INDICE_FLUIG, NIVEL_FLUIG,  		"+
					"	INICIO_PLANEJADO, FINAL_PLANEJADO, FOLGA, IDTRF, IDATVORDEM, AVANCO_REALIZADO, "+
					"	CODHABILIDADE, "+
					"	REC_PREV, "+
					"	NUMPLANOCORTE, "+
					"	ALOCADO, "+
					"	APONTADO, "+
					"	PARALISADO, "+
					"	FAROL "+
				 	"FROM V_CRM_CONSULTAPRINCIPAL "+
				 	"WHERE " +
				 	" CODCOLIGADA=1 AND "+
				 	" STATUS<>6  AND CODSTATUS<>6 AND "+
				 	" "+filial+" "+celula+" "+numOS+" "+codOrdem+" "+codAtividade+" "+idprd+" "+codigoPrd+" "+status+" "+codPosto+" "+dataInicioPrev+" "+dataFinalPrev+" "+folgaDe+" "+folgaAte+" "+inferior+" "+nivelDe+" "+nivelAte+" "+
    				" "+orderBy+" "
    
    
    
    log.info("QUERY dsPlanejamentoGeralOS: " + myQuery);
    
    try {
    	
        var conn = ds.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery(myQuery);
        var columnCount = rs.getMetaData().getColumnCount();
        
        while (rs.next()) {
        	
            if (!created) {
            	
                for (var i = 1; i <= columnCount; i++) {
                	
                	dsPlanejamentoGeralOS.addColumn(rs.getMetaData().getColumnName(i));
                	
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
            
            dsPlanejamentoGeralOS.addRow(Arr);
            
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
    
    return dsPlanejamentoGeralOS;
	
}