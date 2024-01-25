// BUSCA AS INFORMAÇÕES DAS ATIVIDADES QUE PODEM SER EDITADOS EM UM PLANO DE CORTE DE UM DETERMINADO PROJETO
function createDataset(fields, constraints, sortFields) {

	var dsBuscaPlanoCorteEdicao = DatasetBuilder.newDataset();
    var dataSource = "/jdbc/FluigDSRM";
    //var dataSource = "/jdbc/FluigDSRM";
    var ic = new javax.naming.InitialContext();
    var ds = ic.lookup(dataSource);
    var created = false;
    
    log.info("entrei no dataset dsBuscaPlanoCorteEdicao")
    
    var os = ""
	var material = ""
	var numPlano = ""
	var codAtividade = ""
	var dscAtividade = ""
	var codFilial = ""
	//var clausulaAtv = ""
	var clausulaAtv = "	AND ( KA.DSCATIVIDADE LIKE '%CORTAR MANUAL%' OR KA.DSCATIVIDADE LIKE '%SERRAR%' OR KA.DSCATIVIDADE LIKE '%CORTAR%' OR KA.DSCATIVIDADE LIKE '%TRAÇAR%' OR KA.DSCATIVIDADE LIKE '%CHANFRAR%') "
    var order = ""
		
    if (constraints != null) {
    	
        for (var i = 0; i < constraints.length; i++) {
        	
        	if (constraints[i].fieldName == "OS") {
        		
        		if(!(constraints[i].initialValue=="")){
        			
        			os = constraints[i].initialValue
        			
        		}
        		
            }
        	if (constraints[i].fieldName == "MATERIAL") {
            	
        		if(!(constraints[i].initialValue=="")){
        			
        			material = " AND N.MATERIAL='"+constraints[i].initialValue+"'"
        			
        		}
        		
            }
        	/*if (constraints[i].fieldName == "CODATIVIDADE") {
            	
        		if(!(constraints[i].initialValue=="")){
        			
        			codAtividade = " AND KA.CODATIVIDADE='"+constraints[i].initialValue+"'"
        			
        		}
        		
            }
        	if (constraints[i].fieldName == "DSCATIVIDADE") {
            	
        		if(!(constraints[i].initialValue=="")){
        			
        			dscAtividade = constraints[i].initialValue
        			
        			// SE A ATIVIDADE FOR A DE CORTAR MANUAL
        			if(dscAtividade=="CORTAR MANUAL"){
        				
        				//codAtividade = " AND (KA.CODATIVIDADE LIKE '%WATV031%' OR KA.CODATIVIDADE LIKE '%WATV022%') "
            			clausulaAtv = " AND (KA.CODATIVIDADE LIKE '%WATV031%' OR KA.CODATIVIDADE LIKE '%WATV022%') AND ( KA.DSCATIVIDADE LIKE '%CORTAR MANUAL%' OR KA.DSCATIVIDADE LIKE '%SERRAR%' OR KA.DSCATIVIDADE LIKE '%OXICORTAR%' OR KA.DSCATIVIDADE LIKE '%CORTAR A PLASMA%' OR KA.DSCATIVIDADE LIKE '%TRAÇAR%') "
            			dscAtividade = ""
            				
        			} else {
        				
        				dscAtividade = " AND KA.DSCATIVIDADE LIKE '%"+dscAtividade+"%' "
        				
        			}
        			
        		}
        		
            }*/
        	if (constraints[i].fieldName == "NUMPLANOCORTE") {
            	
        		numPlano = constraints[i].initialValue
            	
            }
        	if (constraints[i].fieldName == "CODFILIAL") {
            	
        		codFilial = " AND O.CODFILIAL ="+constraints[i].initialValue+" "
            	
            }
        	if (constraints[i].fieldName == "ORDER") {
            	
        		log.info("order foi preenchido")
        		
        		if(!(constraints[i].initialValue=="")){
        			
        			if(constraints[i].initialValue=="OP"){
        			
        				log.info("order é NUMDESENHO")
        				
        				order = " , O.CODORDEM "	
        				
        			}
        			if(constraints[i].initialValue=="NUMDESENHO"){
        			
        				log.info("order é NUMDESENHO")
        				
        				order = " , N.NUMDESENHO "
        				
        			}
        			        		
        		}
        		
            }
			        	
        }
        
	}
    
    var myQuery = "SELECT * FROM ( "+
    		"SELECT O.CODCCUSTO OS, "+
    	      " O.CODORDEM   OP, "+
    	      " A.CODESTRUTURA, "+	
    	      " KA.CODATIVIDADE, "+
    	      " KA.DSCATIVIDADE, "+
    	      " N.DESCRICAO  ITEM, "+
    	      " N.MATERIAL, "+
    	     "  N.POSICAODESENHO, "+
    	     "  N.BITOLA, "+
    	     "  A.IDATVORDEM, "+
    	     "  N.CODIGOPRD, "+
    	      " Z.NUMPLANOCORTE, "+
    	      " N.NUMDESENHO, "+
    	      " ZN.RECCREATEDBY, " +
    	      " ZN.RECCREATEDON, "+
    	     "  A.QTPREVISTA QTDEPLANEJADA, "+
             " ISNULL(ZN.QTDATENDIDA - (SELECT SUM(A.QUANTIDADE)"+
			 "  FROM ZMDPLANOAPROVEITAMENTOCORTE A "+
			 "  INNER JOIN FLUIG.DBO.Z_DELP_NECESSIDADEPAPC B "+
			 "   ON A.CODCOLIGADA = B.CODCOLIGADA "+
             "          AND A.CODFILIAL = B.CODFILIAL "+
             "          AND A.CODORDEM = B.CODORDEM "+
             "          AND A.CODATIVIDADE = B.CODATIVIDADE "+
			"			 AND A.NUMPLANOCORTE LIKE CONCAT('%COM O PLANO ',B.LOGNECESSIDADE,'%') "+
			"			 AND A.NUMPLANOCORTE != Z.NUMPLANOCORTE) , ISNULL(Z.QUANTIDADE,0)) QUANTIDADE, "+
    	    " CONVERT(VARCHAR(10),ZN.DATANECESSIDADE,103) DATANECESSIDADE, ZN.PAPC,ZN.PRIORIDADE,ZN.QUANTIDADE QTDORIGINAL,ZN.QTDATENDIDA, " +
    	    " O.CODCOLIGADA,O.CODFILIAL , ZN.STATUS, ZN.COMPLEMENTO, ZN.OBS, CONVERT(VARCHAR(10),ZN.DATAREPROGRAMACAO,103) DATAREPROGRAMACAO, ZN.SEMANANECESSIDADE, ZN.SEMANAREPROGRAMACAO, ZN.PRIORIDADEREPROGRAMACAO, "+
    	    "	ZN.LOGNECESSIDADE, "+
    	     "  KC.NUMEXEC EXECUCAO, "+
    	    "  ISNULL((ZN.QUANTIDADE - ISNULL(ZN.QTDATENDIDA,0)),0) SALDO,N.PESOBRUTO,N.PESOLIQUIDO, ZN.NSEQPEDIDO "+
    "	FROM   KORDEM O "+
    	     "   INNER JOIN KORDEMCOMPL KC "+
    	     "           ON O.CODCOLIGADA = KC.CODCOLIGADA "+
    	     "               AND O.CODFILIAL = KC.CODFILIAL "+
    	     "               AND O.CODORDEM = KC.CODORDEM "+
    	     "   INNER JOIN KATVORDEM A "+
    	     "           ON A.CODCOLIGADA = O.CODCOLIGADA "+
    	     "               AND A.CODFILIAL = O.CODFILIAL "+
    	     "               AND A.CODORDEM = O.CODORDEM "+
    	     "   INNER JOIN KATIVIDADE KA "+
    	     "           ON A.CODCOLIGADA = KA.CODCOLIGADA "+
    	     "               AND A.CODFILIAL = KA.CODFILIAL "+
    	     "               AND A.CODATIVIDADE = KA.CODATIVIDADE "+
    	     "   INNER JOIN FLUIG.DBO.Z_CRM_EX001005 N "+
    	     "           ON N.OS = O.CODCCUSTO COLLATE LATIN1_GENERAL_CI_AS "+
    	     "               AND N.CODIGOPRD = A.CODESTRUTURA COLLATE "+
    	     "                               LATIN1_GENERAL_CI_AS "+
    	     "               AND N.EXECUCAO = KC.NUMEXEC "+
    	     "               AND N.ITEMEXCLUSIVO <> 2 "+
    	     "  LEFT JOIN ZMDPLANOAPROVEITAMENTOCORTE Z "+
	        "      		ON O.CODCOLIGADA = Z.CODCOLIGADA "+
	        "         		AND O.CODFILIAL = Z.CODFILIAL "+
	        "         		AND N.CODIGOPRD = Z.CODESTRUTURA COLLATE SQL_Latin1_General_CP1_CI_AI "+
	        "         		AND A.CODATIVIDADE = Z.CODATIVIDADE "+
	        "         		AND O.CODORDEM = Z.CODORDEM "+ 
	        "   LEFT JOIN FLUIG.DBO.Z_DELP_NECESSIDADEPAPC ZN "+
	        "         	ON ZN.CODCOLIGADA=A.CODCOLIGADA "+
	        "         		AND ZN.CODFILIAL=A.CODFILIAL "+
	        "         		AND ZN.CODORDEM=A.CODORDEM "+
	        "         		AND ZN.CODATIVIDADE=A.CODATIVIDADE "+
				" WHERE	"+
				"	O.CODCOLIGADA=1 "+codFilial+" "+
				"	AND O.RESPONSAVEL IS NOT NULL "+
				"	AND		O.CODCCUSTO IS NOT NULL "+
				" "+clausulaAtv+" "+
				"	AND		O.STATUS NOT IN (4,6) "+
				"   AND 	A.STATUS NOT IN (4,6) "+
				// AND		O.STATUS NOT IN (4,5,6) 
				//"	AND		C.POSICAO <> '0' "+
				"	AND O.CODCCUSTO='"+os+"' "+
				//"	"+dscAtividade+" "+
				//" "+material+" "+codAtividade+" "+
				"	AND ((Z.NUMPLANOCORTE IS NULL "+
				"	OR  Z.NUMPLANOCORTE='"+numPlano+"') OR (NSEQPEDIDO IS NOT NULL AND Z.NUMPLANOCORTE IS NULL ) ) ) R WHERE SALDO>0 OR NUMPLANOCORTE IS NOT NULL "+
				"ORDER BY "+
				"	NUMPLANOCORTE DESC,DATANECESSIDADE  "+order+" "
    
  
	    
    log.info("QUERY dsBuscaPlanoCorteEdicao: " + myQuery);
    
    try {
    	
        var conn = ds.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery(myQuery);
        var columnCount = rs.getMetaData().getColumnCount();
        
        while (rs.next()) {
        	
            if (!created) {
            	
                for (var i = 1; i <= columnCount; i++) {
                	
                	dsBuscaPlanoCorteEdicao.addColumn(rs.getMetaData().getColumnName(i));
                	
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
            
            dsBuscaPlanoCorteEdicao.addRow(Arr);
            
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
    
    return dsBuscaPlanoCorteEdicao;
	
}