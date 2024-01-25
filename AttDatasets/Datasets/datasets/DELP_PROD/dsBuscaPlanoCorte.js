// BUSCA AS INFORMAÇÕES DAS ATIVIDADES QUE PODEM SER CADASTRADAS EM UM PLANO DE CORTE DE UM DETERMINADO PROJETO
function createDataset(fields, constraints, sortFields) {

	var dsBuscaPlanoCorte = DatasetBuilder.newDataset();
    var dataSource = "/jdbc/FluigDSRM";
    var ic = new javax.naming.InitialContext();
    var ds = ic.lookup(dataSource);
    var created = false;
    
    log.info("entrei no dataset dsBuscaPlanoCorte")
    
    var os = ""
	var material = ""
	var codAtividade = ""
	var numDesenho = ""
	var clausulaAtv = " AND ( KA.DSCATIVIDADE LIKE '%CORTAR MANUAL%' OR KA.DSCATIVIDADE LIKE '%SERRAR%' OR KA.DSCATIVIDADE LIKE '%CORTAR%'  OR KA.DSCATIVIDADE LIKE '%TRAÇAR%' OR KA.DSCATIVIDADE LIKE '%CHANFRAR%') "
	var dscAtividade = ""
	var filial = ""
	var order = ""
			
    if (constraints != null) {
    	
        for (var i = 0; i < constraints.length; i++) {
        	
        	if (constraints[i].fieldName == "OS") {
            	
        		if(!(constraints[i].initialValue=="")){
        			
        			os = " AND  O.CODCCUSTO='"+constraints[i].initialValue+"' "
        			        			
        		}
        		
            }
        	if (constraints[i].fieldName == "MATERIAL") {
            	
        		if(!(constraints[i].initialValue=="")){
        			
        			material = " AND N.MATERIAL='"+constraints[i].initialValue+"' "
        			        			
        		}
        		
            }
        	if (constraints[i].fieldName == "CODFILIAL") {
            	
        		if(!(constraints[i].initialValue=="")){
        			
        			filial = " AND O.CODFILIAL='"+constraints[i].initialValue+"' "
        			        			
        		}
        		
            }
        	if (constraints[i].fieldName == "CODATIVIDADE") {
            	
        		if(!(constraints[i].initialValue=="")){
        			
        			codAtividade = " AND KA.DSCATIVIDADE LIKE '%"+constraints[i].initialValue+"%' "
        			        			
        		}
        		
            }
        	if (constraints[i].fieldName == "DSCATIVIDADE") {
            	
        		if(!(constraints[i].initialValue=="")){
        			
        			dscAtividade = constraints[i].initialValue
        			
        			// SE A ATIVIDADE FOR A DE CORTAR MANUAL
        			if(dscAtividade=="CORTAR MANUAL"){
        				
        				codAtividade = " AND (KA.CODATIVIDADE LIKE '%WATV031%' OR KA.CODATIVIDADE LIKE '%WATV022%' ) "
            			clausulaAtv = " AND ( KA.DSCATIVIDADE LIKE '%CORTAR MANUAL%' OR KA.DSCATIVIDADE LIKE '%SERRAR%' OR KA.DSCATIVIDADE LIKE '%CORTAR%' OR KA.DSCATIVIDADE LIKE '%TRAÇAR%') "
            			
        			} 
        			
        		}
        		
            }
        	if (constraints[i].fieldName == "NUMDESENHO") {
            	
        		if(!(constraints[i].initialValue=="")){
        			
        			numDesenho = " AND N.NUMDESENHO='"+constraints[i].initialValue+"' "
        			        		
        		}
        		
            }
        	if (constraints[i].fieldName == "ORDER") {
            	
        		log.info("tem order preenchido")
        		
        		if(!(constraints[i].initialValue=="")){
        			
        			if(constraints[i].initialValue=="OP"){
        			
        				log.info("order é OP")
        				
        				order = " ORDER BY OP "	
        				
        			}
        			if(constraints[i].initialValue=="NUMDESENHO"){
        			
        				log.info("order é NUMDESENHO")
        				
        				order = " ORDER BY NUMDESENHO "
        				
        			}
        			        		
        		}
        		
            }
        	      	
        }
        
	}
    
    var myQuery =   " SELECT * FROM (SELECT O.CODCOLIGADA,O.CODCCUSTO OS, "+
					" O.CODORDEM OP, "+
					" KA.CODATIVIDADE, "+
					" KA.DSCATIVIDADE, "+
					" N.DESCRICAO ITEM, "+
					" N.MATERIAL, "+
					" KC.NUMEXEC EXECUCAO, "+
					" N.POSICAODESENHO POSICAO, "+
					" N.BITOLA, "+
					" A.IDATVORDEM, "+
					" A.CODESTRUTURA, "+
					" N.NUMDESENHO, "+
					" A.QTPREVISTA QTDEPLANEJADA, "+
					" (SELECT SALDOS.QTDEPREV - CASE "+
					" WHEN COALESCE(SALDOS.QTDE,0) = 0 THEN "+
					" SALDOS.QUANTIDADE "+
					" ELSE SALDOS.QTDE "+
					" END AS QTD "+
					" FROM   (SELECT KA.QTPREVISTA  QTDEPREV, "+
					" (SELECT Sum(QUANTIDADE) QTDE "+
					" FROM   ZMDPLANOAPROVEITAMENTOCORTE "+
					" WHERE  CODCOLIGADA = KA.CODCOLIGADA "+
					" AND CODFILIAL = KA.CODFILIAL "+
					" AND CODORDEM = KA.CODORDEM "+
					" AND CODESTRUTURA = KA.CODESTRUTURA "+
					" AND CODATIVIDADE = KA.CODATIVIDADE) QTDE, "+
					" KA.QUANTIDADE "+
					" FROM    KATVORDEM KA "+
					" WHERE  KA.CODCOLIGADA = A.CODCOLIGADA "+
					" AND KA.CODFILIAL = A.CODFILIAL "+
					" AND KA.CODORDEM = A.CODORDEM "+
					" AND KA.CODESTRUTURA = A.CODESTRUTURA "+
					" AND KA.CODATIVIDADE = A.CODATIVIDADE) SALDOS) SALDO FROM KORDEM O "+
					" INNER JOIN KORDEMCOMPL KC "+
					" ON O.CODCOLIGADA = KC.CODCOLIGADA "+
					" AND O.CODFILIAL = KC.CODFILIAL "+
					" AND O.CODORDEM = KC.CODORDEM "+
					" INNER JOIN KATVORDEM A "+
					" ON A.CODCOLIGADA = O.CODCOLIGADA "+
					" AND A.CODFILIAL = O.CODFILIAL "+
					" AND A.CODORDEM = O.CODORDEM "+
					" INNER JOIN KATIVIDADE KA "+
					" ON A.CODCOLIGADA = KA.CODCOLIGADA "+
					" AND A.CODFILIAL = KA.CODFILIAL "+
					" AND A.CODATIVIDADE = KA.CODATIVIDADE "+
					" INNER JOIN FLUIG.DBO.Z_CRM_EX001005 N "+
					" ON N.OS = O.CODCCUSTO COLLATE LATIN1_GENERAL_CI_AS "+
					" AND N.CODIGOPRD = A.CODESTRUTURA COLLATE LATIN1_GENERAL_CI_AS "+
					" AND N.EXECUCAO = KC.NUMEXEC "+
					" AND N.ITEMEXCLUSIVO <> 2 "+
					"WHERE	"+
					"	O.CODCOLIGADA=1 "+filial+" "+os+" "+
					"	AND	O.CODCCUSTO IS NOT NULL "+
					"	AND	O.STATUS NOT IN (4,5,6) "+
					"   AND A.STATUS NOT IN (4,5,6)   "+
					"	"+clausulaAtv+" "+
					//"	AND	C.POSICAO <> '0' "+
					" "+material+" "+codAtividade+" "+numDesenho+" ) R WHERE SALDO > 0 "+order
    

	    
    log.info("QUERY dsBuscaPlanoCorte: " + myQuery);
    
    try {
    	
        var conn = ds.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery(myQuery);
        var columnCount = rs.getMetaData().getColumnCount();
        
        while (rs.next()) {
        	
            if (!created) {
            	
                for (var i = 1; i <= columnCount; i++) {
                	
                	dsBuscaPlanoCorte.addColumn(rs.getMetaData().getColumnName(i));
                	
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
            
            dsBuscaPlanoCorte.addRow(Arr);
            
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
    
    return dsBuscaPlanoCorte;
	
}