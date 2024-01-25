function createDataset(fields, constraints, sortFields) {

	var dsBuscaAtividade = DatasetBuilder.newDataset();
    var dataSource = "/jdbc/FluigDS";
    var ic = new javax.naming.InitialContext();
    var ds = ic.lookup(dataSource);
    var created = false;
    
    var atividade = "";
    var desenho = "";	
    var posicao = "";
    var descricao = "";
    var os = "";
    var alvo = "";
    var exec = "";
    var codtrf = "";
    var codtrfWHERE = "";
    var versao = "";
    var where = "";
    
    if (constraints != null) {
    	
        for (var i = 0; i < constraints.length; i++) {
        	
        	if(constraints[i].fieldName == "CODATIVIDADE"){
        		
        		atividade = constraints[i].initialValue;
        		
        	} 
        	if(constraints[i].fieldName == "DESENHO"){
        		
        		desenho = constraints[i].initialValue;
        		
        	} 
        	if(constraints[i].fieldName == "POSICAO"){
        		
        		posicao = constraints[i].initialValue;
        		
        	} 
        	if(constraints[i].fieldName == "DESCRICAO"){
        		
        		descricao = constraints[i].initialValue;
        		
        	} 
        	if(constraints[i].fieldName == "OS"){
        		
        		os = constraints[i].initialValue;
        		
        	} 
        	if(constraints[i].fieldName == "ALVO"){
        		
        		alvo = constraints[i].initialValue;
        		
        	} 
        	if(constraints[i].fieldName == "EXECUCAO"){
        		
        		exec = constraints[i].initialValue;
        		
        	} 
        	if(constraints[i].fieldName == "CODTRFPAI"){
        		

        		if(codtrf!="0"){
        			codtrfWHERE = " AND Z.INDICE LIKE CONCAT((SELECT INDICE FROM Z_CRM_ML001005 WHERE OS='"+os+"' AND CODTRFPAI='"+constraints[i].initialValue+"' AND ITEMEXCLUSIVO!=2 ),'%') ";
        			codtrf = " AND Z.CODTRFPAI = '"+constraints[i].initialValue+"'"
        		}
        		
        	} 
        	if(constraints[i].fieldName == "VERSAO"){
        		
        		versao = constraints[i].initialValue;
        		
        	} 
        	if(constraints[i].fieldName == "WHERE"){
        		
        		where += " "+constraints[i].initialValue+" ";
        		
        	} 
        	
        }
        
    }  
    
    var myQuery = "";
    
    	
	if( alvo == "ML"){
    	
    	myQuery = "SELECT Z1.PRIORIDADE,Z.INDICE,Z1.CODATIVIDADE,Z1.DESCATIVIDADE FROM Z_CRM_ML001005 Z  "+
				"	INNER JOIN Z_CRM_ML001021 Z1 ON "+
				"	Z.IDCRIACAO=Z1.IDCRIACAOPROCESSO "+
				"	AND Z.OS=Z1.OSPROCESSO "+
				"	WHERE ITEMEXCLUSIVO!=2 AND Z.OS='"+os+"' AND Z1.CODATIVIDADE='"+atividade+"' "+where+
				codtrfWHERE
    	
    }
    else if(alvo == "EX"){
    	
    	myQuery = "SELECT Z1.PRIORIDADE,Z.INDICE,Z1.CODATIVIDADE,Z1.DESCATIVIDADE,Z.EXECUCAO FROM Z_CRM_EX001005 Z  "+
				"	INNER JOIN Z_CRM_EX001021 Z1 ON "+
				"	Z.IDCRIACAO=Z1.IDCRIACAOPROCESSO "+
				"	AND Z.OS=Z1.OSPROCESSO "+
				"	AND Z.EXECUCAO=Z1.EXECUCAO "+
				"	WHERE Z.OS='"+os+"' " +where+
				"	AND Z1.CODATIVIDADE='"+atividade+"' AND Z.ITEMEXCLUSIVO!=2 AND Z.EXECUCAO IN ("+exec+") "+codtrf
    	
    }
    			
   
    log.info("QUERY dsBuscaAtividade: " + myQuery);
    
    try {
    	
        var conn = ds.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery(myQuery);
        var columnCount = rs.getMetaData().getColumnCount();
        
        while (rs.next()) {
        	
            if (!created) {
            	
                for (var i = 1; i <= columnCount; i++) {
                	
                	dsBuscaAtividade.addColumn(rs.getMetaData().getColumnName(i));
                	
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
            
            dsBuscaAtividade.addRow(Arr);
            
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
    
    return dsBuscaAtividade;
	
}