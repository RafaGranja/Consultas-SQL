// BUSCA TODOS OS INDICES DA ESTRUTURA DE UM DETERMINADO PROJETO (OS)
function createDataset(fields, constraints, sortFields) {

	var dsNewDataset = DatasetBuilder.newDataset();
    var dataSource = "/jdbc/FluigDS";
    var ic = new javax.naming.InitialContext();
    var ds = ic.lookup(dataSource);
    var created = false;
    
    var numOS = ""
    var indice = ""
    var idCriacao = ""
    var notIndice = ""
    var indicePai = ""
    var nivel = " AND (M.NIVEL IS NOT NULL AND M.NIVEL<>'') "
    
    log.info("entrei no dataset dsIndicesEstrutura")
    	
    if (constraints != null) {
    	
        for (var i = 0; i < constraints.length; i++) {
        	
        	log.info("Fild Name: "+constraints[i].fieldName)
        	
        	if(constraints[i].fieldName == "OS"){
        		
        		numOS = constraints[i].initialValue;
        		
        	}
        	if(constraints[i].fieldName == "INDICE"){
        		
        		indice = " AND M.INDICE LIKE '"+constraints[i].initialValue+"%' ";
        		
        	}
        	if(constraints[i].fieldName == "INDICEPAI"){
        		
        		indicePai = " AND M.INDICE <> '"+constraints[i].initialValue+"' ";
        		
        	}
        	if(constraints[i].fieldName == "NOTINDICE"){
        		
        		var ind = constraints[i].initialValue.substring(0,constraints[i].initialValue.indexOf(".")-1)
        		notIndice = " AND M.INDICE NOT LIKE '"+constraints[i].initialValue+"%' AND M.INDICE LIKE '"+ind+"%' ";
        		
        	}
        	if(constraints[i].fieldName == "IDCRIACAO"){
        		
        		idCriacao = constraints[i].initialValue;
        		
        	}
        	if(constraints[i].fieldName == "NIVEL"){
        		
        		nivel = " AND (M.NIVEL IS NULL OR M.NIVEL LIKE '') "
        		
        	}
        	
        }
        
    }  
    
    // SE IDCRIACAO FOI INFORMADO
    if(idCriacao==""){
    	
    	 myQuery =    "SELECT " +
					  "		M.INDICE " +
					  "FROM " +
					  "		Z_CRM_ML001005 M " +
					  "WHERE " +
					  "		M.ITEMEXCLUSIVO!=2 AND M.OS='"+numOS+"' " +indice+" "+nivel+" "+notIndice+" "+indicePai+" "+
				  	  "ORDER BY " +
				  	  "		CAST('/' + REPLACE(M.INDICE, '.', '/') + '/' AS HIERARCHYID) "
	
    } else {
    	// SE NÃO
    	
    	myQuery =   "SELECT "+
					"	M.INDICE "+
					"FROM "+
					"	Z_CRM_ML001005 M "+
					"	RIGHT JOIN Z_CRM_ML001005 ML ON M.OS = ML.OS "+
					"WHERE "+
					"	M.OS='"+numOS+"' AND M.ITEMEXCLUSIVO!=2 AND ML.ITEMEXCLUSIVO!=2 "+
					"	AND (M.INDICE LIKE ML.INDICE OR M.INDICE LIKE ML.INDICE+'.%' OR (M.INDICE LIKE 'X%' AND M.INDICEANTIGO LIKE ML.INDICE+'.%')) " +
					"	AND ML.IDCRIACAO = '"+idCriacao+"' "+indice+" "+nivel+" "+notIndice+" "+indicePai+" "+
					"ORDER BY "+
					"    CAST('/' + REPLACE(M.INDICE, '.', '/') + '/' AS HIERARCHYID) "

    }
        
    log.info("QUERY dsIndicesEstrutura: " + myQuery);
    
    try {
    	
        var conn = ds.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery(myQuery);
        var columnCount = rs.getMetaData().getColumnCount();
        
        while (rs.next()) {
        	
            if (!created) {
            	
                for (var i = 1; i <= columnCount; i++) {
                	
                	dsNewDataset.addColumn(rs.getMetaData().getColumnName(i));
                	
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
            
            dsNewDataset.addRow(Arr);
            
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
    
    return dsNewDataset;
	
}