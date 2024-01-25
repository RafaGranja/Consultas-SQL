// VERIFICA SE UM DETERMINADO ITEM DA ESTRUTURA TEM FILHOS PELO IDCRIACAO
function createDataset(fields, constraints, sortFields) {

	var dsItemTemFilhoOS = DatasetBuilder.newDataset();
    var dataSource = "/jdbc/FluigDS";
    var ic = new javax.naming.InitialContext();
    var ds = ic.lookup(dataSource);
    var created = false;
   
    var idCriacao = ""
    var nivel = ""
	
    if (constraints != null) {
    	
        for (var i = 0; i < constraints.length; i++) {
        	
        	if(constraints[i].fieldName == "OS"){
        		
        		numOS = constraints[i].initialValue;
        	
        	} else if(constraints[i].fieldName == "IDCRIACAO"){
        		
        		idCriacao = constraints[i].initialValue;
        	
        	} 
        	
        }
        
    }  

    log.info("VOU MONTAR A QUERY")
    
    var myQuery = "SELECT "+
    			  "		M.INDICE, M.NIVEL, M.ID, M.OS, M.IDCRIACAO "+
    			  "FROM "+ 
    			  "		Z_CRM_ML001005 M "+
    			  "		RIGHT JOIN Z_CRM_ML001005 ML ON M.OS = ML.OS "+
    			  "WHERE "+
    			  "		M.INDICE LIKE ML.INDICE+'.%' AND M.OS='"+numOS+"' AND ML.IDCRIACAO = '"+idCriacao+"' AND M.ITEMEXCLUSIVO!=2 AND ML.ITEMEXCLUSIVO!=2 "
    
				  
    log.info("QUERY dsItemTemFilhoOS: " + myQuery);
    
    try {
    	
        var conn = ds.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery(myQuery);
        var columnCount = rs.getMetaData().getColumnCount();
        
        while (rs.next()) {
        	
            if (!created) {
            	
                for (var i = 1; i <= columnCount; i++) {
                	
                	dsItemTemFilhoOS.addColumn(rs.getMetaData().getColumnName(i));
                	
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
            
            dsItemTemFilhoOS.addRow(Arr);
            
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
    
    return dsItemTemFilhoOS;
	
}