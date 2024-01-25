// VERIFICA SE EXISTE ALGUMA FALHA DE INDEXAÇÃO NA ESTRUTURA
function createDataset(fields, constraints, sortFields) {

	var dsNewDataset = DatasetBuilder.newDataset();
    var dataSource = "/jdbc/FluigDS";
    var ic = new javax.naming.InitialContext();
    var ds = ic.lookup(dataSource);
    var created = false;
   
    var numOS = ""
	
    if (constraints != null) {
    	
        for (var i = 0; i < constraints.length; i++) {
        	
        	if(constraints[i].fieldName == "OS"){
        		
        		numOS = constraints[i].initialValue;
        	
        	} 
        	
        }
        
    }  

    log.info("VOU MONTAR A QUERY")
    
    var myQuery =   "SELECT "+
					"	M.INDICE, M.IDCRIACAO, (SELECT COUNT(*) FROM Z_CRM_ML001005 WHERE OS=M.OS AND INDICE=M.INDICE AND ITEMEXCLUSIVO!=2) REPETICOES "+
					"FROM "+
					"	Z_CRM_ML001005 M "+
					"WHERE "+
					"	M.OS='"+numOS+"' AND (SELECT COUNT(*) FROM Z_CRM_ML001005 WHERE OS=M.OS AND INDICE=M.INDICE AND ITEMEXCLUSIVO!=2) > 1 AND M.ITEMEXCLUSIVO!=2 "
					    				  
    log.info("QUERY dsVerificaFalhaIndexacao: " + myQuery);
    
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