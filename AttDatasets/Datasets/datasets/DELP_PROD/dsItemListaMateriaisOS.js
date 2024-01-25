function createDataset(fields, constraints, sortFields) {

	var dsNewDataset = DatasetBuilder.newDataset();
    var dataSource = "/jdbc/FluigDS";
    var ic = new javax.naming.InitialContext();
    var ds = ic.lookup(dataSource);
    var created = false;
    
    var numOS = "";
    var posicao = "";
    var desenho = "";
    var descricao = "";
    
    if (constraints != null) {
    	
        for (var i = 0; i < constraints.length; i++) {
        	
        	if(constraints[i].fieldName == "OS"){
        		
        		numOS = constraints[i].initialValue;
        		
        	} 
        	if(constraints[i].fieldName == "POSICAO"){
        		
        		posicao = constraints[i].initialValue;
        		
        	} 
        	if(constraints[i].fieldName == "DESENHO"){
        		
        		desenho = constraints[i].initialValue;
        		
        	} 
        	if(constraints[i].fieldName == "DESCRICAO"){
        		
        		descricao = constraints[i].initialValue;
        		
        	}
        	
        }
        
    }  
    
    var myQuery =   " SELECT L.*,Z.PESOUNITARIO FROM Z_CRM_LISTAMATERIAIS L "+ 
    				" INNER JOIN Z_CRM_ML001005 Z ON " +
					" Z.IDCRIACAO=L.IDCRIACAOSALVOS " +
					" AND Z.OS=L.NUMOSSALVOS " + 
					" WHERE NUMOSSALVOS='"+numOS+"' " +
    				" AND NUMDESENHOSALVOS='"+desenho+"' AND POSICAOSALVOS='"+posicao+"' AND DESCRICAOSALVOS='"+descricao+"' AND Z.ITEMEXCLUSIVO!=2 "
   
    log.info("QUERY dsItemListaMateriaisOS: " + myQuery);
    
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