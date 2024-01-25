function createDataset(fields, constraints, sortFields) {

	var dsNewDataset = DatasetBuilder.newDataset();
    var dataSource = "/jdbc/FluigDS";
    var ic = new javax.naming.InitialContext();
    var ds = ic.lookup(dataSource);
    var created = false;
    
    var numOS = "";
    
    if (constraints != null) {
    	
        for (var i = 0; i < constraints.length; i++) {
        	
        	if(constraints[i].fieldName == "OS"){
        		
        		numOS = constraints[i].initialValue;
        		
        	} 
        	
        }
        
    }  
    
    var myQuery =   "SELECT "+
					"	P.*, M.DIAMETROEXTERNODISCO, M.DIAMETROINTERNODISCO,M.OBSGERAL   "+
					"FROM "+
					"	ML001015 P "+
					"	INNER JOIN Z_CRM_ML001005 M ON P.NUMOSSALVOS=M.OS AND P.IDCRIACAOSALVOS=M.IDCRIACAO "+
					"WHERE "+
					"	NUMOSSALVOS='"+numOS+"' "+
					"	AND P.masterid = (SELECT MAX(masterid) FROM ML001015 WHERE NUMOSSALVOS='"+numOS+"') AND M.ITEMEXCLUSIVO!=2 "		
    
    //var myQuery = "SELECT * FROM ML001015 WHERE NUMOSSALVOS='"+numOS+"' AND masterid = (SELECT MAX(masterid) FROM ML001015 WHERE NUMOSSALVOS='"+numOS+"') "
        
    log.info("QUERY dsBuscaMateriaisSalvos: " + myQuery);
    
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