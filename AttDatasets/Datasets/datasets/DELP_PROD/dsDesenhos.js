// BUSCA TODOS OS DESENHOS CADASTRADOS
function createDataset(fields, constraints, sortFields) {

	var dsDesenhos = DatasetBuilder.newDataset();
    var dataSource = "/jdbc/FluigDS";
    var ic = new javax.naming.InitialContext();
    var ds = ic.lookup(dataSource);
    var created = false;
    
    var numDesenho = "";
    var os = ""
    
    if (constraints != null) {
    	
        for (var i = 0; i < constraints.length; i++) {
        	
        	if(constraints[i].fieldName == "NUMDESENHO"){
        		
        		numDesenho = constraints[i].initialValue;
        		
        	} 
        	if(constraints[i].fieldName == "OS"){
        		
        		os = constraints[i].initialValue;
        		
        	} 
        	
        }
        
    }  
    
   var myQuery = "SELECT NUMDESENHO FROM Z_CRM_EX001005 WHERE ITEMEXCLUSIVO <> 2 AND OS LIKE '"+os+"%' " +
   					"	UNION " +
   					"SELECT NUMDESENHO FROM Z_CRM_ML001005 WHERE  ITEMEXCLUSIVO!=2 AND OS LIKE '"+os+"%' " +
   					"ORDER BY NUMDESENHO DESC"; 
    
    log.info("QUERY dsDesenhos: " + myQuery);
    
    try {
    	
        var conn = ds.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery(myQuery);
        var columnCount = rs.getMetaData().getColumnCount();
        
        while (rs.next()) {
        	
            if (!created) {
            	
                for (var i = 1; i <= columnCount; i++) {
                	
                	dsDesenhos.addColumn(rs.getMetaData().getColumnName(i));
                	
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
            
            dsDesenhos.addRow(Arr);
            
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
    
    return dsDesenhos;
	
}