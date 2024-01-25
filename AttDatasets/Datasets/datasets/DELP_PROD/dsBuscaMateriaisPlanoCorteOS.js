// BUSCA TODOS OS MATERIAIS CADASTRADOS EM UMA DETERMINADA ESTRUTURA
function createDataset(fields, constraints, sortFields) {

	var dsBuscaMateriaisPlanoCorteOS = DatasetBuilder.newDataset();
    var dataSource = "/jdbc/FluigDS";
    //var dataSource = "/jdbc/FluigDSRM";
    var ic = new javax.naming.InitialContext();
    var ds = ic.lookup(dataSource);
    var created = false;
    
    var os = ""
    
    if (constraints != null) {
    	
        for (var i = 0; i < constraints.length; i++) {
        	
        	if (constraints[i].fieldName == "OS") {
            	
        		os = " AND  OS='"+constraints[i].initialValue+"'"
            	
            }
        	     	
        }
        
	}
    
    var myQuery =   "SELECT "+
					"	DISTINCT MATERIAL "+ 
					"FROM "+
					"	Z_CRM_ML001005 "+
					"WHERE "+
					"	MATERIAL<>'' " +
					"	AND MATERIAL IS NOT NULL AND ITEMEXCLUSIVO!=2 "+
					" "+os+" "
	    
    log.info("QUERY dsBuscaMateriaisPlanoCorteOS: " + myQuery);
    
    try {
    	
        var conn = ds.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery(myQuery);
        var columnCount = rs.getMetaData().getColumnCount();
        
        while (rs.next()) {
        	
            if (!created) {
            	
                for (var i = 1; i <= columnCount; i++) {
                	
                	dsBuscaMateriaisPlanoCorteOS.addColumn(rs.getMetaData().getColumnName(i));
                	
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
            
            dsBuscaMateriaisPlanoCorteOS.addRow(Arr);
            
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
    
    return dsBuscaMateriaisPlanoCorteOS;
	
}