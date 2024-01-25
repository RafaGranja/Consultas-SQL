// BUSCA TODAS AS OS's NO PROJETO 4.0
function createDataset(fields, constraints, sortFields) {

	var dsBuscaOS4= DatasetBuilder.newDataset();
    var dataSource = "/jdbc/FluigDS";
    var ic = new javax.naming.InitialContext();
    var ds = ic.lookup(dataSource);
    var created = false;
    
    var codcoligada = ""
    var codfilial = ""
    
if (constraints != null) {
    	
        for (var i = 0; i < constraints.length; i++) {
        	
            if (constraints[i].fieldName == "CODCOLIGADA") {
            	
            	codcoligada = constraints[i].initialValue
            	
            }
            
            if (constraints[i].fieldName == "CODFILIAL") {
            	
            	codfilial =  constraints[i].initialValue
            	
            }
            
        }
        
    }
    
    var myQuery = "SELECT DISTINCT CONCAT(OS,' - ',DESCOS) DESCRICAOOS ,OS, DESCOS, CODCOLIGADA, CODFILIAL FROM Z_CRM_ML001005 WHERE ITEMEXCLUSIVO!=2 AND NIVEL='' AND CODCOLIGADA LIKE '%"+codcoligada+"' AND CODFILIAL LIKE '%"+codfilial+"'"
    
   
    log.info("QUERY dsBuscaOS4: " + myQuery);
    
    try {
    	
        var conn = ds.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery(myQuery);
        var columnCount = rs.getMetaData().getColumnCount();
        
        while (rs.next()) {
        	
            if (!created) {
            	
                for (var i = 1; i <= columnCount; i++) {
                	
                	dsBuscaOS4.addColumn(rs.getMetaData().getColumnName(i));
                	
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
            
            dsBuscaOS4.addRow(Arr);
            
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
    
    return dsBuscaOS4;
	
}