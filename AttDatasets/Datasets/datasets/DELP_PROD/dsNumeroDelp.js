// BUSCA OS PLANOS QUE FORAM CADASTRADOS E AINDA NÂO FORAM PROGRAMADOS
function createDataset(fields, constraints, sortFields) {

	var dsNumeroDelp = DatasetBuilder.newDataset()
    var dataSource = "/jdbc/FluigDS"
    var ic = new javax.naming.InitialContext()
    var ds = ic.lookup(dataSource)
    var created = false

    var desenho = "" 
    var os = ""

    	
    log.info("Entrei no dataset dsNumeroDelp")
    	
	if (constraints != null) {
    	
        for (var i = 0; i < constraints.length; i++) {
        	
        	if (constraints[i].fieldName == "DESENHO"){
        		
        		desenho = constraints[i].initialValue
        		
        	}
			if (constraints[i].fieldName == "OS"){
			        		
				os = constraints[i].initialValue
        		
        	}
        	
        }
        
    }
	
	myQuery =   " SELECT DISTINCT NUMDOCDELP FROM Z_CRM_ML001005 "+
				" WHERE  OS = '"+os+"'"+
				"	   AND NUMDESENHO LIKE '"+desenho+"%' AND COALESCE(NUMDOCDELP,'VAZIO') != 'VAZIO'  AND ITEMEXCLUSIVO!=2 " 
					      
    log.info("dsNumeroDelp MY QUERY: " + myQuery)
    
    try {
    	
        var conn = ds.getConnection()
        var stmt = conn.createStatement()
        var rs = stmt.executeQuery(myQuery)
        var columnCount = rs.getMetaData().getColumnCount()
        
        while (rs.next()) {
        	
            if (!created) {
            	
                for (var i = 1; i <= columnCount; i++) {
                	
                	dsNumeroDelp.addColumn(rs.getMetaData().getColumnName(i))
                    
                }
                
                created = true
                
            }
            
            var Arr = new Array()
            
            for (var i = 1; i <= columnCount; i++) {
            	
                var obj = rs.getObject(rs.getMetaData().getColumnName(i))
                
                if (null != obj) {
                	
                    Arr[i - 1] = rs.getObject(rs.getMetaData().getColumnName(i)).toString()
                    
                } else {
                	
                    Arr[i - 1] = "null"
                    	
                }
                
            }
            
            dsNumeroDelp.addRow(Arr)
            
        }
        
    } catch (e) {
    	
        log.error("ERRO==============> " + e.message)
        
    } finally {
    	
        if (stmt != null) {
        	
            stmt.close()
            
        }
        
        if (conn != null) {
        	
            conn.close()
        }
        
    }
    
    return dsNumeroDelp;

	
}