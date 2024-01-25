// BUSCA SE UM DETERMINADO PRODUTO TEM ESTRUTURA CRIADA
function createDataset(fields, constraints, sortFields) {

	var dsNewDataset = DatasetBuilder.newDataset();
    var dataSource = "/jdbc/FluigDSRM";
    var ic = new javax.naming.InitialContext();
    var ds = ic.lookup(dataSource);
    var created = false;
   
    var numOS = ""
    var idprd = ""
    var codColigada = ""
    var codFilial = ""
    	
    if (constraints != null) {
    	
        for (var i = 0; i < constraints.length; i++) {
        	
        	if(constraints[i].fieldName == "OS"){
        		
        		numOS = constraints[i].initialValue;
        	
        	}
			if(constraints[i].fieldName == "CODCOLIGADA"){
			        		
        		codColigada = constraints[i].initialValue;
        	
        	}
			if(constraints[i].fieldName == "CODFILIAL"){
				
				codFilial = constraints[i].initialValue;
			
			}
        	if(constraints[i].fieldName == "IDPRD"){
        		
        		idprd = constraints[i].initialValue;
        	
        	}
        
        }
        
    }  

    log.info("VOU MONTAR A QUERY")
    
    var myQuery = "SELECT * FROM KESTRUTURA WHERE CODCCUSTO='"+numOS+"' AND IDPRODUTO="+idprd+" AND CODCOLIGADA="+codColigada+" AND CODFILIAL="+codFilial+" "
    
    
    log.info("QUERY dsTemEstrutura: " + myQuery);
    
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