// BUSCA PELO MENOS UM ITEM CADASTRADO QUE SEJA HERDEIRO DE UM DETERMINADO ITEM DA ESTRUTURA E QUE ESTEJA SENDO DETALHADO
function createDataset(fields, constraints, sortFields) {

	var dsHerdeirosDetalhadosOS = DatasetBuilder.newDataset();
    var dataSource = "/jdbc/FluigDS";
    var ic = new javax.naming.InitialContext();
    var ds = ic.lookup(dataSource);
    var created = false;
   
    var numOS = ""
    var idCriacao = ""
    var indice = ""
	
    if (constraints != null) {
    	
        for (var i = 0; i < constraints.length; i++) {
        	
        	if(constraints[i].fieldName == "OS"){
        		
        		numOS = constraints[i].initialValue;
        	
        	}
        	
        	if(constraints[i].fieldName == "IDCRIACAO"){
        		
        		idCriacao = constraints[i].initialValue;
        	
        	}
        	
        	if(constraints[i].fieldName == "INDICE"){
        		
        		indice = constraints[i].initialValue;
        	
        	}

        }
        
    }  

    log.info("VOU MONTAR A QUERY")
    
    var myQuery = "SELECT TOP 1 * FROM Z_CRM_ML001005 WHERE  INDICE LIKE '"+indice+".%' AND OS='"+numOS+"' AND DETALHADO='SIM' AND ITEMEXCLUSIVO!=2 "
    				  
    log.info("QUERY dsHerdeirosDetalhadosOS: " + myQuery);
    
    try {
    	
        var conn = ds.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery(myQuery);
        var columnCount = rs.getMetaData().getColumnCount();
        
        while (rs.next()) {
        	
            if (!created) {
            	
                for (var i = 1; i <= columnCount; i++) {
                	
                	dsHerdeirosDetalhadosOS.addColumn(rs.getMetaData().getColumnName(i));
                	
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
            
            dsHerdeirosDetalhadosOS.addRow(Arr);
            
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
    
    return dsHerdeirosDetalhadosOS;
	
}