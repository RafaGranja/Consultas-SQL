// BUSCA TODOS OS FILHOS DE UM DETERMINADO ITEM PELO INDICE
function createDataset(fields, constraints, sortFields) {

	var dsBuscaIndicesFilhosOS = DatasetBuilder.newDataset();
    var dataSource = "/jdbc/FluigDS";
    var ic = new javax.naming.InitialContext();
    var ds = ic.lookup(dataSource);
    var created = false;
   
    var idCriacao = ""
    var indice = ""
	
    if (constraints != null) {
    	
        for (var i = 0; i < constraints.length; i++) {
        	
        	if(constraints[i].fieldName == "OS"){
        		
        		numOS = constraints[i].initialValue;
        	
        	} else if(constraints[i].fieldName == "INDICE"){
        		
        		indice = constraints[i].initialValue;
        	
        	} 
        	
        }
        
    }  

    log.info("VOU MONTAR A QUERY")
    
    var myQuery =   "SELECT "+ 
					"	INDICE "+ 
					"FROM "+
					"	Z_CRM_ML001005 "+
					"WHERE "+
					"	NIVEL LIKE '"+indice+"' AND OS='"+numOS+"' AND ITEMEXCLUSIVO!=2 "
   
    log.info("QUERY dsBuscaIndicesFilhosOS: " + myQuery);
    
    try {
    	
        var conn = ds.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery(myQuery);
        var columnCount = rs.getMetaData().getColumnCount();
        
        while (rs.next()) {
        	
            if (!created) {
            	
                for (var i = 1; i <= columnCount; i++) {
                	
                	dsBuscaIndicesFilhosOS.addColumn(rs.getMetaData().getColumnName(i));
                	
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
            
            dsBuscaIndicesFilhosOS.addRow(Arr);
            
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
    
    return dsBuscaIndicesFilhosOS;

}