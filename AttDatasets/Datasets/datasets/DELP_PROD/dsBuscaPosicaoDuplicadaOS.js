// BUSCA TODAS AS POSIÇÕES IDÊNTICAS QUE FORAM CADASTRADAS DENTRO DE UMA ESTRUTURA DE UM PROJETO (OS)
function createDataset(fields, constraints, sortFields) {

	var dsBuscaPosicaoDuplicadaOS = DatasetBuilder.newDataset();
    var dataSource = "/jdbc/FluigDS";
    var ic = new javax.naming.InitialContext();
    var ds = ic.lookup(dataSource);
    var created = false;
   
    var numOS = ""
    var numDesenho = ""
    var posicaoDesenho = ""
	
    if (constraints != null) {
    	
        for (var i = 0; i < constraints.length; i++) {
        	
        	if(constraints[i].fieldName == "OS"){
        		
        		numOS = constraints[i].initialValue;
        	
        	}
        	if(constraints[i].fieldName == "NUMDESENHO"){
        		
        		numDesenho = constraints[i].initialValue;
        	
        	}
        	if(constraints[i].fieldName == "POSICAODESENHO"){
        		
        		posicaoDesenho = constraints[i].initialValue;
        	
        	}
        
        }
        
    }  

    log.info("VOU MONTAR A QUERY")
    
    var myQuery = "SELECT * " +
    			  "		FROM Z_CRM_ML001005 " +
    			  "WHERE " +
    			  "		NUMDESENHO='"+numDesenho+"'  AND POSICAODESENHO='"+posicaoDesenho+"' AND OS='"+numOS+"' AND ITEMEXCLUSIVO!=2 AND COMPORLISTA='SIM' "
    
    
    log.info("QUERY dsBuscaPosicaoDuplicadaOS: " + myQuery);
    
    try {
    	
        var conn = ds.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery(myQuery);
        var columnCount = rs.getMetaData().getColumnCount();
        
        while (rs.next()) {
        	
            if (!created) {
            	
                for (var i = 1; i <= columnCount; i++) {
                	
                	dsBuscaPosicaoDuplicadaOS.addColumn(rs.getMetaData().getColumnName(i));
                	
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
            
            dsBuscaPosicaoDuplicadaOS.addRow(Arr);
            
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
    
    return dsBuscaPosicaoDuplicadaOS;
	
}