function createDataset(fields, constraints, sortFields) {

	var dsVerificaSubstitutoFluig= DatasetBuilder.newDataset();
    var dataSource = "/jdbc/FluigDS";
    var ic = new javax.naming.InitialContext();
    var ds = ic.lookup(dataSource);
    var created = false;
    
    var os=""
    var estrutura=""
    var codigoprincipal=""
    var prioridade=""
    var unidade=""
    var codigosubstituto=""	
    
    
if (constraints != null) {
    	
        for (var i = 0; i < constraints.length; i++) {
        	
        	if(constraints[i].fieldName == "OS"){
        		
        		os = constraints[i].initialValue
        		
        	} 
        	if(constraints[i].fieldName == "ESTRUTURA"){
        		
        		estrutura = constraints[i].initialValue
              	
        	}
        	if(constraints[i].fieldName == "CODIGOPRINCIPAL"){
        		
        		codigoprincipal = constraints[i].initialValue
              	
        	} 
        	if(constraints[i].fieldName == "PRIORIDADE"){
        		
        		prioridade = constraints[i].initialValue
        		
        	} 
        	if(constraints[i].fieldName == "CODIGOUNIDADE"){
        		
        		unidade = constraints[i].initialValue
              	
        	}
        	if(constraints[i].fieldName == "CODIGOSUBSTITUTO"){
        		
        		codigosubstituto = constraints[i].initialValue
              	
        	}
        	
        }
        
    }
    
    var myQuery =   " SELECT * "+
    				" FROM Z_CRM_ML001019 Z1"+
    				" INNER JOIN Z_CRM_ML001005 Z2 ON Z2.OS=Z1.OSCOMPONENTES AND Z2.IDCRIACAO=Z1.IDCRIACAOCOMPONENTES "+
    				" WHERE Z1.OSCOMPONENTES='"+os+"' AND  Z1.CODIGOPRDCOMPONENTES='"+codigosubstituto+"' " +
    				" AND Z1.CODUNDCOMPONENTES='"+unidade+"' AND Z1.SUBSTITUTOCOMPONENTES='"+codigoprincipal+"' AND Z2.ITEMEXCLUSIVO!=2 "
    
   
    log.info("QUERY dsVerificaSubstitutoFluig: " + myQuery);
    
    try {
    	
        var conn = ds.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery(myQuery);
        var columnCount = rs.getMetaData().getColumnCount();
        
        while (rs.next()) {
        	
            if (!created) {
            	
                for (var i = 1; i <= columnCount; i++) {
                	
                	dsVerificaSubstitutoFluig.addColumn(rs.getMetaData().getColumnName(i));
                	
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
            
            dsVerificaSubstitutoFluig.addRow(Arr);
            
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
    
    return dsVerificaSubstitutoFluig;
	
}