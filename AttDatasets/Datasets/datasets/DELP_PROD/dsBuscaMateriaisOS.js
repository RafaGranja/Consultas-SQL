function createDataset(fields, constraints, sortFields) {

	var dsBuscaMateriaisOS = DatasetBuilder.newDataset();
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
    
    var myQuery =   "SELECT *, " +
    				"		( "+
					"			CASE WHEN P.NIVEL = '' OR P.NIVEL= NULL THEN P.EXECUCOES "+
					"			ELSE "+
					"		 "+
    				"		(SELECT "+ 
					"			M.EXECUCOES "+
					"		FROM "+
					"			Z_CRM_ML001005 M "+
					"			RIGHT JOIN Z_CRM_ML001005 ML ON M.OS = ML.OS "+
					"		WHERE ML.ITEMEXCLUSIVO!=2 AND M.ITEMEXCLUSIVO!=2 AND "+
					"			(ML.INDICE LIKE M.INDICE+'.%' OR ML.INDICE LIKE M.NIVEL) "+
					"			AND (M.NIVEL IS NULL OR M.NIVEL LIKE '') "+
					"			AND M.OS = P.OS AND ML.IDCRIACAO = P.IDCRIACAO ) END ) EXECUCAO "+
					"FROM "+
					"	Z_CRM_ML001005 P " +
					"WHERE " +
					"	P.OS='"+numOS+"' " +
					"	AND P.COMPORLISTA='SIM' " +
					"	AND P.IDCRIACAO IS NOT NULL AND P.ITEMEXCLUSIVO!=2 " +
					"ORDER BY " +
					"	P.NUMDESENHO "
   
    log.info("QUERY dsBuscaMateriaisOS: " + myQuery);
    
    try {
    	
        var conn = ds.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery(myQuery);
        var columnCount = rs.getMetaData().getColumnCount();
        
        while (rs.next()) {
        	
            if (!created) {
            	
                for (var i = 1; i <= columnCount; i++) {
                	
                	dsBuscaMateriaisOS.addColumn(rs.getMetaData().getColumnName(i));
                	
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
            
            dsBuscaMateriaisOS.addRow(Arr);
            
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
    
    return dsBuscaMateriaisOS;
	
}