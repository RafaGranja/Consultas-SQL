// BUSCA TODOS OS ITENS QUE TÊM INFORMAÇÕES OBRIGATÓRIAS QUE AINDA NÃO FORAM PREENCHIDAS
function createDataset(fields, constraints, sortFields) {

	var dsNewDataset = DatasetBuilder.newDataset();
    var dataSource = "/jdbc/FluigDS";
    var ic = new javax.naming.InitialContext();
    var ds = ic.lookup(dataSource);
    var created = false;
    
    var numOS = ""
    
    if (constraints != null) {
    	
        for (var i = 0; i < constraints.length; i++) {
        	
        	if(constraints[i].fieldName == "OS"){
        		 
        		numOS = constraints[i].initialValue; ;
        		
        	} 
        	
        }
        
    }  
    
    var myQuery = "SELECT "+
				  "		* "+
				  "	FROM "+
				  "		Z_CRM_ML001005 "+
				  "	WHERE "+
				  "		OS='"+numOS+"' AND ITEMEXCLUSIVO!=2 AND ( (TIPODESENHO IS NULL OR TIPODESENHO='') OR (DESQTDE IS NULL OR DESQTDE='') OR (PESOBRUTO IS NULL OR PESOBRUTO='') OR (CODTRFITEM IS NULL OR CODTRFITEM='') "+ 
				  "		OR (QTDEUNCOMP = (CASE WHEN (COMPORLISTA='SIM' AND TIPODESENHO='NAOMANUFATURADO' AND QTDEUNCOMP='') THEN '' "+
				  "							   WHEN (COMPORLISTA='SIM' AND TIPODESENHO='NAOMANUFATURADO' AND QTDEUNCOMP IS NULL) THEN NULL "+
				  "							  END) ) "+
				  "		OR (UNDMEDIDA = (CASE WHEN (TIPODESENHO<>'NAOMANUFATURADO' AND UNDMEDIDA='') THEN '' "+
				  "							  WHEN (TIPODESENHO<>'NAOMANUFATURADO' AND UNDMEDIDA IS NULL) THEN NULL "+
				  "							 END) ) "+
				  "		OR (CODTRFPAI = (CASE WHEN (NIVEL IS NULL OR NIVEL='') AND (CODTRFPAI IS NULL) THEN NULL "+
				  "						   WHEN (NIVEL IS NULL OR NIVEL='') AND (CODTRFPAI='')  THEN '' "+
				  "							   END) )  ) "
        
   
    log.info("QUERY dsBuscaItensPendentesOS: " + myQuery);
    
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