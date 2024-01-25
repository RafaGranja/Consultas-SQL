// BUSCA TODOS OS COMPONENTES QUE FORAM SALVOS DA LISTA DE MATERIAIS DE UM ITEM DA ESTRUTURA
function createDataset(fields, constraints, sortFields) {

	var dsVerificaListaRevisaoOS = DatasetBuilder.newDataset();
    var dataSource = "/jdbc/FluigDS";
    var ic = new javax.naming.InitialContext();
    var ds = ic.lookup(dataSource);
    var created = false;
   
    var numOS = ""
    var idCriacao = ""
	
    if (constraints != null) {
    	
        for (var i = 0; i < constraints.length; i++) {
        	
        	if(constraints[i].fieldName == "OS"){
        		
        		numOS = constraints[i].initialValue;
        	
        	}
       

        }
        
    }  

    log.info("VOU MONTAR A QUERY")
    

    var myQuery = "SELECT * FROM Z_CRM_LISTAMATERIAIS Z1 INNER JOIN Z_CRM_ML001005 Z2 ON "+
    				" Z2.IDCRIACAO=Z1.IDCRIACAOSALVOS AND Z2.OS=Z1.NUMOSSALVOS WHERE NUMOSSALVOS='"+numOS+"' AND REVISAOSALVOS='1' AND Z2.ITEMEXCLUSIVO!=2 "
    
    log.info("QUERY dsVerificaListaRevisaoOS: " + myQuery);
    
    try {
    	
        var conn = ds.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery(myQuery);
        var columnCount = rs.getMetaData().getColumnCount();
        
        while (rs.next()) {
        	
            if (!created) {
            	
                for (var i = 1; i <= columnCount; i++) {
                	
                	dsVerificaListaRevisaoOS.addColumn(rs.getMetaData().getColumnName(i));
                	
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
            
            dsVerificaListaRevisaoOS.addRow(Arr);
            
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
    
    return dsVerificaListaRevisaoOS;
	
}