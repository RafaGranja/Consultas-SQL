// BUSCA TODOS OS COMPONENTES QUE FORAM SALVOS DA LISTA DE MATERIAIS DE UM ITEM DA ESTRUTURA
function createDataset(fields, constraints, sortFields) {

	var dsRevisarItemListaMateriais = DatasetBuilder.newDataset();
    var dataSource = "/jdbc/FluigDS";
    var ic = new javax.naming.InitialContext();
    var ds = ic.lookup(dataSource);
    var created = false;
   
    var numOS = ""
    var numDesenho = ""
	var descricao = ""
	var posicao = ""
	
    if (constraints != null) {
    	
        for (var i = 0; i < constraints.length; i++) {
        	
        	if(constraints[i].fieldName == "NUMOS"){
        		
        		numOs = constraints[i].initialValue;
        	
        	}
        	
        	if(constraints[i].fieldName == "NUMDESENHO"){
        		
        		numDesenho = constraints[i].initialValue;
        	
        	}
        	if(constraints[i].fieldName == "DESCRICAO"){
        		
        		descricao = constraints[i].initialValue;
        	
        	}
        	if(constraints[i].fieldName == "POSICAO"){
        		
        		posicao = constraints[i].initialValue;
        	
        	}

        }
        
    }  

    log.info("VOU MONTAR A QUERY")
    
    
    var myQuery = " UPDATE Z_CRM_LISTAMATERIAIS	SET REVISAOSALVOS=1 WHERE IDCRIACAOSALVOS IN ( SELECT DISTINCT IDCRIACAO FROM Z_CRM_ML001005 WHERE OS='"+numOs+"' AND ITEMEXCLUSIVO!=2 " +
    		" AND NUMDESENHO='"+numDesenho+"' AND POSICAODESENHO='"+posicao+"' AND DESCRICAO='"+descricao+"') AND NUMOSSALVOS='"+numOs+"'  "
    
    log.info("QUERY dsRevisarItemListaMateriais: " + myQuery);
    
    try {
    	
        var conn = ds.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.execute(myQuery);
        var columnCount = rs.getMetaData().getColumnCount();
        
        while (rs.next()) {
        	
            if (!created) {
            	
                for (var i = 1; i <= columnCount; i++) {
                	
                	dsRevisarItemListaMateriais.addColumn(rs.getMetaData().getColumnName(i));
                	
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
            
            dsRevisarItemListaMateriais.addRow(Arr);
            
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
    
    return dsRevisarItemListaMateriais;
	
}