// VERIFICA SE UM DETERMINADO ITEM DA ESTRUTURA QUE É NÃO MANUFATURADO TEM FILHOS
function createDataset(fields, constraints, sortFields) {

	var dsBuscaItensTiposDiferenteOS = DatasetBuilder.newDataset();
    var dataSource = "/jdbc/FluigDS";
    var ic = new javax.naming.InitialContext();
    var ds = ic.lookup(dataSource);
    var created = false;
   
    var numOS = ""
    var exec = ""
    var codtrfpai = ""
	
    if (constraints != null) {
    	
        for (var i = 0; i < constraints.length; i++) {
        	
        	if(constraints[i].fieldName == "OS"){
        		
        		numOS = constraints[i].initialValue;
        	
        	} 
                    	
        	if(constraints[i].fieldName == "EXEC"){
        		
        		exec = constraints[i].initialValue;
        	
        	} 
        	
        	if(constraints[i].fieldName == "CODTRFPAI"){
        		
        		codtrfpai = constraints[i].initialValue;
        	
        	} 
        	
        }
        
    }  

    log.info("VOU MONTAR A QUERY")

    var myQuery =  " "

    if (exec=="") {
        
        myQuery =  " SELECT Z2.* FROM Z_CRM_ML001005 Z1  "+
        " INNER JOIN Z_CRM_ML001005 Z2 ON "+
        " Z1.CODCOLIGADA=Z2.CODCOLIGADA "+
        " AND Z1.CODFILIAL=Z2.CODFILIAL "+
        " AND Z1.OS=Z2.OS "+
        " AND Z1.NUMDESENHO=Z2.NUMDESENHO "+
        " AND Z1.POSICAODESENHO=Z2.POSICAODESENHO "+
        " AND Z1.DESCRICAO=Z2.DESCRICAO "+
        " AND Z1.IDCRIACAO!=Z2.IDCRIACAO "+
        " WHERE Z1.TIPODESENHO!=Z2.TIPODESENHO AND Z1.OS ='"+numOS+"' "

    }
    else{

        myQuery =  " SELECT Z2.* FROM Z_CRM_EX001005 Z1  "+
        " INNER JOIN Z_CRM_EX001005 Z2 ON "+
        " Z1.CODCOLIGADA=Z2.CODCOLIGADA "+
        " AND Z1.CODFILIAL=Z2.CODFILIAL "+
        " AND Z1.OS=Z2.OS "+
        " AND Z1.NUMDESENHO=Z2.NUMDESENHO "+
        " AND Z1.POSICAODESENHO=Z2.POSICAODESENHO "+
        " AND Z1.DESCRICAO=Z2.DESCRICAO "+
        " AND Z1.IDCRIACAO!=Z2.IDCRIACAO "+
        " AND Z1.EXECUCAO=Z2.EXECUCAO  "+
        " AND Z1.ITEMEXCLUSICO=Z2.ITEMEXCLUSICO  "+
        " WHERE Z1.TIPODESENHO!=Z2.TIPODESENHO AND Z1.OS ='"+numOS+"' AND Z1.EXECUCAO ="+exec+" AND CODTRFEX='"+codtrfpai+"' AND AND Z1.ITEMEXCLUSICO!=2 "


    }

    
    log.info("QUERY dsBuscaItensTiposDiferenteOS: " + myQuery);
    
    try {
    	
        var conn = ds.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery(myQuery);
        var columnCount = rs.getMetaData().getColumnCount();
        
        while (rs.next()) {
        	
            if (!created) {
            	
                for (var i = 1; i <= columnCount; i++) {
                	
                	dsBuscaItensTiposDiferenteOS.addColumn(rs.getMetaData().getColumnName(i));
                	
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
            
            dsBuscaItensTiposDiferenteOS.addRow(Arr);
            
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
    
    return dsBuscaItensTiposDiferenteOS;
	
}