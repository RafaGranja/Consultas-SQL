// VERIFICA SE ITEM É O PAI PRINCIPAL DA ESTRUTURA
function createDataset(fields, constraints, sortFields) {

	var dsEXItemPaiPrincipalOS = DatasetBuilder.newDataset()
    var dataSource = "/jdbc/FluigDS"
    var ic = new javax.naming.InitialContext()
    var ds = ic.lookup(dataSource)
    var created = false;
   
    var numOS = ""
    var idCriacao = ""
    var execucao = ""
	
    if (constraints != null) {
    	
        for (var i = 0; i < constraints.length; i++) {
        	
        	if(constraints[i].fieldName == "OS"){
        		
        		numOS = constraints[i].initialValue
        	
        	}
        	
        	if(constraints[i].fieldName == "IDCRIACAO"){
        		
        		idCriacao = constraints[i].initialValue
        	
        	}
        	
        	if(constraints[i].fieldName == "EXECUCAO"){
        		
        		execucao = constraints[i].initialValue
        	
        	}

        }
        
    }  

    log.info("VOU MONTAR A QUERY")
    
    // ALTERAR PARA PERMITIR IRMÃOS SOMENTE DOS ITENS QUE SÃO PAIS PRINCIPAIS
    /*var myQuery = 	"SELECT "+
					"	* "+
					"FROM "+
					"	Z_CRM_ML001005 "+
					"WHERE "+
					"	OS='"+numOS+"' AND IDCRIACAO='"+idCriacao+"' AND NIVEL='' "*/
    
    var myQuery = 	"SELECT "+
					"	* "+
					"FROM "+
					"	Z_CRM_EX001005 "+
					"WHERE "+
					"	OS='"+numOS+"' AND IDCRIACAO='"+idCriacao+"' AND INDICE='1' AND NIVEL='' AND ITEMEXCLUSIVO!=2 AND EXECUCAO="+execucao
    
				  
    log.info("QUERY dsEXItemPaiPrincipalOS: " + myQuery);
    
    try {
    	
        var conn = ds.getConnection()
        var stmt = conn.createStatement()
        var rs = stmt.executeQuery(myQuery)
        var columnCount = rs.getMetaData().getColumnCount()
        
        while (rs.next()) {
        	
            if (!created) {
            	
                for (var i = 1; i <= columnCount; i++) {
                	
                	dsEXItemPaiPrincipalOS.addColumn(rs.getMetaData().getColumnName(i))
                	
                }
                
                created = true
                
            }
            
            var Arr = new Array()
            
            for (var i = 1; i <= columnCount; i++) {
            	
                var obj = rs.getObject(rs.getMetaData().getColumnName(i))
                
                if (null != obj) {
                	
                    Arr[i - 1] = rs.getObject(rs.getMetaData().getColumnName(i)).toString()
                    
                } else {
                	
                    Arr[i - 1] = "null"
                    
                }
                
            }
            
            dsEXItemPaiPrincipalOS.addRow(Arr)
            
        }
        
    } catch (e) {
        
    	log.error("ERRO==============> " + e.message)
    
    } finally {
    	
        if (stmt != null) {
        	
            stmt.close()
            
        }
        
        if (conn != null) {
        	
            conn.close()
            
        }
        
    }
    
    return dsEXItemPaiPrincipalOS
	
}