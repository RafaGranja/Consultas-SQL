// BUSCA A QUANTIDADE DE EXECUÇÕES DO PAI DE UM DETERMINADO ITEM DA ESTRUTURA
function createDataset(fields, constraints, sortFields) {
	
	var dsBuscaExecPaiOS = DatasetBuilder.newDataset();
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
        	if(constraints[i].fieldName == "IDCRIACAO"){
        		
        		idCriacao = constraints[i].initialValue;
        		
        	}
        	
        }
        
    }  
        
    var myQuery =   "SELECT "+
					"	M.INDICE,M.NIVEL,M.IDCRIACAO,M.NUMDESENHO,M.DESCRICAO,M.POSICAODESENHO,M.DESQTDE,M.EXPANSOR,M.EXECUCOES "+
					"FROM "+
					"	Z_CRM_ML001005 M "+
					"	RIGHT JOIN Z_CRM_ML001005 ML ON M.OS = ML.OS "+
					"WHERE "+
					"	(ML.INDICE LIKE M.INDICE+'%' OR ML.INDICE LIKE M.NIVEL) "+
					"	AND (M.NIVEL IS NULL OR M.NIVEL LIKE '') "+
					"	AND M.OS = '"+numOS+"' AND ML.IDCRIACAO = '"+idCriacao+"' AND M.ITEMEXCLUSIVO!=2 AND ML.ITEMEXCLUSIVO!=2 "
            
    log.info("QUERY dsBuscaExecPaiOS: " + myQuery);
    
    try {
    	
        var conn = ds.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery(myQuery);
        var columnCount = rs.getMetaData().getColumnCount();
        
        while (rs.next()) {
        	
            if (!created) {
            	
                for (var i = 1; i <= columnCount; i++) {
                	
                	dsBuscaExecPaiOS.addColumn(rs.getMetaData().getColumnName(i));
                	
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
            
            dsBuscaExecPaiOS.addRow(Arr);
            
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
    
    return dsBuscaExecPaiOS;

}