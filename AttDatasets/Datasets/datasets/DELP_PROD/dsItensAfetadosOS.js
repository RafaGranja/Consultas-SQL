// RETORNA AS INFORMAÇÕES DE TODOS OS ITENS QUE FORAM SINALIZADOS COMO AFETADOS NO PROCESSO DE EDIÇÃO
function createDataset(fields, constraints, sortFields) {

	var dsItensAfetadosOS = DatasetBuilder.newDataset();
    var dataSource = "/jdbc/FluigDS";
    var ic = new javax.naming.InitialContext();
    var ds = ic.lookup(dataSource);
    var created = false;
   
    var numOS = ""
    var idCriacao = ""
	var clausulaIdCriacao = ""
    
	log.info("entrei no dataset dos itens afetados")
		
    if (constraints != null) {
    	
        for (var i = 0; i < constraints.length; i++) {
        	
        	if(constraints[i].fieldName == "OS"){
        		
        		numOS = constraints[i].initialValue;
        	
        	}
        	if(constraints[i].fieldName == "IDCRIACAO"){
        		
        		idCriacao = constraints[i].initialValue;
        	
        		var clausula = idCriacao.split(";")
        		log.info("tamanho: "+clausula.length)
        		
        		for(var k=0; k<clausula.length; k++){
        			
        			if(k==0){
        				
        				clausulaIdCriacao = " ( IDCRIACAO='"+clausula[k]+"' "
        				
        				log.info("clausulaIdCriacao: "+clausulaIdCriacao)
        				
        			} else {
        				
        				if(!(clausula[k]=="")){
        				
        					clausulaIdCriacao = clausulaIdCriacao + " OR IDCRIACAO='"+clausula[k]+"' "
        				
        					log.info("clausulaIdCriacao: "+clausulaIdCriacao)
        					
        				}
        				
        			}
        			
        		}
        		
        		clausulaIdCriacao = clausulaIdCriacao + ") "
        		log.info("clausulaIdCriacao: "+clausulaIdCriacao)
        	}

        }
        
    }  

    log.info("VOU MONTAR A QUERY")
    
    var myQuery =   "SELECT *" +
    				"FROM Z_CRM_ML001005 "+
    				"WHERE "+
    				" "+clausulaIdCriacao+" AND OS='"+numOS+"' AND ITEMEXCLUSIVO!=2"
    
    log.info("QUERY dsItensAfetadosOS: " + myQuery);
    
    try {
    	
        var conn = ds.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery(myQuery);
        var columnCount = rs.getMetaData().getColumnCount();
        
        while (rs.next()) {
        	
            if (!created) {
            	
                for (var i = 1; i <= columnCount; i++) {
                	
                	dsItensAfetadosOS.addColumn(rs.getMetaData().getColumnName(i));
                	
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
            
            dsItensAfetadosOS.addRow(Arr);
            
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
    
    return dsItensAfetadosOS;
	
}