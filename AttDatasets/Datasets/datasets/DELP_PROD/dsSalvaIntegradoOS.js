// ATUALIZA NA TABELA DA ESTRUTURA O CAMPO INTEGRADO PARA TODOS OS ITENS QUE PASSARAM PELA INTEGRAÇÃO DE UM DETERMINADO PROJETO (OS)
function createDataset(fields, constraints, sortFields) {

	log.info("entrei para executar o dataset dsSalvaIntegradoOS")
	
	var dsSalvaIntegradoOS = DatasetBuilder.newDataset();
    var dataSource = "/jdbc/FluigDS";
    var ic = new javax.naming.InitialContext();
    var ds = ic.lookup(dataSource);
    var created = false;
    
    var numOS = ""
    
    log.info("vou montar as constraints")
    	
    if (constraints != null) {
    	
        for (var i = 0; i < constraints.length; i++) {
        	
        	if(constraints[i].fieldName == "OS"){
        		
        		numOS = constraints[i].initialValue;
        	
        	} 
        	
        }
        
    }  

    log.info("VOU MONTAR A QUERY")
    
    var myQuery = "UPDATE Z_CRM_ML001005 SET INTEGRADO='SIM' WHERE OS='"+numOS+"' AND ITEMEXCLUSIVO!=2"
    
    log.info("QUERY dsSalvaIntegradoOS: " + myQuery);
    
    try {
    	
        var conn = ds.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.execute(myQuery);
        
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
    
    return dsSalvaIntegradoOS;
	
}