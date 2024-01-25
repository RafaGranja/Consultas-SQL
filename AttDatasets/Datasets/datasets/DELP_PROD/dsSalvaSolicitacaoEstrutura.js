// SALVA O NÚMERO DA SOLICITAÇÃO NA TABELA DA ESTRUTURA DE UM DETERMINADO PROJETO (OS) QUE FOI CADASTRADO
function createDataset(fields, constraints, sortFields) {

	var dsSalvaSolicitacaoEstrutura = DatasetBuilder.newDataset();
    var dataSource = "/jdbc/FluigDS";
    var ic = new javax.naming.InitialContext();
    var ds = ic.lookup(dataSource);
    var created = false;
    
    var solicitacao = ""
    var numOS = ""
    
    if (constraints != null) {
    	
        for (var i = 0; i < constraints.length; i++) {
        	
        	if(constraints[i].fieldName == "OS"){
        		
        		numOS = constraints[i].initialValue; ;
        		
        	}
        	if(constraints[i].fieldName == "SOLICITACAO"){
        		
        		solicitacao = constraints[i].initialValue; ;
        		
        	}
        	
        }
        
    }  
    
    var myQuery = "UPDATE Z_CRM_ML001005 SET SOLICITACAO='"+solicitacao+"' WHERE OS='"+numOS+"' "
    
    log.info("QUERY dsSalvaSolicitacaoEstrutura: " + myQuery);
    
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
    
    return dsSalvaSolicitacaoEstrutura;

}