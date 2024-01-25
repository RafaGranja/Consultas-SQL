// ATUALIZA AS INFORMAÇÕES DE UM DETERMINADO ITEM DA ESTRUTURA COM AS INFORMAÇÕES DO PRODUTO QUE FOI CRIADO NO RM APÓS A INTEGRAÇÃO 
function createDataset(fields, constraints, sortFields) {

	var dsSalvaProdutoOS = DatasetBuilder.newDataset();
    var dataSource = "/jdbc/FluigDS";
    var ic = new javax.naming.InitialContext();
    var ds = ic.lookup(dataSource);
    var created = false;
    
    var numOS = ""
    var idprd = ""
    var codigoprd = ""
    var idCriacao = ""
    var produtoRM = ""
    
    log.info("Entrei no dataset de salvar produto OS")
    	
    if (constraints != null) {
    	
        for (var i = 0; i < constraints.length; i++) {
        	
        	if(constraints[i].fieldName == "OS"){
        		
        		numOS = constraints[i].initialValue; ;
        		
        	}
        	if(constraints[i].fieldName == "IDPRD"){
        		
        		idprd = constraints[i].initialValue; ;
        		
        	}
        	if(constraints[i].fieldName == "CODIGOPRD"){
        		
        		codigoprd = constraints[i].initialValue; ;
        		
        	}
        	if(constraints[i].fieldName == "IDCRIACAO"){
        		
        		idCriacao = constraints[i].initialValue; ;
        		
        	} 
        	if(constraints[i].fieldName == "PRODUTORM"){
        		
        		produtoRM = ", PRODUTORM='"+constraints[i].initialValue+"' ";
        		
        	} 
        	
        }
        
    }  
    
    log.info("numOS: "+numOS+", idprd: "+idprd+", codigoprd: "+codigoprd+", idCriacao: "+idCriacao)
    
    var myQuery = "UPDATE Z_CRM_ML001005 SET IDPRD='"+idprd+"', CODIGOPRD='"+codigoprd+"' "+produtoRM+" WHERE OS='"+numOS+"' AND ITEMEXCLUSIVO!=2 AND IDCRIACAO='"+idCriacao+"'"
    
    log.info("QUERY dsSalvaProdutoOS: " + myQuery);
    
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
    
    return dsSalvaProdutoOS;

}