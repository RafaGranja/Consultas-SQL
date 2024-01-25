// BUSCA TODOS OS DADOS DAS RA'S GERADAS
function createDataset(fields, constraints, sortFields) {

	var dsRelSolicitacoesEstrutura = DatasetBuilder.newDataset()
    var dataSource = "/jdbc/FluigDS"
    var ic = new javax.naming.InitialContext()
    var ds = ic.lookup(dataSource)
    var created = false
    
    log.info("dsRelSolicitacoesEstrutura")
    
    var myQuery = " "
    var data = " "
    var os = " "
	var idproduto = ""
	var codColigada = ""
	var codFilial = ""
	var relatorio = ""
	var papc = ""
	var processopai = ""
    	
if (constraints != null) {
	
    for (var i = 0; i < constraints.length; i++) {

    	log.info(constraints[i].fieldName)
		if (constraints[i].fieldName == "DATADE") {
		        	
        	if(!(constraints[i].initialValue=="" || constraints[i].initialValue==null || constraints[i].initialValue==undefined)
        			&& !(constraints[i+1].initialValue=="" || constraints[i+1].initialValue==null || constraints[i+1].initialValue==undefined)){
        	    data = " AND ( CAST(T2.DATAEMISSAO as date) BETWEEN CAST('"+constraints[i].initialValue+"' as date) AND CAST('"+constraints[i+1].initialValue+"' as date)"
        	    data +=  " OR CAST(T2.HORULTIMAALTERACAO as date) BETWEEN CAST('"+constraints[i].initialValue+"'as date) AND CAST('"+constraints[i+1].initialValue+"'as date)" +
        	    		" OR T2.DATAEMISSAO IS NULL )"
	        }
		        	
        }
    	if(constraints[i].fieldName == "RELATORIO"){
    		
    		relatorio = constraints[i].initialValue
    		
    	}
    	if(constraints[i].fieldName == "PROCESSOPAI"){
    		
    		 processopai = " AND PROCESSOPAI='"+constraints[i].initialValue+"' "
    		
    	}
        
    }
    
}
		
	log.info("relatorio:"+relatorio)
	if(relatorio==1){
		
		myQuery = "SELECT *,COALESCE((FORMAT(W.START_DATE,'D','pt-BR')+' '+CONVERT(VARCHAR(20),W.START_DATE,108)),'') DATAINICIO,4 CODRELATORIO,ISNULL(SOLICITANTE,(SELECT TOP 1 CD_PUBLICADOR FROM DOCUMENTO WHERE NR_DOCUMENTO = W.NR_DOCUMENTO_CARD)) SOLICITANTE2 "+
					"	FROM ML001004 M INNER JOIN PROCES_WORKFLOW W "+
					"	ON W.NR_DOCUMENTO_CARD=M.documentid "+
					" WHERE W.STATUS=0 "
		
	}
	else if(relatorio==2){
		
		myQuery = "SELECT *,COALESCE((FORMAT(W.START_DATE,'D','pt-BR')+' '+CONVERT(VARCHAR(20),W.START_DATE,108)),'') DATAINICIO,5 CODRELATORIO,ISNULL(SOLICITANTE,(SELECT TOP 1 CD_PUBLICADOR FROM DOCUMENTO WHERE NR_DOCUMENTO = W.NR_DOCUMENTO_CARD)) SOLICITANTE2 "+
		"	FROM ML001007 M INNER JOIN PROCES_WORKFLOW W "+
		"	ON W.NR_DOCUMENTO_CARD=M.documentid "+
		" WHERE W.STATUS=0 "
		
	}
	else if(relatorio==3){
		
		myQuery = "SELECT *,COALESCE((FORMAT(W.START_DATE,'D','pt-BR')+' '+CONVERT(VARCHAR(20),W.START_DATE,108)),'') DATAINICIO,6 CODRELATORIO,ISNULL(SOLICITANTE,(SELECT TOP 1 CD_PUBLICADOR FROM DOCUMENTO WHERE NR_DOCUMENTO = W.NR_DOCUMENTO_CARD)) SOLICITANTE2 "+
		"	FROM ML001081 M INNER JOIN PROCES_WORKFLOW W "+
		"	ON W.NR_DOCUMENTO_CARD=M.documentid "+
		" WHERE W.STATUS=0 "
		
	}
	else if(relatorio==4){
		
		myQuery = "SELECT * FROM (SELECT *,COALESCE((FORMAT(W.START_DATE,'D','pt-BR')+' '+CONVERT(VARCHAR(20),W.START_DATE,108)),'') DATAINICIO, " +
				" ISNULL(SOLICITANTE,(SELECT TOP 1 CD_PUBLICADOR FROM DOCUMENTO WHERE NR_DOCUMENTO = W.NR_DOCUMENTO_CARD)) SOLICITANTE2,ROW_NUMBER()OVER( PARTITION BY NUMPROCESSO ORDER BY VERSION DESC) SEQ "+
					"	FROM ML001017 M INNER JOIN PROCES_WORKFLOW W "+
					"	ON W.NR_DOCUMENTO_CARD=M.documentid "+
					" WHERE W.STATUS=0 "+processopai+
					" )R WHERE SEQ=1 "+
					"ORDER BY CAST('/' + REPLACE(INDICEPAI, '.', '/') + '/' AS HIERARCHYID) "
		
	}
	else if(relatorio==5){
		
		myQuery = "SELECT * FROM (SELECT *,COALESCE((FORMAT(W.START_DATE,'D','pt-BR')+' '+CONVERT(VARCHAR(20),W.START_DATE,108)),'') DATAINICIO, " +
				" ISNULL(SOLICITANTE,(SELECT TOP 1 CD_PUBLICADOR FROM DOCUMENTO WHERE NR_DOCUMENTO = W.NR_DOCUMENTO_CARD)) SOLICITANTE2,ROW_NUMBER()OVER( PARTITION BY NUMPROCESSO ORDER BY VERSION DESC) SEQ "+ 
		"	FROM ML001020 M INNER JOIN PROCES_WORKFLOW W "+
		"	ON W.NR_DOCUMENTO_CARD=M.documentid "+
		" WHERE W.STATUS=0 "+processopai+
		" )R WHERE SEQ=1 "+
		"ORDER BY CAST('/' + REPLACE(INDICEPAI, '.', '/') + '/' AS HIERARCHYID) "
		
	}
	else if(relatorio==6){
		
		myQuery = "SELECT * FROM (SELECT *,COALESCE((FORMAT(W.START_DATE,'D','pt-BR')+' '+CONVERT(VARCHAR(20),W.START_DATE,108)),'') DATAINICIO, " +
				" ISNULL(SOLICITANTE,(SELECT TOP 1 CD_PUBLICADOR FROM DOCUMENTO WHERE NR_DOCUMENTO = W.NR_DOCUMENTO_CARD)) SOLICITANTE2,ROW_NUMBER()OVER( PARTITION BY NUMPROCESSO ORDER BY VERSION DESC) SEQ "+
		"	FROM ML001084 M INNER JOIN PROCES_WORKFLOW W "+
		"	ON W.NR_DOCUMENTO_CARD=M.documentid "+
		" WHERE W.STATUS=0 "+processopai+
		" )R WHERE SEQ=1 "+
		"ORDER BY CAST('/' + REPLACE(INDICEPAI, '.', '/') + '/' AS HIERARCHYID) "
		
	}
	else if(relatorio==7){
		
		myQuery = " SELECT *  "+
					" FROM ( "+
			" 	SELECT distinct  "+
			" 		E1.OS,E1.INDICE,E1.NIVEL,E1.POSICAOINDICE,E1.DESCRICAO,E1.NUMDESENHO,E1.CODTRFPAI,E1.POSICAODESENHO,E1.INDICEANTIGO,E1.IDCRIACAO,E1.IDCRIACAOPAI,E1.ITEMEXCLUSIVO,E1.EXECUCAO "+
			" 				FROM Z_CRM_EX001005 E1 "+
			" 					INNER JOIN Z_CRM_EX001005 E2 ON "+
			" 					E2.INDICE=E1.INDICE "+
			" 					AND E2.OS=E1.OS "+
			" 					AND E2.EXECUCAO=E1.EXECUCAO "+
			" 				WHERE E1.IDCRIACAO!=E2.IDCRIACAO AND E1.ITEMEXCLUSIVO<>2 AND E2.ITEMEXCLUSIVO<>2 "+
			" 		UNION  "+
			" 		SELECT  "+
			" 			distinct E1.OS,E1.INDICE,E1.NIVEL,E1.POSICAOINDICE,E1.DESCRICAO,E1.NUMDESENHO,E1.CODTRFPAI,E1.POSICAODESENHO,E1.INDICEANTIGO,E1.IDCRIACAO,E1.IDCRIACAOPAI,NULL,NULL EXECUCAO "+
			" 				FROM  "+
			" 					Z_CRM_ML001005 E1 "+
			" 					INNER JOIN Z_CRM_ML001005 E2 ON "+
			" 					E2.INDICE=E1.INDICE "+
			" 					AND E2.OS=E1.OS "+
			" 				WHERE E1.IDCRIACAO!=E2.IDCRIACAO AND E1.ITEMEXCLUSIVO!=2 AND E2.ITEMEXCLUSIVO!=2 )R "+
			" 	ORDER BY CAST('/' + REPLACE(R.INDICE, '.', '/') + '/' AS HIERARCHYID),R.EXECUCAO,R.DESCRICAO "
		
	}
				  

				
    log.info("dsRelSolicitacoesEstrutura MY QUERY: " + myQuery)
    
    try {
    	
        var conn = ds.getConnection()
        var stmt = conn.createStatement()
        var rs = stmt.executeQuery(myQuery)
        var columnCount = rs.getMetaData().getColumnCount()
        
        while (rs.next()) {
        	
            if (!created) {
            	
                for (var i = 1; i <= columnCount; i++) {
                	
                	dsRelSolicitacoesEstrutura.addColumn(rs.getMetaData().getColumnName(i))
                    
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
            
            dsRelSolicitacoesEstrutura.addRow(Arr)
            
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
    
    return dsRelSolicitacoesEstrutura
	
}