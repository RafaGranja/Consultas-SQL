// BUSCA OS PLANOS QUE FORAM CADASTRADOS E AINDA NÂO FORAM PROGRAMADOS
function createDataset(fields, constraints, sortFields) {

	var dsListaDetalhada = DatasetBuilder.newDataset()
    var dataSource = "/jdbc/FluigDS"
    var ic = new javax.naming.InitialContext()
    var ds = ic.lookup(dataSource)
    var created = false

    var desenho = "" 
    var os = ""
    var numdelp = ""
    var codtrfpai = ""
    var numexec = ""

    	
    log.info("Entrei no dataset dsQuantidadeApontada")
    	
	if (constraints != null) {
    	
        for (var i = 0; i < constraints.length; i++) {
        	
        	if (constraints[i].fieldName == "DESENHO"){
        		
        		desenho = constraints[i].initialValue
        		
        	}
			if (constraints[i].fieldName == "OS"){
			        		
				os = constraints[i].initialValue
        		
        	}
			if (constraints[i].fieldName == "NUMDELP"){
        		
				numdelp = constraints[i].initialValue
        		
        	}
			
			if (constraints[i].fieldName == "NUMEXEC"){
        		
				numexec = constraints[i].initialValue
        		
        	}

				if (constraints[i].fieldName == "CODTRFPAI"){
	
					codtrfpai = constraints[i].initialValue
	
}
        	
        }
        
    }
	
	myQuery =   "SELECT 'Principal' AS EXEC2,							  "+//F.CODCLIENTE, "+
			    "   Z.NUMDESENHO, "+
			    "   Z.REVISAODESENHO, "+
			    "   Z.NUMDBI, "+
			    "   Z.REVISAODBI, "+
			    "   Z.NUMDOCDELP, "+
			    "   Z.REVISAODOCDELP, "+
			    "   Z.DESQTDE, "+
			    "   Z.TOTALQTDE, "+
			    "   Z.POSICAODESENHO, "+
			    "   Z.DESCRICAO, "+
			    "   Z.MATERIAL, "+
			    "   Z.BITOLA, "+
			    "   Z.LARGURA, "+
			    "   Z.COMPRIMENTO, "+
			    "   Z.ESPROSCA, "+
			    "   Z.MASSALINEAR, "+
			    "   isnull(Z.PESOUNLIQUIDO,0) PESOUNLIQUIDO, "+
			    "   round((cast(isnull(replace(Z.PESOUNLIQUIDO,',','.'),0) as float)*cast(Z.DESQTDE as int)),4) PESOBRUTO, "+
			    "   Z.DIAMETROEXTERNO, "+
			    "   Z.DIAMETROINTERNO, "+
			    "   Z.ESPESSURA, "+
			    "   Z.DIAMETROEXTERNODISCO, "+
			    "   Z.DIAMETROINTERNODISCO, "+
			    "   Z.ALTURA, "+
			    "   Z.LARGURAABA, "+
			    "   Z.ESPALMA, "+
			    "   Z.ESPABA, "+
			    "   Z.OBSPROCESSO "+
		"	FROM   Z_CRM_ML001005 Z "+
			" WHERE  Z.OS = '"+os+"'"+
			"	   AND Z.NUMDESENHO LIKE '"+desenho+"%' AND Z.NUMDOCDELP LIKE '"+numdelp+"%' AND Z.ITEMEXCLUSIVO!=2 " +
		     "  UNION  ALL                                                "+
             "  SELECT     'Execução' AS EXEC2,							  "+
			 "  Y.NUMDESENHO, 											  "+
		     "  Y.REVISAODESENHO, 										  "+
		     "  Y.NUMDBI, 												  "+
		     "  Y.REVISAODBI, 											  "+
		     "  Y.NUMDOCDELP, 											  "+
		     "  Y.REVISAODOCDELP, 										  "+
		     "  Y.DESQTDE, 												  "+
		     "  Y.TOTALQTDE, 											  "+
		     "  Y.POSICAODESENHO, 										  "+
		     "  Y.DESCRICAO, 											  "+
		     "  Y.MATERIAL, 											  "+
		     "  Y.BITOLA, 												  "+
		     "  Y.LARGURA, 												  "+
		     "  Y.COMPRIMENTO, 											  "+
		     "  Y.ESPROSCA, 											  "+
		     "  Y.MASSALINEAR, 											  "+
		     "  isnull(Y.PESOUNLIQUIDO,0) PESOUNLIQUIDO, 				  "+
		     "  round((cast(isnull(replace(Y.PESOUNLIQUIDO,',','.'),0) as float)*cast(Y.DESQTDE as int)),4) PESOBRUTO, "+
		     "  Y.DIAMETROEXTERNO,										  "+
		     "  Y.DIAMETROINTERNO, 										  "+
		     "  Y.ESPESSURA, 											  "+
		     "  Y.DIAMETROEXTERNODISCO, 								  "+
		     "  Y.DIAMETROINTERNODISCO, 								  "+
		     "  Y.ALTURA, 												  "+
		     "  Y.LARGURAABA, 											  "+
		     "  Y.ESPALMA, 												  "+
		     "  Y.ESPABA, 												  "+
		     "  Y.OBSPROCESSO  from    Z_CRM_EX001005 Y 	  "+
			 "  WHERE Y.CODTRFPAI = '"+codtrfpai+"' 					  "+
			 "  AND Y.EXECUCAO = '"+numexec+"' 							  "+
			 "  AND Y.OS =  '"+os+"' AND Y.ITEMEXCLUSIVO!=2            						  "
			
				//	" ORDER BY CAST( '/'+REPLACE(INDICE,'.','/')+'/' AS hierarchyid ) "
			      
    log.info("dsListaDetalhada MY QUERY: " + myQuery)
    
    try {
    	
        var conn = ds.getConnection()
        var stmt = conn.createStatement()
        var rs = stmt.executeQuery(myQuery)
        var columnCount = rs.getMetaData().getColumnCount()
        
        while (rs.next()) {
        	
            if (!created) {
            	
                for (var i = 1; i <= columnCount; i++) {
                	
                	dsListaDetalhada.addColumn(rs.getMetaData().getColumnName(i))
                    
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
            
            dsListaDetalhada.addRow(Arr)
            
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
    
    return dsListaDetalhada;

	
}