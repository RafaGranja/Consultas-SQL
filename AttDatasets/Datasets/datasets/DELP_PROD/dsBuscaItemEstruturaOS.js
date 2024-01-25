// BUSCA TODOS OS DADOS CADASTRADOS DE UM DETERMINADO ITEM DA ESTRUTURA 
function createDataset(fields, constraints, sortFields) {

	var dsBuscaItemEstruturaOS = DatasetBuilder.newDataset();
    var dataSource = "/jdbc/FluigDS";
    var ic = new javax.naming.InitialContext();
    var ds = ic.lookup(dataSource);
    var created = false;
    
    var numOS = ""
    var idCriacao = ""
    var nivel = ""
    
    if (constraints != null) {
    	
        for (var i = 0; i < constraints.length; i++) {
        	
        	if(constraints[i].fieldName == "OS"){
        		
        		numOS = constraints[i].initialValue;
        		
        	}
        	if(constraints[i].fieldName == "IDCRIACAO"){
        		
        		idCriacao = " AND IDCRIACAO='"+constraints[i].initialValue+"' "
        		
        	} 
        	if(constraints[i].fieldName == "NIVEL"){
        		
        		nivel = " AND NIVEL='"+constraints[i].initialValue+"'"
        		
        	} 
        	
        }
        
    }  
    
    var myQuery = "SELECT ID ,companyid ,cardid ,documentid ,version ,tableid ,TITULOITEM ,NIVEL ,POSICAO ,DESCRICAO ,NUMDESENHO ,REVISAODESENHO ," +
    		"NUMDBI ,REVISAODBI ,DESQTDE ,TOTALQTDE ,BITOLAESP ,LARGURAMASSA ,ESPLARGURAROSCA ,COMPRIMENTO ,MATERIAL ,PESO ,OBSERVACOES ,TIPO ,SEQ ," +
    		"masterid ,PESOMATERIAL ,OS ,DATAREVISAO ,PESOBRUTO ,PESOLIQUIDO ,OBSERVACOESDESENHO ,PERIMETROCORTE ,AREAPINTURA ,OBSPROCESSO ,OBSGERAL ," +
    		"QUANTIDADEMATERIAL ,TIPODESENHO ,POSICAOCOMPLETA ,ESPESSURA ,BITOLA ,LARGURA ,MASSALINEAR ,DIAMETROEXTERNO ,DIAMETROINTERNO ,ESPROSCA ," +
    		"POSICAOCOMPLETAANTIGA ,NOVOMATERIAL ,MATERIAL_ZOOM ,IDMATERIAL ,CODIGOPRD ,INDICE ,POSICAOESTRUTURA ,POSICAODESENHO ,AREASECAO ,ALTURA ," +
    		"LARGURAMESA ,ESPALMA ,ESPMESA ,POSICAOINDICE ,INDICEANTIGO ,IDCRIACAO ,PRODUTORM ,IDPRD ,EXPANSOR ,CODIGOPRDMATERIAL ,CODTRFOS ,UNDMEDIDA ," +
    		"ORDEM ,LARGURAABA ,ESPABA ,INTEGRADO ,DSCTRFOS ,SOLICITACAO ,PAIDETALHADO ,COMPORLISTA ,CODTRFITEM ,IDTRFITEM ,NOMETRFITEM ,CODIGOTAREFADESC ," +
    		"IDPRJOS ,DESCOS ,DETALHADO ,NUMDOCDELP ,REVISAODOCDELP ,CODCOLIGADA ,DIAMETROEXTERNODISCO ,DIAMETROINTERNODISCO ,CHECKUSINAGEM ,CHECKCALDERARIA ," +
    		"CODFILIAL ,CODTRFPAI ,ISNULL(EXECINTEGRADAS,0) EXECINTEGRADAS ,IDTRFPAI ,NOMETRFPAI ,EXECUCOES ,PESOUNITARIO ,OPSUNITARIAS ," +
    		"QTDEUNCOMP ,IDCRIACAOPAI ,RECCREATEDBY ,RECCREATEDON ,RECMODIFIEDBY ,RECMODIFIEDON ,PESOUNLIQUIDO, ITEMDERETORNO " +
    		"FROM Z_CRM_ML001005 WHERE ITEMEXCLUSIVO!=2 AND OS='"+numOS+"' "+idCriacao+" "+nivel
    
    log.info("QUERY dsBuscaItemEstruturaOS: " + myQuery);
    
    try {
    	
        var conn = ds.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery(myQuery);
        var columnCount = rs.getMetaData().getColumnCount();
        
        while (rs.next()) {
        	
            if (!created) {
            	
                for (var i = 1; i <= columnCount; i++) {
                	
                	dsBuscaItemEstruturaOS.addColumn(rs.getMetaData().getColumnName(i));
                	
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
            
            dsBuscaItemEstruturaOS.addRow(Arr);
            
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
    
    return dsBuscaItemEstruturaOS;
	
}