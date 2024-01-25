// BUSCA AS INFORMAÇÕES CADASTRADAS NO METADADOS DE RELATÓRIOS FLUIG
function createDataset(fields, constraints, sortFields) {

var dsMapaRastreabilidade = DatasetBuilder.newDataset();
var dataSource = "/jdbc/FluigDSRM";
var ic = new javax.naming.InitialContext();
var ds = ic.lookup(dataSource);
var created = false;
    
var os = ""
var filial = ""
var exec = ""
var codtrf = ""

 
if (constraints != null) {
    	
        for (var i = 0; i < constraints.length; i++) {
        	
        	if (constraints[i].fieldName == "FILIAL") {
            	
        		filial = constraints[i].initialValue
            	
            }
        	if (constraints[i].fieldName == "OS") {
            	
        		os = constraints[i].initialValue
            	
            }
        	if (constraints[i].fieldName == "EXEC") {
        		if (constraints[i].initialValue!="" && constraints[i].initialValue!=null && constraints[i].initialValue!=undefined ){
        			
        			exec = " AND KOL.NUMEXEC = "+constraints[i].initialValue+" "
        			
        		}
            	
        		
            }
        	if (constraints[i].fieldName == "CODTRF") {
            	
        		codtrf = constraints[i].initialValue
            	
            }      
       	
        }
        
	}
    
    var myQuery ="SELECT DISTINCT                                                             "+             
    "    	 '' NOME,	 																	  "+
    "        ML.NUMDESENHO + ' - Pos. ' +  ML.POSICAODESENHO + ' - ' + ML.DESCRICAO DESCRICAO,"+
    "    	 ML.NUMDESENHO DESENHO,											  				  "+
    "    	 ML.POSICAODESENHO POSICAO,										  				  "+
    "    	 KI.QTDEPLANEJADA QUANTIDADE,	  												  "+
    "    	 (SELECT NOMEFANTASIA FROM TPRD TP									  			  "+
    "    	 WHERE KMP.IDPRODUTO = TP.IDPRD) DESCPRODUTO,						  			  "+
    "    	 (SELECT NUMLOTE FROM TLOTEPRD TL									  			  "+
    "    	 WHERE KMPL.IDLOTE = TL.IDLOTE) NUMLOTE,							  			  "+
    "    	 '' OBS															  				  "+
    "    	 FROM FLUIG.DBO.Z_CRM_ML001005 ML (NOLOCK) 						  				  "+
    "    	 INNER JOIN KITEMORDEM KI (NOLOCK) 								  				  "+
    "    	 ON KI.CODCOLIGADA = ML.CODCOLIGADA								  				  "+
    "    	 AND KI.CODFILIAL = ML.CODFILIAL									  			  "+
    "    	 AND KI.CODESTRUTURA = ML.CODIGOPRD COLLATE LATIN1_GENERAL_CI_AS	  			  "+
    "    	 INNER JOIN KORDEMCOMPL KOL (NOLOCK)								  			  "+
    "    	 ON KI.CODCOLIGADA = KOL.CODCOLIGADA								  			  "+
    "    	 AND KI.CODFILIAL = KOL.CODFILIAL									  			  "+
    "    	 AND KI.CODORDEM = KOL.CODORDEM                                      			  "+
    "    	 INNER JOIN KORDEM KO	                                              			  "+
    "    	 ON KI.CODCOLIGADA = KO.CODCOLIGADA								  				  "+
    "    	 AND KI.CODFILIAL = KO.CODFILIAL									  			  "+
    "    	 AND KI.CODORDEM = KO.CODORDEM                                       			  "+			      
    "    	 INNER JOIN KATVORDEM KA (NOLOCK) 									  			  "+
    "    	 ON  KA.CODCOLIGADA = KI.CODCOLIGADA								  			  "+
    "    	 AND KA.CODFILIAL = KI.CODFILIAL									  			  "+
    "    	 AND KA.CODORDEM = KI.CODORDEM										  			  "+
    "    	 AND KA.CODESTRUTURA = KI.CODESTRUTURA								  			  "+
    "    	 AND KA.CODMODELO = KI.CODMODELO									  			  "+
    "    	 INNER JOIN KATVORDEMMP KMP (NOLOCK)								  			  "+
    "    	 ON KMP.CODCOLIGADA = KA.CODCOLIGADA								  			  "+
    "    	 AND KMP.CODFILIAL = KA.CODFILIAL									  			  "+
    "    	 AND KMP.CODORDEM = KA.CODORDEM								      				  "+
    "    	 AND KMP.CODESTRUTURA = KA.CODESTRUTURA							  				  "+
    "    	 AND KMP.CODMODELO = KA.CODMODELO									  			  "+	
    "    	 AND KMP.IDATIVIDADE = KA.IDATVORDEM								  			  "+
    "    	 INNER JOIN KATVORDEMMPLOTE KMPL (NOLOCK)							  			  "+
    "    	 ON KMP.IDATVORDEMMATERIAPRIMA = KMPL.IDATVORDEMMATERIAPRIMA		  			  "+
    " 																	 					  "+
    " WHERE ML.COMPORLISTA = 'SIM'	AND ML.ITEMEXCLUSIVO!=2									 					  "+
    " AND KMP.EFETIVADO = 1												 					  "+
    " AND ML.OS LIKE '%"+os+ "' "+									      
    " AND ML.CODFILIAL = "+filial+							  
    " AND KO.RESPONSAVEL LIKE CONCAT('%;%-%/','"+codtrf+"','%')          					  "+										  
    " "+exec+							  
    " ORDER BY NUMLOTE													 " 


    
    log.info("QUERY dsMapaRastreabilidade: " + myQuery);
    
    try {
    	
        var conn = ds.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery(myQuery);
        var columnCount = rs.getMetaData().getColumnCount();
        
        while (rs.next()) {
        	
            if (!created) {
            	
                for (var i = 1; i <= columnCount; i++) {
                	
                	dsMapaRastreabilidade.addColumn(rs.getMetaData().getColumnName(i));
                	
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
            
            dsMapaRastreabilidade.addRow(Arr);
            
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
    
    return dsMapaRastreabilidade;
	
}