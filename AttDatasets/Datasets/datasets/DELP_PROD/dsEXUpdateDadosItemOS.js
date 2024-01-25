// ATUALIZA OS DADOS DE UM DETERMINADO ITEM DA ESTRUTURA
function createDataset(fields, constraints, sortFields) {

	log.info("ENTREI NO DATASET dsEXUpdateDadosItemOS")
	
	var dsEXUpdateDadosItemOS = DatasetBuilder.newDataset();
    var dataSource = "/jdbc/FluigDS";
    var ic = new javax.naming.InitialContext();
    var ds = ic.lookup(dataSource);
    var created = false;
   
    var posicaoIndice=""
    var posicaoDesenho=""
    var indiceAntigo=""
    var nivel=""
	var numDbi=""
	var revisaoDbi=""
	var numDesenho=""
	var revisaoDesenho=""
	var desQtde=""
	var undMedida=""
	var totalQtde=""
	var descricao=""
	var bitola=""
	var espessura=""
	var largura=""
	var massaLinear=""
	var espRosca=""
	var diametroInterno=""
	var diametroExterno=""
	var comprimento=""
	var produtoRM=""
	var idprd=""
	var codigoPrd=""
	var dataRevisao=""
	var obsDesenho=""
	var pesoBruto=""
	var pesoLiquido=""
	var perimetroCorte=""
	var areaPintura=""
	var obsProcesso=""
	var obsGeral=""
	var tipoDesenho=""
	var areaSecao=""
	var altura=""
	var larguraAba=""
	var espAlma=""
	var espAba=""
	var material=""
	var indice=""
	var comporLista=""
	var numOS=""
	var idCriacao=""
	var codigoTarefa=""
	var codTrfItem=""
	var idTrfItem=""
	var nomeTrfItem=""
	var numDocDelp = ""
	var revisaoDocDelp = ""
	var diametroExtDisco = ""
	var diametroIntDisco = ""
	var codTrfPai = ""
	var idtrfPai = ""
	var nomeTrfPai = ""
	var pesoUnit = ""
	var pesoUnLiq = ""
	var opsUnitarias = ""
	var execucao = ""
	var qtdeUnComp = ""
	var reccreatedby = ""
	var reccreatedon = " , RECCREATEDON=GETDATE() "
	var recmodifiedby = ""
	var recmodifiedon = " , RECMODIFIEDON=GETDATE() "
	var itemderetorno = ""
				
    log.info("VOU SALVAR AS CONSTRAINTS")
    	
    if (constraints != null) {
    	
        for (var i = 0; i < constraints.length; i++) {
        	
        	if(constraints[i].fieldName == "OS"){
        		
        		numOS = " WHERE OS='"+constraints[i].initialValue+"'";
        	
        	} else if(constraints[i].fieldName == "IDCRIACAO"){
        		
        		idCriacao = " AND IDCRIACAO='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "POSICAOINDICE"){
        		
        		posicaoIndice = " SET POSICAOINDICE='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "POSICAODESENHO"){
        		
        		posicaoDesenho = ", POSICAODESENHO='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "INDICEANTIGO"){
        		
        		indiceAntigo = ", INDICEANTIGO='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "NIVEL"){
        		
        		nivel = ", NIVEL='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "NUMDBI"){
        		
        		numDbi = ", NUMDBI='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "REVISAODBI"){
        		
        		revisaoDbi = ", REVISAODBI='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "NUMDESENHO"){
        		
        		numDesenho = ", NUMDESENHO='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "REVISAODESENHO"){
        		
        		revisaoDesenho = ", REVISAODESENHO='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "DESQTDE"){
        		
        		desQtde = ", DESQTDE='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "UNDMEDIDA"){
        		
        		undMedida = ", UNDMEDIDA='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "TOTALQTDE"){
        		
        		totalQtde = ", TOTALQTDE='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "DESCRICAO"){
        		
        		descricao = ", DESCRICAO='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "BITOLA"){
        		
        		bitola = ", BITOLA='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "ESPESSURA"){
        		
        		espessura = ", ESPESSURA='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "LARGURA"){
        		
        		largura = ", LARGURA='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "MASSALINEAR"){
        		
        		massaLinear = ", MASSALINEAR='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "ESPROSCA"){
        		
        		espRosca = ", ESPROSCA='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "DIAMETROEXTERNO"){
        		
        		diametroExterno = ", DIAMETROEXTERNO='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "DIAMETROINTERNO"){
        		
        		diametroInterno = ", DIAMETROINTERNO='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "COMPRIMENTO"){
        		
        		comprimento = ", COMPRIMENTO='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "PRODUTORM"){
        		
        		produtoRM = ", PRODUTORM='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "IDPRD"){
        		
        		idprd = ", IDPRD='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "CODIGOPRD"){
        		
        		codigoPrd = ", CODIGOPRD='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "DATAREVISAO"){
        		
        		dataRevisao = ", DATAREVISAO='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "OBSERVACOESDESENHO"){
        		
        		obsDesenho = ", OBSERVACOESDESENHO='"+constraints[i].initialValue+"'";
        	
        	} else if(constraints[i].fieldName == "QTDEUNCOMP"){
        		
        		qtdeUnComp = ", QTDEUNCOMP='"+constraints[i].initialValue+"'";
        	
        	} else if(constraints[i].fieldName == "PESOBRUTO"){
        		
        		pesoBruto = ", PESOBRUTO='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "PESOLIQUIDO"){
        		
        		pesoLiquido = ", PESOLIQUIDO='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "PESOUNLIQUIDO"){
        		
        		pesoUnLiq = ", PESOUNLIQUIDO='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "PERIMETROCORTE"){
        		
        		perimetroCorte = ", PERIMETROCORTE='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "AREAPINTURA"){
        		
        		areaPintura = ", AREAPINTURA='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "OBSPROCESSO"){
        		
        		obsProcesso = ", OBSPROCESSO='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "OBSGERAL"){
        		
        		obsGeral = ", OBSGERAL='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "TIPODESENHO"){
        		
        		tipoDesenho = ", TIPODESENHO='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "AREASECAO"){
        		
        		areaSecao = ", AREASECAO='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "ALTURA"){
        		
        		altura = ", ALTURA='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "LARGURAABA"){
        		
        		larguraAba = ", LARGURAABA='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "ESPALMA"){
        		
        		espAlma = ", ESPALMA='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "ESPABA"){
        		
        		espAba = ", ESPABA='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "MATERIAL"){
        		
        		material = ", MATERIAL='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "INDICE"){
        		
        		indice = ", INDICE='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "COMPORLISTA"){
        		
        		comporLista = ", COMPORLISTA='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "CODIGOTAREFADESC"){
        		
        		codigoTarefa = ", CODIGOTAREFADESC='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "CODTRFITEM"){
        		
        		codTrfItem = ", CODTRFITEM='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "IDTRFITEM"){
        		
        		idTrfItem = ", IDTRFITEM='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "NOMETRFITEM"){
        		
        		nomeTrfItem = ", NOMETRFITEM='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "NUMDOCDELP"){
        		
        		numDocDelp = ", NUMDOCDELP='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "REVISAODOCDELP"){
        		
        		revisaoDocDelp = ", REVISAODOCDELP='"+constraints[i].initialValue+"'";
        	
        	} else if(constraints[i].fieldName == "DIAMETROEXTERNODISCO"){
        		
        		diametroExtDisco = ", DIAMETROEXTERNODISCO='"+constraints[i].initialValue+"'";
        	
        	}  else if(constraints[i].fieldName == "DIAMETROINTERNODISCO"){
        		
        		diametroIntDisco = ", DIAMETROINTERNODISCO='"+constraints[i].initialValue+"'";
        	
        	} else if(constraints[i].fieldName == "CODTRFPAI"){
        		
        		codTrfPai = ", CODTRFPAI='"+constraints[i].initialValue+"'";
        	
        	} else if(constraints[i].fieldName == "IDTRFPAI"){
        		
        		idTrfPai = ", IDTRFPAI='"+constraints[i].initialValue+"'";
        	
        	} else if(constraints[i].fieldName == "NOMETRFPAI"){
        		
        		nomeTrfPai = ", NOMETRFPAI='"+constraints[i].initialValue+"'";
        	
        	} else if(constraints[i].fieldName == "PESOUNITARIO"){
        		
        		pesoUnit = ", PESOUNITARIO='"+constraints[i].initialValue+"'";
        	
        	} else if(constraints[i].fieldName == "OPSUNITARIAS"){
        		
        		opsUnitarias = ", OPSUNITARIAS='"+constraints[i].initialValue+"'";
        	
        	} else if(constraints[i].fieldName == "EXECUCAO"){
        		
        		execucao = " AND EXECUCAO="+constraints[i].initialValue+"";
        	
        	} else if(constraints[i].fieldName == "RECMODIFIEDBY"){
        		
        		recmodifiedby = ", RECMODIFIEDBY='"+constraints[i].initialValue+"' ";
        	
        	} else if(constraints[i].fieldName == "RECCREATEDBY"){
        		
        		reccreatedby = ", RECCREATEDBY='"+constraints[i].initialValue+"' ";
        	
        	}
     	   	else if(constraints[i].fieldName == "ITEMDERETORNO"){
          		
          		itemderetorno = ", ITEMDERETORNO="+constraints[i].initialValue+" ";
          	
          	}
        	
        }
        
    }  

    log.info("VOU MONTAR A QUERY")
    
    var myQuery = "UPDATE Z_CRM_EX001005 " +
    			  "		"+posicaoIndice+" "+posicaoDesenho+" "+indiceAntigo+" "+nivel+" "+numDbi+" "+revisaoDbi+" "+numDesenho+" "+revisaoDesenho+" "+desQtde+" "+undMedida+" "+
    			  totalQtde+" "+descricao+" "+bitola+" "+espessura+" "+largura+" "+massaLinear+" "+espRosca+" "+diametroInterno+" "+diametroExterno+" "+comprimento+" "+
    			  produtoRM+" "+idprd+" "+codigoPrd+" "+qtdeUnComp+" "+
    			  dataRevisao+" "+obsDesenho+" "+pesoBruto+" "+pesoLiquido+" "+perimetroCorte+" "+areaPintura+" "+obsProcesso+" "+obsGeral+" "+
    			  " "+tipoDesenho+" "+areaSecao+" "+altura+" "+larguraAba+" "+espAlma+" "+espAba+" "+material+" "+indice+" "+comporLista+" "+codigoTarefa+" "+codTrfItem+" "+
    			  idTrfItem+" "+nomeTrfItem+" "+numDocDelp+" "+revisaoDocDelp+" "+diametroExtDisco+" "+diametroIntDisco+" "+pesoUnit+" "+opsUnitarias+" "+pesoUnLiq+" "+
	  			  " "+recmodifiedby+" "+recmodifiedon+" "+itemderetorno+" "+
    			  //codTrfPai+" "+idTrfPai+" "+nomeTrfPai+" "+
    			  numOS+" "+idCriacao+" "+execucao
    
    			  
    log.info("QUERY dsEXUpdateDadosItemOS: " + myQuery);
    
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
   
    return dsEXUpdateDadosItemOS;
	
}