// BUSCA TODOS OS DADOS DAS RA'S GERADAS
function createDataset(fields, constraints, sortFields) {

	var dsLinkDocumento = DatasetBuilder.newDataset()
    
    log.info("dsLinkDocumento")

    var doc = ""
    	
	if (constraints != null) {
		
	    for (var i = 0; i < constraints.length; i++) {
	
			if (constraints[i].fieldName == "DOC") {
			        	
	        	var doc = constraints[i].initialValue
			        	
	        }
	        
	    }
	    
	}
	
	
	WCMAPI.Create({
	    url: WCMAPI.serverURL+"/api/public/2.0/documents/getDownloadURL/"+84407,
	    success: function(data){
	             var xmlResp=parser.parseFromString(data.firstChild.innerHTML,"text/xml");
	             console.log("Documento Publicado: " + xmlResp.getElementsByTagName("documentId")[0].innerHTML);
	    }
	});

    var start = new Date().getTime();
    while (new Date().getTime() < start + 3000);
    
    try {	
            	
        for (var i = 1; i <= columnCount; i++) {
        	
        	dsRelatorioRaChapas.addColumn("RETORNO")
            
        }
               
        var Arr = new Array(data)

        dsLinkDocumento.addRow(Arr)

    } catch (e) {
    	
        log.error("ERRO==============> " + e.message)
        
    }
    
    return dsLinkDocumento;
	
}