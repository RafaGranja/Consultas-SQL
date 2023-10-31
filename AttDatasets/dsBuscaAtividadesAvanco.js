// BUSCA TODOS OS DADOS DAS RA'S GERADAS
function createDataset(fields, constraints, sortFields) {
  var dsBuscaAtividadesAvanco = DatasetBuilder.newDataset();
  var dataSource = "/jdbc/FluigDSRM";
  var ic = new javax.naming.InitialContext();
  var ds = ic.lookup(dataSource);
  var created = false;
  log.info("dsBuscaAtividadesAvanco");
  var myQuery = "";
  var os = "";
  var celula = "";
  var op = "";
  var data = "";
  var filial = "";
  if (constraints != null) {
    for (var i = 0; i < constraints.length; i++) {
      log.info(constraints[i].fieldName);
      if (constraints[i].fieldName == "OS") {
        if (
          !(
            constraints[i].initialValue == "" ||
            constraints[i].initialValue == null ||
            constraints[i].initialValue == undefined
          )
        ) {
          os = " AND K.CODCCUSTO='" + constraints[i].initialValue + "' ";
        }
      }
      if (constraints[i].fieldName == "CELULA") {
        if (
          !(
            constraints[i].initialValue == "" ||
            constraints[i].initialValue == null ||
            constraints[i].initialValue == undefined
          )
        ) {
          celula = " AND MCL.CODCELULA='" + constraints[i].initialValue + "' ";
        }
      }
      if (constraints[i].fieldName == "OP") {
        if (
          !(
            constraints[i].initialValue == "" ||
            constraints[i].initialValue == null ||
            constraints[i].initialValue == undefined
          )
        ) {
          var ops = constraints[i].initialValue;
          log.info("ops: " + ops);
          ops = ops.replace(";", ",");
          if (ops.indexOf(",") != -1) {
            ops = ops.split(",");
            var sent = "";
            for (var k = 0; k < ops.length; k++) {
              if (sent == "") {
                sent = "'" + ops[k] + "'";
              } else {
                sent = sent + ",'" + ops[k] + "'";
              }
            }
            log.info("sent: " + sent);
            op = " AND K.CODORDEM IN (" + sent + ") ";
          } else {
            op = " AND K.CODORDEM ='" + constraints[i].initialValue + "' ";
          }
        }
      }
      if (constraints[i].fieldName == "DATADE") {
        if (
          !(
            constraints[i].initialValue == "" ||
            constraints[i].initialValue == null ||
            constraints[i].initialValue == undefined
          )
        ) {
          data +=
            " AND CAST(Z.DTHRINICIAL AS DATE)>='" +
            constraints[i].initialValue +
            "' ";
        }
      }
      if (constraints[i].fieldName == "DATAATE") {
        if (
          !(
            constraints[i].initialValue == "" ||
            constraints[i].initialValue == null ||
            constraints[i].initialValue == undefined
          )
        ) {
          data +=
            " AND CAST(Z.DTHRFINAL AS DATE)<='" +
            constraints[i].initialValue +
            "' ";
        }
      }
      if (constraints[i].fieldName == "FILIAL") {
        if (
          !(
            constraints[i].initialValue == "" ||
            constraints[i].initialValue == null ||
            constraints[i].initialValue == undefined
          )
        ) {
          filial = " AND K.CODFILIAL=" + constraints[i].initialValue + " ";
        }
      }
    }
  }
  myQuery =
    " SELECT DISTINCT KA.CODCOLIGADA,K.CODCCUSTO,K.STATUS OP_STATUS,KA.STATUS ATV_STATUS,KA.CODESTRUTURA,K.CODORDEM,CASE WHEN MCL.TAG IS NULL THEN 'SEM TAG' ELSE MCL.TAG END AS TAG,K.DSCORDEM," +
    " A.DSCATIVIDADE,A.CODATIVIDADE,KAL.PRIORIDADE,KA.IDATVORDEM,KA.PERCENTUAL,MCL.CODCELULA CELULA,CASE WHEN ZP.CODATIVIDADE IS NULL THEN '0' ELSE ZP.NUMPLANOCORTE END AS PLANODECORTE, " +
    " CASE WHEN KAL.PRIORIDADE = (SELECT MAX(PRIORIDADE) FROM KATVORDEMCOMPL WHERE CODORDEM=KA.CODORDEM) THEN 1 ELSE 0 END AS ULTIMAATVOP,KA.TEMPO " +
    " FROM KORDEM K(NOLOCK) " +
    " INNER JOIN KATVORDEM KA (NOLOCK) ON " +
    " KA.CODCOLIGADA=K.CODCOLIGADA  " +
    " AND KA.CODFILIAL=K.CODFILIAL " +
    " AND KA.CODORDEM=K.CODORDEM " +
    " LEFT JOIN ZMDKATVORDEMPROGRAMACAO Z (NOLOCK) ON " +
    " K.CODCOLIGADA=Z.CODCOLIGADA " +
    " AND K.CODFILIAL=Z.CODFILIAL " +
    " AND K.CODORDEM=Z.CODORDEM " +
    " AND KA.CODESTRUTURA=Z.CODESTRUTURA " +
    " AND KA.IDATVORDEM=Z.IDATVORDEM " +
    " INNER JOIN KATVORDEMCOMPL KAL (NOLOCK) ON " +
    " KAL.CODCOLIGADA=KA.CODCOLIGADA " +
    " AND KAL.CODFILIAL=KA.CODFILIAL " +
    " AND KAL.CODORDEM=KA.CODORDEM " +
    " AND KAL.IDATVORDEM=KA.IDATVORDEM " +
    " INNER JOIN KATIVIDADE A (NOLOCK) ON " +
    " A.CODCOLIGADA=KA.CODCOLIGADA " +
    " AND A.CODFILIAL=KA.CODFILIAL " +
    " AND A.CODATIVIDADE=KA.CODATIVIDADE " +
    " INNER JOIN MPRJ M (NOLOCK) ON " +
    " M.CODCOLIGADA=KA.CODCOLIGADA " +
    " AND M.CODFILIAL=KA.CODFILIAL " +
    " AND M.CODPRJ=K.CODCCUSTO " +
    " AND M.POSICAO IN ( 1,4) " +
    " LEFT JOIN FLUIG.DBO.Z_CRM_EX001005COMPL MCL ON "+
    " MCL.CODCOLIGADA=KA.CODCOLIGADA " +
    " AND MCL.CODFILIAL=KA.CODFILIAL " +
    " AND MCL.CODORDEM=KA.CODORDEM " +
    " LEFT JOIN ZMDPLANOAPROVEITAMENTOCORTE ZP " +
    " ON ZP.CODCOLIGADA=KA.CODCOLIGADA " +
    " AND ZP.CODFILIAL=KA.CODFILIAL " +
    " AND ZP.CODORDEM=KA.CODORDEM " +
    " AND ZP.CODATIVIDADE=KA.CODATIVIDADE " +
    " WHERE KA.STATUS IN (2,3,5) AND K.STATUS NOT IN (5,6) AND  ( Z.STATUS IN (0,1) OR Z.STATUS IS NULL )  " +
    filial +
    os +
    celula +
    op +
    data +
    " UNION " +
    " SELECT DISTINCT KA.CODCOLIGADA,K.CODCCUSTO,K.STATUS OP_STATUS,KA.STATUS ATV_STATUS,KA.CODESTRUTURA,K.CODORDEM,CASE WHEN MCL.TAG IS NULL THEN 'SEM TAG' ELSE MCL.TAG END AS TAG,K.DSCORDEM," +
    " A.DSCATIVIDADE,A.CODATIVIDADE,KAL.PRIORIDADE,KA.IDATVORDEM,KA.PERCENTUAL,MCL.CODCELULA CELULA,CASE WHEN ZP.NUMPLANOCORTE IS NULL THEN '0' ELSE ZP.NUMPLANOCORTE END AS PLANODECORTE, " +
    " CASE WHEN KAL.PRIORIDADE = (SELECT MAX(PRIORIDADE) FROM KATVORDEMCOMPL WHERE CODORDEM=KA.CODORDEM) THEN 1 ELSE 0 END AS ULTIMAATVOP,KA.TEMPO " +
    " FROM KORDEM K (NOLOCK) " +
    " INNER JOIN KATVORDEM KA (NOLOCK) ON " +
    " KA.CODCOLIGADA=K.CODCOLIGADA  " +
    " AND KA.CODFILIAL=K.CODFILIAL " +
    " AND KA.CODORDEM=K.CODORDEM " +
    " LEFT JOIN  KMAOOBRAALOC Z (NOLOCK) ON " +
    " K.CODCOLIGADA=Z.CODCOLIGADA " +
    " AND K.CODFILIAL=Z.CODFILIAL " +
    " AND K.CODORDEM=Z.CODORDEM " +
    " AND KA.IDATVORDEM=Z.IDATVORDEM " +
    " INNER JOIN KATVORDEMCOMPL KAL (NOLOCK) ON " +
    " KAL.CODCOLIGADA=KA.CODCOLIGADA " +
    " AND KAL.CODFILIAL=KA.CODFILIAL " +
    " AND KAL.CODORDEM=KA.CODORDEM " +
    " AND KAL.IDATVORDEM=KA.IDATVORDEM " +
    " INNER JOIN KATIVIDADE A (NOLOCK) ON " +
    " A.CODCOLIGADA=KA.CODCOLIGADA " +
    " AND A.CODFILIAL=KA.CODFILIAL " +
    " AND A.CODATIVIDADE=KA.CODATIVIDADE " +
    " INNER JOIN MPRJ M (NOLOCK) ON " +
    " M.CODCOLIGADA=KA.CODCOLIGADA " +
    " AND M.CODFILIAL=KA.CODFILIAL " +
    " AND M.CODPRJ=K.CODCCUSTO " +
    " AND M.POSICAO IN ( 1,4) " +
    " LEFT JOIN FLUIG.DBO.Z_CRM_EX001005COMPL MCL ON "+
    " MCL.CODCOLIGADA=KA.CODCOLIGADA " +
    " AND MCL.CODFILIAL=KA.CODFILIAL " +
    " AND MCL.CODORDEM=KA.CODORDEM " +
    " LEFT JOIN ZMDPLANOAPROVEITAMENTOCORTE ZP " +
    " ON ZP.CODCOLIGADA=KA.CODCOLIGADA " +
    " AND ZP.CODFILIAL=KA.CODFILIAL " +
    " AND ZP.CODORDEM=KA.CODORDEM " +
    " AND ZP.CODATIVIDADE=KA.CODATIVIDADE " +
    " WHERE KA.STATUS IN (2,3,5) AND K.STATUS NOT IN (5,6) AND  ( Z.EFETIVADO=0 OR Z.EFETIVADO IS NULL ) " +
    filial +
    os +
    celula +
    op +
    data;
  log.info("dsBuscaAtividadesAvanco MY QUERY: " + myQuery);
  try {
    var conn = ds.getConnection();
    var stmt = conn.createStatement();
    var rs = stmt.executeQuery(myQuery);
    var columnCount = rs.getMetaData().getColumnCount();
    while (rs.next()) {
      if (!created) {
        for (var i = 1; i <= columnCount; i++) {
          dsBuscaAtividadesAvanco.addColumn(rs.getMetaData().getColumnName(i));
        }
        created = true;
      }
      var Arr = new Array();
      for (var i = 1; i <= columnCount; i++) {
        var obj = rs.getObject(rs.getMetaData().getColumnName(i));
        if (null != obj) {
          Arr[i - 1] = rs
            .getObject(rs.getMetaData().getColumnName(i))
            .toString();
        } else {
          Arr[i - 1] = "null";
        }
      }
      dsBuscaAtividadesAvanco.addRow(Arr);
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
  return dsBuscaAtividadesAvanco;
}
