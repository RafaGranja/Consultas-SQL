
SELECT * FROM KORDEM WHERE CODORDEM IN ('78092/24','79182/24','79183/24')
SELECT * FROM KORDEMCOMPL WHERE CODORDEM IN ('78092/24','79182/24','79183/24')
SELECT * FROM KITEMORDEM WHERE CODORDEM IN ('78092/24','79182/24','79183/24')
SELECT * FROM KATVORDEM WHERE CODORDEM IN ('78092/24','79182/24','79183/24')
SELECT * FROM KATVORDEMCOMPL WHERE CODORDEM IN ('78092/24','79182/24','79183/24')
SELECT * FROM KATVORDEMMP WHERE CODORDEM IN ('78092/24','79182/24','79183/24')
SELECT * FROM KATVORDEMMPLOTE WHERE IDATVORDEMMATERIAPRIMA IN (2599887,2514504)
SELECT * FROM KATVORDEMCUSTOMAOOBRA WHERE CODORDEM IN ('78092/24','79182/24','79183/24')
SELECT * FROM KATVORDEMCUSTOPOSTO WHERE CODORDEM IN ('78092/24','79182/24','79183/24')
SELECT * FROM KATVORDEMCUSTOEQP WHERE CODORDEM IN ('78092/24','79182/24','79183/24')
SELECT * FROM KATVORDEMHISTORICOCUSTO WHERE CODORDEM IN ('78092/24','79182/24','79183/24')
SELECT * FROM KLOGAPONTAMENTO WHERE CODORDEM IN ('78092/24','79182/24','79183/24')
SELECT * FROM KLOGATIVIDADE WHERE CODORDEM IN ('78092/24','79182/24','79183/24')
SELECT * FROM KLOGPRODUCAO WHERE CODORDEM IN ('78092/24','79182/24','79183/24')
SELECT * FROM KPARADAATV WHERE CODORDEM IN ('78092/24','79182/24','79183/24')
SELECT * FROM KMAOOBRAALOC WHERE CODORDEM IN ('78092/24','79182/24','79183/24')
SELECT * FROM KATVORDEMPROCESSO WHERE CODORDEM IN ('78092/24','79182/24','79183/24')
SELECT * FROM CORPORE.DBO.ZMDPLANOAPROVEITAMENTOCORTE WHERE CODORDEM IN ('78092/24','79182/24','79183/24')
SELECT * FROM CORPORE.DBO.ZMDKATVORDEMPROGRAMACAO WHERE CODORDEM IN ('78092/24','79182/24','79183/24')
SELECT * FROM Z_DELP_NECESSIDADEPAPC WHERE CODORDEM IN ('78092/24','79182/24','79183/24')

SELECT * FROM KESTRUTURA WHERE CODESTRUTURA IN ('03.023.0153934','03.023.0153934','03.023.0154526')
SELECT * FROM KMODELO WHERE CODESTRUTURA IN ('03.023.0153934','03.023.0153934','03.023.0154526')
SELECT * FROM KESTRUTURACOMPL WHERE CODESTRUTURA IN ('03.023.0153934','03.023.0153934','03.023.0154526')
SELECT * FROM KATVESTRUTURA WHERE CODESTRUTURA IN ('03.023.0153934','03.023.0153934','03.023.0154526')
SELECT * FROM KATVESTRUTURACOMPL WHERE CODESTRUTURA IN ('03.023.0153934','03.023.0153934','03.023.0154526')
SELECT * FROM KLINHAEST WHERE CODESTRUTURA IN ('03.023.0153934','03.023.0153934','03.023.0154526')
SELECT * FROM KATVESTPOSTO WHERE CODESTRUTURA IN ('03.023.0153934','03.023.0153934','03.023.0154526')
SELECT * FROM KCOMPONENTECOMPL WHERE CODESTRUTURA IN ('03.023.0153934','03.023.0153934','03.023.0154526')
SELECT * FROM KCOMPONENTE WHERE CODESTRUTURA IN ('03.023.0153934','03.023.0153934','03.023.0154526')
SELECT * FROM KCOMPSUBSTITUTO WHERE CODESTRUTURA IN ('03.023.0153934','03.023.0153934','03.023.0154526')


SELECT * FROM Z_CRM_ML001019 WHERE OSCOMPONENTES='3.07793.32.001'
SELECT * FROM Z_CRM_ML001021 WHERE OSPROCESSO='3.07793.32.001'
SELECT * FROM Z_CRM_ML001005 WHERE OS='3.07793.32.001'
SELECT * FROM Z_CRM_EX001019 WHERE OSCOMPONENTES='3.07793.32.001'
SELECT * FROM Z_CRM_EX001021 WHERE OSPROCESSO='3.07793.32.001'
SELECT * FROM Z_CRM_EX001005COMPL WHERE OS='3.07793.32.001'
SELECT * FROM Z_CRM_EX001005 WHERE OS='3.07793.32.001'

SELECT * FROM TPRDCOMPL WHERE IDPRD IN (153934,153934,154526,153911,154523) AND CODCOLIGADA=1
SELECT * FROM TPRDFIL WHERE IDPRD IN (153934,153934,154526,153911,154523)  AND CODCOLIGADA=1
SELECT * FROM TPRDFISCAL WHERE IDPRD IN (153934,153934,154526,153911,154523)  AND CODCOLIGADA=1
SELECT * FROM TPRDHISTORICO WHERE IDPRD IN (153934,153934,154526,153911,154523)  AND CODCOLIGADA=1
SELECT * FROM TPRDUND WHERE IDPRD IN (153934,153934,154526,153911,154523)  AND CODCOLIGADA=1
SELECT * FROM TPRODUTODEF WHERE IDPRD IN (153934,153934,154526,153911,154523)  AND CODCOLIGADA=1
SELECT * FROM TPRODUTO WHERE IDPRD IN (153934,153934,154526,153911,154523)  AND CODCOLPRD=1