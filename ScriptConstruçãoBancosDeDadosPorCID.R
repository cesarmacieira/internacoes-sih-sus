####===============================================================================
#### Trabalho Internações SIH SUS - Construção dos bancos de dados por internações
####===============================================================================
####=============================
#### Preparando o R para análise
####=============================
rm(list=ls(all=T))#Limpar ambiente/histórico
tryCatch({setwd("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus")},
         error = function(e) { setwd("D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus") })

####=================================
#### Instalando e carregando pacotes
####=================================
if(!require(openxlsx)){ install.packages("openxlsx"); require(openxlsx)}#Ler e exportar excel
if(!require(purrr)){ install.packages("purrr"); require(purrr)}#Programação funcional
if(!require(tidyverse)){ install.packages("tidyverse"); require(tidyverse)}#Manipulação de dados
if(!require(stringr)){ install.packages("stringr"); require(stringr)}
if(!require(read.dbc)){ devtools::install_github("danicat/read.dbc"); require(read.dbc)}

####=========
#### Funções
####=========
DescritivaCat = function(x){
  tabela = cbind(table(x), prop.table(table(x)))
  colnames(tabela) = c("Freq. Absoluta (N)","Freq. Relativa (%)")
  return(tabela)
}

DescritivaNum = function(x, more = F) {
  stats = list();
  clean.x = x[!is.na(x)]
  stats$N_validos = round(length(clean.x),3)
  stats$Média = round(mean(clean.x),3)
  stats$Var = round(var(clean.x),3)
  stats$D.P = round(sd(clean.x),3)
  stats$Mín. = round(min(clean.x),3)
  stats$Q1 = round(fivenum(clean.x)[2],3)
  stats$Q2 = round(fivenum(clean.x)[3],3)
  stats$Q3 = round(fivenum(clean.x)[4],3)
  stats$Máx. = round(max(clean.x),3)
  t1 = unlist(stats)
  names(t1) = c("N","Média","Variância","D.P.","Mínimo","1ºQ","2ºQ","3ºQ","Máximo")
  t1
}

basic.stats = function(x, more = F) {
  stats = list()
  clean.x = x[!is.na(x)]
  stats$N_validos = round(length(clean.x),3)
  stats$Média = round(mean(clean.x),3)
  stats$Var = round(var(clean.x),3)
  stats$D.P = round(sd(clean.x),3)
  stats$E.P = round(sd(clean.x)/sqrt(length(clean.x)),3)
  stats$Min = round(min(clean.x),3)
  stats$Q1 = round(fivenum(clean.x)[2],3)
  stats$Q2 = round(fivenum(clean.x)[3],3)
  stats$Q3 = round(fivenum(clean.x)[4],3)
  stats$Max = round(max(clean.x),3)
  t1 = unlist(stats)
  names(t1) = c("N válidos","Média","Variância","D.P.","E.P.","Mínimo","1ºQ","2ºQ","3ºQ","Máximo")
  t1
}

####===============================
#### Carregando os bancos de dados 
####===============================
dados_AC = arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/AC/dados_empilhados_AC.parquet')
dados_AL = arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/AL/dados_empilhados_AL.parquet')
dados_AM = arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/AM/dados_empilhados_AM.parquet')
dados_AP = arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/AP/dados_empilhados_AP.parquet')
dados_BA = arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/BA/dados_empilhados_BA.parquet')
dados_CE = arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/CE/dados_empilhados_CE.parquet')
dados_DF = arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/DF/dados_empilhados_DF.parquet')
dados_ES = arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/ES/dados_empilhados_ES.parquet')
dados_GO = arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/GO/dados_empilhados_GO.parquet')
dados_MA = arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/MA/dados_empilhados_MA.parquet')
dados_MG = arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/MG/dados_empilhados_MG.parquet')
dados_MS = arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/MS/dados_empilhados_MS.parquet')
dados_MT = arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/MT/dados_empilhados_MT.parquet')
dados_PA = arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/PA/dados_empilhados_PA.parquet')
dados_PB = arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/PB/dados_empilhados_PB.parquet')
dados_PE = arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/PE/dados_empilhados_PE.parquet')
dados_PI = arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/PI/dados_empilhados_PI.parquet')
dados_PR = arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/PR/dados_empilhados_PR.parquet')
dados_RJ = arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/RJ/dados_empilhados_RJ.parquet')
dados_RN = arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/RN/dados_empilhados_RN.parquet')
dados_RO = arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/RO/dados_empilhados_RO.parquet')
dados_RR = arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/RR/dados_empilhados_RR.parquet')
dados_RS = arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/RS/dados_empilhados_RS.parquet')
dados_SC = arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/SC/dados_empilhados_SC.parquet')
dados_SE = arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/SE/dados_empilhados_SE.parquet')
dados_SP1 = arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/SP/dados_empilhados_SP1.parquet')
dados_SP2 = arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/SP/dados_empilhados_SP2.parquet')
dados_SP = rbind(dados_SP1,dados_SP2)
dados_TO = arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/TO/dados_empilhados_TO.parquet')

####=====================
#### Tratamento de dados
####=====================
#ANO_CMPT: 2010 a 2024
#COD_IDADE = 1: idade em horas
#COD_IDADE = 2: idade em dias
#COD_IDADE = 3: idade em meses
#COD_IDADE = 4: idade em anos
#COD_IDADE = 5: somar 100 anos

#Categorizar a idade em: 
#0 <= idade < 15 anos
#15 <= idade < 60 anos
#60 <= idade

FiltraDadosCID = function(dados, diag_list){
  dados_agg = dados %>%  
    mutate(DIAG_ABREV = str_sub(DIAG_PRINC, 1, 3),
           IDADE_ANOS = case_when(COD_IDADE == "1" ~ IDADE / 24 / 365.25, COD_IDADE == "2" ~ IDADE / 365.25,
                                  COD_IDADE == "3" ~ IDADE / 12, COD_IDADE == "4" ~ as.numeric(IDADE),
                                  COD_IDADE == "5" ~ IDADE + 100),
           FAIXA_ETARIA = case_when(IDADE_ANOS < 15 ~ '0 <= idade < 15 anos',
                                    IDADE_ANOS >= 15 & IDADE_ANOS < 60 ~ '15 <= idade < 60 anos',
                                    IDADE_ANOS >= 60 ~ '60 <= idade')) %>% 
    filter(DIAG_ABREV %in% diag_list) %>% 
    group_by(ANO, MES, MUNIC_RES, SEXO, FAIXA_ETARIA) %>% summarise(Qtd_Internacoes = n())
  return(dados_agg)
}

####=================================
#### Anemia por deficiência de ferro
####=================================
dados_AC_Anemia = FiltraDadosCID(dados_AC, c("D50"))
dados_AL_Anemia = FiltraDadosCID(dados_AL, c("D50"))
dados_AM_Anemia = FiltraDadosCID(dados_AM, c("D50"))
dados_AP_Anemia = FiltraDadosCID(dados_AP, c("D50"))
dados_BA_Anemia = FiltraDadosCID(dados_BA, c("D50"))
dados_CE_Anemia = FiltraDadosCID(dados_CE, c("D50"))
dados_DF_Anemia = FiltraDadosCID(dados_DF, c("D50"))
dados_ES_Anemia = FiltraDadosCID(dados_ES, c("D50"))
dados_GO_Anemia = FiltraDadosCID(dados_GO, c("D50"))
dados_MA_Anemia = FiltraDadosCID(dados_MA, c("D50"))
dados_MG_Anemia = FiltraDadosCID(dados_MG, c("D50"))
dados_MS_Anemia = FiltraDadosCID(dados_MS, c("D50"))
dados_MT_Anemia = FiltraDadosCID(dados_MT, c("D50"))
dados_PA_Anemia = FiltraDadosCID(dados_PA, c("D50"))
dados_PB_Anemia = FiltraDadosCID(dados_PB, c("D50"))
dados_PE_Anemia = FiltraDadosCID(dados_PE, c("D50"))
dados_PI_Anemia = FiltraDadosCID(dados_PI, c("D50"))
dados_PR_Anemia = FiltraDadosCID(dados_PR, c("D50"))
dados_RJ_Anemia = FiltraDadosCID(dados_RJ, c("D50"))
dados_RN_Anemia = FiltraDadosCID(dados_RN, c("D50"))
dados_RO_Anemia = FiltraDadosCID(dados_RO, c("D50"))
dados_RR_Anemia = FiltraDadosCID(dados_RR, c("D50"))
dados_RS_Anemia = FiltraDadosCID(dados_RS, c("D50"))
dados_SC_Anemia = FiltraDadosCID(dados_SC, c("D50"))
dados_SE_Anemia = FiltraDadosCID(dados_SE, c("D50"))
dados_SP_Anemia = FiltraDadosCID(dados_SP, c("D50"))
dados_TO_Anemia = FiltraDadosCID(dados_TO, c("D50"))

# arrow::write_parquet(dados_AC_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_AC_Anemia.parquet')
# arrow::write_parquet(dados_AL_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_AL_Anemia.parquet')
# arrow::write_parquet(dados_AM_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_AM_Anemia.parquet')
# arrow::write_parquet(dados_AP_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_AP_Anemia.parquet')
# arrow::write_parquet(dados_BA_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_BA_Anemia.parquet')
# arrow::write_parquet(dados_CE_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_CE_Anemia.parquet')
# arrow::write_parquet(dados_DF_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_DF_Anemia.parquet')
# arrow::write_parquet(dados_ES_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_ES_Anemia.parquet')
# arrow::write_parquet(dados_GO_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_GO_Anemia.parquet')
# arrow::write_parquet(dados_MA_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_MA_Anemia.parquet')
# arrow::write_parquet(dados_MG_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_MG_Anemia.parquet')
# arrow::write_parquet(dados_MS_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_MS_Anemia.parquet')
# arrow::write_parquet(dados_MT_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_MT_Anemia.parquet')
# arrow::write_parquet(dados_PA_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_PA_Anemia.parquet')
# arrow::write_parquet(dados_PB_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_PB_Anemia.parquet')
# arrow::write_parquet(dados_PE_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_PE_Anemia.parquet')
# arrow::write_parquet(dados_PI_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_PI_Anemia.parquet')
# arrow::write_parquet(dados_PR_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_PR_Anemia.parquet')
# arrow::write_parquet(dados_RJ_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_RJ_Anemia.parquet')
# arrow::write_parquet(dados_RN_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_RN_Anemia.parquet')
# arrow::write_parquet(dados_RO_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_RO_Anemia.parquet')
# arrow::write_parquet(dados_RR_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_RR_Anemia.parquet')
# arrow::write_parquet(dados_RS_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_RS_Anemia.parquet')
# arrow::write_parquet(dados_SC_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_SC_Anemia.parquet')
# arrow::write_parquet(dados_SE_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_SE_Anemia.parquet')
# arrow::write_parquet(dados_SP_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_SP_Anemia.parquet')
# arrow::write_parquet(dados_TO_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_TO_Anemia.parquet')

dados_Anemia = rbind(dados_AC_Anemia,dados_AL_Anemia,dados_AM_Anemia,dados_AP_Anemia,
                     dados_BA_Anemia,dados_CE_Anemia,dados_DF_Anemia,dados_ES_Anemia,
                     dados_GO_Anemia,dados_MA_Anemia,dados_MG_Anemia,dados_MS_Anemia,
                     dados_MT_Anemia,dados_PA_Anemia,dados_PB_Anemia,dados_PE_Anemia,
                     dados_PI_Anemia,dados_PR_Anemia,dados_RJ_Anemia,dados_RN_Anemia,
                     dados_RO_Anemia,dados_RR_Anemia,dados_RS_Anemia,dados_SC_Anemia,
                     dados_SE_Anemia,dados_SP_Anemia,dados_TO_Anemia)
# arrow::write_parquet(dados_Anemia %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_Anemia.parquet")

####========
#### Angina
####========
dados_AC_Angina = FiltraDadosCID(dados_AC, c("I20","I24"))
dados_AL_Angina = FiltraDadosCID(dados_AL, c("I20","I24"))
dados_AM_Angina = FiltraDadosCID(dados_AM, c("I20","I24"))
dados_AP_Angina = FiltraDadosCID(dados_AP, c("I20","I24"))
dados_BA_Angina = FiltraDadosCID(dados_BA, c("I20","I24"))
dados_CE_Angina = FiltraDadosCID(dados_CE, c("I20","I24"))
dados_DF_Angina = FiltraDadosCID(dados_DF, c("I20","I24"))
dados_ES_Angina = FiltraDadosCID(dados_ES, c("I20","I24"))
dados_GO_Angina = FiltraDadosCID(dados_GO, c("I20","I24"))
dados_MA_Angina = FiltraDadosCID(dados_MA, c("I20","I24"))
dados_MG_Angina = FiltraDadosCID(dados_MG, c("I20","I24"))
dados_MS_Angina = FiltraDadosCID(dados_MS, c("I20","I24"))
dados_MT_Angina = FiltraDadosCID(dados_MT, c("I20","I24"))
dados_PA_Angina = FiltraDadosCID(dados_PA, c("I20","I24"))
dados_PB_Angina = FiltraDadosCID(dados_PB, c("I20","I24"))
dados_PE_Angina = FiltraDadosCID(dados_PE, c("I20","I24"))
dados_PI_Angina = FiltraDadosCID(dados_PI, c("I20","I24"))
dados_PR_Angina = FiltraDadosCID(dados_PR, c("I20","I24"))
dados_RJ_Angina = FiltraDadosCID(dados_RJ, c("I20","I24"))
dados_RN_Angina = FiltraDadosCID(dados_RN, c("I20","I24"))
dados_RO_Angina = FiltraDadosCID(dados_RO, c("I20","I24"))
dados_RR_Angina = FiltraDadosCID(dados_RR, c("I20","I24"))
dados_RS_Angina = FiltraDadosCID(dados_RS, c("I20","I24"))
dados_SC_Angina = FiltraDadosCID(dados_SC, c("I20","I24"))
dados_SE_Angina = FiltraDadosCID(dados_SE, c("I20","I24"))
dados_SP_Angina = FiltraDadosCID(dados_SP, c("I20","I24"))
dados_TO_Angina = FiltraDadosCID(dados_TO, c("I20","I24"))

# arrow::write_parquet(dados_AC_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_AC_Angina.parquet')
# arrow::write_parquet(dados_AL_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_AL_Angina.parquet')
# arrow::write_parquet(dados_AM_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_AM_Angina.parquet')
# arrow::write_parquet(dados_AP_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_AP_Angina.parquet')
# arrow::write_parquet(dados_BA_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_BA_Angina.parquet')
# arrow::write_parquet(dados_CE_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_CE_Angina.parquet')
# arrow::write_parquet(dados_DF_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_DF_Angina.parquet')
# arrow::write_parquet(dados_ES_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_ES_Angina.parquet')
# arrow::write_parquet(dados_GO_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_GO_Angina.parquet')
# arrow::write_parquet(dados_MA_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_MA_Angina.parquet')
# arrow::write_parquet(dados_MG_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_MG_Angina.parquet')
# arrow::write_parquet(dados_MS_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_MS_Angina.parquet')
# arrow::write_parquet(dados_MT_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_MT_Angina.parquet')
# arrow::write_parquet(dados_PA_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_PA_Angina.parquet')
# arrow::write_parquet(dados_PB_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_PB_Angina.parquet')
# arrow::write_parquet(dados_PE_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_PE_Angina.parquet')
# arrow::write_parquet(dados_PI_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_PI_Angina.parquet')
# arrow::write_parquet(dados_PR_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_PR_Angina.parquet')
# arrow::write_parquet(dados_RJ_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_RJ_Angina.parquet')
# arrow::write_parquet(dados_RN_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_RN_Angina.parquet')
# arrow::write_parquet(dados_RO_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_RO_Angina.parquet')
# arrow::write_parquet(dados_RR_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_RR_Angina.parquet')
# arrow::write_parquet(dados_RS_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_RS_Angina.parquet')
# arrow::write_parquet(dados_SC_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_SC_Angina.parquet')
# arrow::write_parquet(dados_SE_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_SE_Angina.parquet')
# arrow::write_parquet(dados_SP_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_SP_Angina.parquet')
# arrow::write_parquet(dados_TO_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_TO_Angina.parquet')

dados_Angina = rbind(dados_AC_Angina,dados_AL_Angina,dados_AM_Angina,dados_AP_Angina,
                     dados_BA_Angina,dados_CE_Angina,dados_DF_Angina,dados_ES_Angina,
                     dados_GO_Angina,dados_MA_Angina,dados_MG_Angina,dados_MS_Angina,
                     dados_MT_Angina,dados_PA_Angina,dados_PB_Angina,dados_PE_Angina,
                     dados_PI_Angina,dados_PR_Angina,dados_RJ_Angina,dados_RN_Angina,
                     dados_RO_Angina,dados_RR_Angina,dados_RS_Angina,dados_SC_Angina,
                     dados_SE_Angina,dados_SP_Angina,dados_TO_Angina)
# arrow::write_parquet(dados_Angina %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_Angina.parquet")

####======
#### Asma
####======
dados_AC_Asma = FiltraDadosCID(dados_AC, c("J45","J46"))
dados_AL_Asma = FiltraDadosCID(dados_AL, c("J45","J46"))
dados_AM_Asma = FiltraDadosCID(dados_AM, c("J45","J46"))
dados_AP_Asma = FiltraDadosCID(dados_AP, c("J45","J46"))
dados_BA_Asma = FiltraDadosCID(dados_BA, c("J45","J46"))
dados_CE_Asma = FiltraDadosCID(dados_CE, c("J45","J46"))
dados_DF_Asma = FiltraDadosCID(dados_DF, c("J45","J46"))
dados_ES_Asma = FiltraDadosCID(dados_ES, c("J45","J46"))
dados_GO_Asma = FiltraDadosCID(dados_GO, c("J45","J46"))
dados_MA_Asma = FiltraDadosCID(dados_MA, c("J45","J46"))
dados_MG_Asma = FiltraDadosCID(dados_MG, c("J45","J46"))
dados_MS_Asma = FiltraDadosCID(dados_MS, c("J45","J46"))
dados_MT_Asma = FiltraDadosCID(dados_MT, c("J45","J46"))
dados_PA_Asma = FiltraDadosCID(dados_PA, c("J45","J46"))
dados_PB_Asma = FiltraDadosCID(dados_PB, c("J45","J46"))
dados_PE_Asma = FiltraDadosCID(dados_PE, c("J45","J46"))
dados_PI_Asma = FiltraDadosCID(dados_PI, c("J45","J46"))
dados_PR_Asma = FiltraDadosCID(dados_PR, c("J45","J46"))
dados_RJ_Asma = FiltraDadosCID(dados_RJ, c("J45","J46"))
dados_RN_Asma = FiltraDadosCID(dados_RN, c("J45","J46"))
dados_RO_Asma = FiltraDadosCID(dados_RO, c("J45","J46"))
dados_RR_Asma = FiltraDadosCID(dados_RR, c("J45","J46"))
dados_RS_Asma = FiltraDadosCID(dados_RS, c("J45","J46"))
dados_SC_Asma = FiltraDadosCID(dados_SC, c("J45","J46"))
dados_SE_Asma = FiltraDadosCID(dados_SE, c("J45","J46"))
dados_SP_Asma = FiltraDadosCID(dados_SP, c("J45","J46"))
dados_TO_Asma = FiltraDadosCID(dados_TO, c("J45","J46"))

# arrow::write_parquet(dados_AC_Asma %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Asma/dados_AC_Asma.parquet')
# arrow::write_parquet(dados_AL_Asma %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Asma/dados_AL_Asma.parquet')
# arrow::write_parquet(dados_AM_Asma %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Asma/dados_AM_Asma.parquet')
# arrow::write_parquet(dados_AP_Asma %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Asma/dados_AP_Asma.parquet')
# arrow::write_parquet(dados_BA_Asma %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Asma/dados_BA_Asma.parquet')
# arrow::write_parquet(dados_CE_Asma %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Asma/dados_CE_Asma.parquet')
# arrow::write_parquet(dados_DF_Asma %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Asma/dados_DF_Asma.parquet')
# arrow::write_parquet(dados_ES_Asma %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Asma/dados_ES_Asma.parquet')
# arrow::write_parquet(dados_GO_Asma %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Asma/dados_GO_Asma.parquet')
# arrow::write_parquet(dados_MA_Asma %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Asma/dados_MA_Asma.parquet')
# arrow::write_parquet(dados_MG_Asma %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Asma/dados_MG_Asma.parquet')
# arrow::write_parquet(dados_MS_Asma %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Asma/dados_MS_Asma.parquet')
# arrow::write_parquet(dados_MT_Asma %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Asma/dados_MT_Asma.parquet')
# arrow::write_parquet(dados_PA_Asma %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Asma/dados_PA_Asma.parquet')
# arrow::write_parquet(dados_PB_Asma %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Asma/dados_PB_Asma.parquet')
# arrow::write_parquet(dados_PE_Asma %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Asma/dados_PE_Asma.parquet')
# arrow::write_parquet(dados_PI_Asma %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Asma/dados_PI_Asma.parquet')
# arrow::write_parquet(dados_PR_Asma %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Asma/dados_PR_Asma.parquet')
# arrow::write_parquet(dados_RJ_Asma %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Asma/dados_RJ_Asma.parquet')
# arrow::write_parquet(dados_RN_Asma %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Asma/dados_RN_Asma.parquet')
# arrow::write_parquet(dados_RO_Asma %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Asma/dados_RO_Asma.parquet')
# arrow::write_parquet(dados_RR_Asma %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Asma/dados_RR_Asma.parquet')
# arrow::write_parquet(dados_RS_Asma %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Asma/dados_RS_Asma.parquet')
# arrow::write_parquet(dados_SC_Asma %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Asma/dados_SC_Asma.parquet')
# arrow::write_parquet(dados_SE_Asma %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Asma/dados_SE_Asma.parquet')
# arrow::write_parquet(dados_SP_Asma %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Asma/dados_SP_Asma.parquet')
# arrow::write_parquet(dados_TO_Asma %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Asma/dados_TO_Asma.parquet')

dados_Asma = rbind(dados_AC_Asma,dados_AL_Asma,dados_AM_Asma,dados_AP_Asma,
                   dados_BA_Asma,dados_CE_Asma,dados_DF_Asma,dados_ES_Asma,
                   dados_GO_Asma,dados_MA_Asma,dados_MG_Asma,dados_MS_Asma,
                   dados_MT_Asma,dados_PA_Asma,dados_PB_Asma,dados_PE_Asma,
                   dados_PI_Asma,dados_PR_Asma,dados_RJ_Asma,dados_RN_Asma,
                   dados_RO_Asma,dados_RR_Asma,dados_RS_Asma,dados_SC_Asma,
                   dados_SE_Asma,dados_SP_Asma,dados_TO_Asma)
# arrow::write_parquet(dados_Asma %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Asma/dados_Asma.parquet")

####===========================
#### Deficiências nutricionais
####===========================
dados_AC_Def_nut = FiltraDadosCID(dados_AC, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E63","E64"))
dados_AL_Def_nut = FiltraDadosCID(dados_AL, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E63","E64"))
dados_AM_Def_nut = FiltraDadosCID(dados_AM, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E63","E64"))
dados_AP_Def_nut = FiltraDadosCID(dados_AP, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E63","E64"))
dados_BA_Def_nut = FiltraDadosCID(dados_BA, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E63","E64"))
dados_CE_Def_nut = FiltraDadosCID(dados_CE, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E63","E64"))
dados_DF_Def_nut = FiltraDadosCID(dados_DF, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E63","E64"))
dados_ES_Def_nut = FiltraDadosCID(dados_ES, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E63","E64"))
dados_GO_Def_nut = FiltraDadosCID(dados_GO, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E63","E64"))
dados_MA_Def_nut = FiltraDadosCID(dados_MA, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E63","E64"))
dados_MG_Def_nut = FiltraDadosCID(dados_MG, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E63","E64"))
dados_MS_Def_nut = FiltraDadosCID(dados_MS, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E63","E64"))
dados_MT_Def_nut = FiltraDadosCID(dados_MT, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E63","E64"))
dados_PA_Def_nut = FiltraDadosCID(dados_PA, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E63","E64"))
dados_PB_Def_nut = FiltraDadosCID(dados_PB, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E63","E64"))
dados_PE_Def_nut = FiltraDadosCID(dados_PE, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E63","E64"))
dados_PI_Def_nut = FiltraDadosCID(dados_PI, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E63","E64"))
dados_PR_Def_nut = FiltraDadosCID(dados_PR, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E63","E64"))
dados_RJ_Def_nut = FiltraDadosCID(dados_RJ, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E63","E64"))
dados_RN_Def_nut = FiltraDadosCID(dados_RN, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E63","E64"))
dados_RO_Def_nut = FiltraDadosCID(dados_RO, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E63","E64"))
dados_RR_Def_nut = FiltraDadosCID(dados_RR, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E63","E64"))
dados_RS_Def_nut = FiltraDadosCID(dados_RS, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E63","E64"))
dados_SC_Def_nut = FiltraDadosCID(dados_SC, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E63","E64"))
dados_SE_Def_nut = FiltraDadosCID(dados_SE, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E63","E64"))
dados_SP_Def_nut = FiltraDadosCID(dados_SP, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E63","E64"))
dados_TO_Def_nut = FiltraDadosCID(dados_TO, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E63","E64"))

# arrow::write_parquet(dados_AC_Def_nut %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Deficiências nutricionais/dados_AC_Def_nut.parquet')
# arrow::write_parquet(dados_AL_Def_nut %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Deficiências nutricionais/dados_AL_Def_nut.parquet')
# arrow::write_parquet(dados_AM_Def_nut %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Deficiências nutricionais/dados_AM_Def_nut.parquet')
# arrow::write_parquet(dados_AP_Def_nut %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Deficiências nutricionais/dados_AP_Def_nut.parquet')
# arrow::write_parquet(dados_BA_Def_nut %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Deficiências nutricionais/dados_BA_Def_nut.parquet')
# arrow::write_parquet(dados_CE_Def_nut %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Deficiências nutricionais/dados_CE_Def_nut.parquet')
# arrow::write_parquet(dados_DF_Def_nut %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Deficiências nutricionais/dados_DF_Def_nut.parquet')
# arrow::write_parquet(dados_ES_Def_nut %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Deficiências nutricionais/dados_ES_Def_nut.parquet')
# arrow::write_parquet(dados_GO_Def_nut %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Deficiências nutricionais/dados_GO_Def_nut.parquet')
# arrow::write_parquet(dados_MA_Def_nut %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Deficiências nutricionais/dados_MA_Def_nut.parquet')
# arrow::write_parquet(dados_MG_Def_nut %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Deficiências nutricionais/dados_MG_Def_nut.parquet')
# arrow::write_parquet(dados_MS_Def_nut %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Deficiências nutricionais/dados_MS_Def_nut.parquet')
# arrow::write_parquet(dados_MT_Def_nut %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Deficiências nutricionais/dados_MT_Def_nut.parquet')
# arrow::write_parquet(dados_PA_Def_nut %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Deficiências nutricionais/dados_PA_Def_nut.parquet')
# arrow::write_parquet(dados_PB_Def_nut %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Deficiências nutricionais/dados_PB_Def_nut.parquet')
# arrow::write_parquet(dados_PE_Def_nut %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Deficiências nutricionais/dados_PE_Def_nut.parquet')
# arrow::write_parquet(dados_PI_Def_nut %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Deficiências nutricionais/dados_PI_Def_nut.parquet')
# arrow::write_parquet(dados_PR_Def_nut %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Deficiências nutricionais/dados_PR_Def_nut.parquet')
# arrow::write_parquet(dados_RJ_Def_nut %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Deficiências nutricionais/dados_RJ_Def_nut.parquet')
# arrow::write_parquet(dados_RN_Def_nut %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Deficiências nutricionais/dados_RN_Def_nut.parquet')
# arrow::write_parquet(dados_RO_Def_nut %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Deficiências nutricionais/dados_RO_Def_nut.parquet')
# arrow::write_parquet(dados_RR_Def_nut %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Deficiências nutricionais/dados_RR_Def_nut.parquet')
# arrow::write_parquet(dados_RS_Def_nut %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Deficiências nutricionais/dados_RS_Def_nut.parquet')
# arrow::write_parquet(dados_SC_Def_nut %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Deficiências nutricionais/dados_SC_Def_nut.parquet')
# arrow::write_parquet(dados_SE_Def_nut %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Deficiências nutricionais/dados_SE_Def_nut.parquet')
# arrow::write_parquet(dados_SP_Def_nut %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Deficiências nutricionais/dados_SP_Def_nut.parquet')
# arrow::write_parquet(dados_TO_Def_nut %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Deficiências nutricionais/dados_TO_Def_nut.parquet')

dados_Def_nut = rbind(dados_AC_Def_nut,dados_AL_Def_nut,dados_AM_Def_nut,dados_AP_Def_nut,
                      dados_BA_Def_nut,dados_CE_Def_nut,dados_DF_Def_nut,dados_ES_Def_nut,
                      dados_GO_Def_nut,dados_MA_Def_nut,dados_MG_Def_nut,dados_MS_Def_nut,
                      dados_MT_Def_nut,dados_PA_Def_nut,dados_PB_Def_nut,dados_PE_Def_nut,
                      dados_PI_Def_nut,dados_PR_Def_nut,dados_RJ_Def_nut,dados_RN_Def_nut,
                      dados_RO_Def_nut,dados_RR_Def_nut,dados_RS_Def_nut,dados_SC_Def_nut,
                      dados_SE_Def_nut,dados_SP_Def_nut,dados_TO_Def_nut)
# arrow::write_parquet(dados_Def_nut %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Deficiências nutricionais/dados_Deficiências_nutricionais.parquet")

####==========
#### Diabetes
####==========
dados_AC_Diabetes = FiltraDadosCID(dados_AC, c("E10","E11","E12","E13","E14"))
dados_AL_Diabetes = FiltraDadosCID(dados_AL, c("E10","E11","E12","E13","E14"))
dados_AM_Diabetes = FiltraDadosCID(dados_AM, c("E10","E11","E12","E13","E14"))
dados_AP_Diabetes = FiltraDadosCID(dados_AP, c("E10","E11","E12","E13","E14"))
dados_BA_Diabetes = FiltraDadosCID(dados_BA, c("E10","E11","E12","E13","E14"))
dados_CE_Diabetes = FiltraDadosCID(dados_CE, c("E10","E11","E12","E13","E14"))
dados_DF_Diabetes = FiltraDadosCID(dados_DF, c("E10","E11","E12","E13","E14"))
dados_ES_Diabetes = FiltraDadosCID(dados_ES, c("E10","E11","E12","E13","E14"))
dados_GO_Diabetes = FiltraDadosCID(dados_GO, c("E10","E11","E12","E13","E14"))
dados_MA_Diabetes = FiltraDadosCID(dados_MA, c("E10","E11","E12","E13","E14"))
dados_MG_Diabetes = FiltraDadosCID(dados_MG, c("E10","E11","E12","E13","E14"))
dados_MS_Diabetes = FiltraDadosCID(dados_MS, c("E10","E11","E12","E13","E14"))
dados_MT_Diabetes = FiltraDadosCID(dados_MT, c("E10","E11","E12","E13","E14"))
dados_PA_Diabetes = FiltraDadosCID(dados_PA, c("E10","E11","E12","E13","E14"))
dados_PB_Diabetes = FiltraDadosCID(dados_PB, c("E10","E11","E12","E13","E14"))
dados_PE_Diabetes = FiltraDadosCID(dados_PE, c("E10","E11","E12","E13","E14"))
dados_PI_Diabetes = FiltraDadosCID(dados_PI, c("E10","E11","E12","E13","E14"))
dados_PR_Diabetes = FiltraDadosCID(dados_PR, c("E10","E11","E12","E13","E14"))
dados_RJ_Diabetes = FiltraDadosCID(dados_RJ, c("E10","E11","E12","E13","E14"))
dados_RN_Diabetes = FiltraDadosCID(dados_RN, c("E10","E11","E12","E13","E14"))
dados_RO_Diabetes = FiltraDadosCID(dados_RO, c("E10","E11","E12","E13","E14"))
dados_RR_Diabetes = FiltraDadosCID(dados_RR, c("E10","E11","E12","E13","E14"))
dados_RS_Diabetes = FiltraDadosCID(dados_RS, c("E10","E11","E12","E13","E14"))
dados_SC_Diabetes = FiltraDadosCID(dados_SC, c("E10","E11","E12","E13","E14"))
dados_SE_Diabetes = FiltraDadosCID(dados_SE, c("E10","E11","E12","E13","E14"))
dados_SP_Diabetes = FiltraDadosCID(dados_SP, c("E10","E11","E12","E13","E14"))
dados_TO_Diabetes = FiltraDadosCID(dados_TO, c("E10","E11","E12","E13","E14"))

# arrow::write_parquet(dados_AC_Diabetes %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Diabetes/dados_AC_Diabetes.parquet')
# arrow::write_parquet(dados_AL_Diabetes %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Diabetes/dados_AL_Diabetes.parquet')
# arrow::write_parquet(dados_AM_Diabetes %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Diabetes/dados_AM_Diabetes.parquet')
# arrow::write_parquet(dados_AP_Diabetes %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Diabetes/dados_AP_Diabetes.parquet')
# arrow::write_parquet(dados_BA_Diabetes %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Diabetes/dados_BA_Diabetes.parquet')
# arrow::write_parquet(dados_CE_Diabetes %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Diabetes/dados_CE_Diabetes.parquet')
# arrow::write_parquet(dados_DF_Diabetes %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Diabetes/dados_DF_Diabetes.parquet')
# arrow::write_parquet(dados_ES_Diabetes %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Diabetes/dados_ES_Diabetes.parquet')
# arrow::write_parquet(dados_GO_Diabetes %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Diabetes/dados_GO_Diabetes.parquet')
# arrow::write_parquet(dados_MA_Diabetes %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Diabetes/dados_MA_Diabetes.parquet')
# arrow::write_parquet(dados_MG_Diabetes %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Diabetes/dados_MG_Diabetes.parquet')
# arrow::write_parquet(dados_MS_Diabetes %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Diabetes/dados_MS_Diabetes.parquet')
# arrow::write_parquet(dados_MT_Diabetes %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Diabetes/dados_MT_Diabetes.parquet')
# arrow::write_parquet(dados_PA_Diabetes %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Diabetes/dados_PA_Diabetes.parquet')
# arrow::write_parquet(dados_PB_Diabetes %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Diabetes/dados_PB_Diabetes.parquet')
# arrow::write_parquet(dados_PE_Diabetes %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Diabetes/dados_PE_Diabetes.parquet')
# arrow::write_parquet(dados_PI_Diabetes %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Diabetes/dados_PI_Diabetes.parquet')
# arrow::write_parquet(dados_PR_Diabetes %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Diabetes/dados_PR_Diabetes.parquet')
# arrow::write_parquet(dados_RJ_Diabetes %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Diabetes/dados_RJ_Diabetes.parquet')
# arrow::write_parquet(dados_RN_Diabetes %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Diabetes/dados_RN_Diabetes.parquet')
# arrow::write_parquet(dados_RO_Diabetes %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Diabetes/dados_RO_Diabetes.parquet')
# arrow::write_parquet(dados_RR_Diabetes %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Diabetes/dados_RR_Diabetes.parquet')
# arrow::write_parquet(dados_RS_Diabetes %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Diabetes/dados_RS_Diabetes.parquet')
# arrow::write_parquet(dados_SC_Diabetes %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Diabetes/dados_SC_Diabetes.parquet')
# arrow::write_parquet(dados_SE_Diabetes %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Diabetes/dados_SE_Diabetes.parquet')
# arrow::write_parquet(dados_SP_Diabetes %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Diabetes/dados_SP_Diabetes.parquet')
# arrow::write_parquet(dados_TO_Diabetes %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Diabetes/dados_TO_Diabetes.parquet')

dados_Diabetes = rbind(dados_AC_Diabetes,dados_AL_Diabetes,dados_AM_Diabetes,dados_AP_Diabetes,
                       dados_BA_Diabetes,dados_CE_Diabetes,dados_DF_Diabetes,dados_ES_Diabetes,
                       dados_GO_Diabetes,dados_MA_Diabetes,dados_MG_Diabetes,dados_MS_Diabetes,
                       dados_MT_Diabetes,dados_PA_Diabetes,dados_PB_Diabetes,dados_PE_Diabetes,
                       dados_PI_Diabetes,dados_PR_Diabetes,dados_RJ_Diabetes,dados_RN_Diabetes,
                       dados_RO_Diabetes,dados_RR_Diabetes,dados_RS_Diabetes,dados_SC_Diabetes,
                       dados_SE_Diabetes,dados_SP_Diabetes,dados_TO_Diabetes)
# arrow::write_parquet(dados_Diabetes %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Diabetes/dados_Diabetes.parquet")

####===============================================
#### Doença Inflamatória órgãos pélvicos femininos
####===============================================
dados_AC_D_Inf_Org_Pelv_Fem = FiltraDadosCID(dados_AC, c("N70","N71","N72","N73","N75","N76"))
dados_AL_D_Inf_Org_Pelv_Fem = FiltraDadosCID(dados_AL, c("N70","N71","N72","N73","N75","N76"))
dados_AM_D_Inf_Org_Pelv_Fem = FiltraDadosCID(dados_AM, c("N70","N71","N72","N73","N75","N76"))
dados_AP_D_Inf_Org_Pelv_Fem = FiltraDadosCID(dados_AP, c("N70","N71","N72","N73","N75","N76"))
dados_BA_D_Inf_Org_Pelv_Fem = FiltraDadosCID(dados_BA, c("N70","N71","N72","N73","N75","N76"))
dados_CE_D_Inf_Org_Pelv_Fem = FiltraDadosCID(dados_CE, c("N70","N71","N72","N73","N75","N76"))
dados_DF_D_Inf_Org_Pelv_Fem = FiltraDadosCID(dados_DF, c("N70","N71","N72","N73","N75","N76"))
dados_ES_D_Inf_Org_Pelv_Fem = FiltraDadosCID(dados_ES, c("N70","N71","N72","N73","N75","N76"))
dados_GO_D_Inf_Org_Pelv_Fem = FiltraDadosCID(dados_GO, c("N70","N71","N72","N73","N75","N76"))
dados_MA_D_Inf_Org_Pelv_Fem = FiltraDadosCID(dados_MA, c("N70","N71","N72","N73","N75","N76"))
dados_MG_D_Inf_Org_Pelv_Fem = FiltraDadosCID(dados_MG, c("N70","N71","N72","N73","N75","N76"))
dados_MS_D_Inf_Org_Pelv_Fem = FiltraDadosCID(dados_MS, c("N70","N71","N72","N73","N75","N76"))
dados_MT_D_Inf_Org_Pelv_Fem = FiltraDadosCID(dados_MT, c("N70","N71","N72","N73","N75","N76"))
dados_PA_D_Inf_Org_Pelv_Fem = FiltraDadosCID(dados_PA, c("N70","N71","N72","N73","N75","N76"))
dados_PB_D_Inf_Org_Pelv_Fem = FiltraDadosCID(dados_PB, c("N70","N71","N72","N73","N75","N76"))
dados_PE_D_Inf_Org_Pelv_Fem = FiltraDadosCID(dados_PE, c("N70","N71","N72","N73","N75","N76"))
dados_PI_D_Inf_Org_Pelv_Fem = FiltraDadosCID(dados_PI, c("N70","N71","N72","N73","N75","N76"))
dados_PR_D_Inf_Org_Pelv_Fem = FiltraDadosCID(dados_PR, c("N70","N71","N72","N73","N75","N76"))
dados_RJ_D_Inf_Org_Pelv_Fem = FiltraDadosCID(dados_RJ, c("N70","N71","N72","N73","N75","N76"))
dados_RN_D_Inf_Org_Pelv_Fem = FiltraDadosCID(dados_RN, c("N70","N71","N72","N73","N75","N76"))
dados_RO_D_Inf_Org_Pelv_Fem = FiltraDadosCID(dados_RO, c("N70","N71","N72","N73","N75","N76"))
dados_RR_D_Inf_Org_Pelv_Fem = FiltraDadosCID(dados_RR, c("N70","N71","N72","N73","N75","N76"))
dados_RS_D_Inf_Org_Pelv_Fem = FiltraDadosCID(dados_RS, c("N70","N71","N72","N73","N75","N76"))
dados_SC_D_Inf_Org_Pelv_Fem = FiltraDadosCID(dados_SC, c("N70","N71","N72","N73","N75","N76"))
dados_SE_D_Inf_Org_Pelv_Fem = FiltraDadosCID(dados_SE, c("N70","N71","N72","N73","N75","N76"))
dados_SP_D_Inf_Org_Pelv_Fem = FiltraDadosCID(dados_SP, c("N70","N71","N72","N73","N75","N76"))
dados_TO_D_Inf_Org_Pelv_Fem = FiltraDadosCID(dados_TO, c("N70","N71","N72","N73","N75","N76"))

# arrow::write_parquet(dados_AC_D_Inf_Org_Pelv_Fem %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Inflamatória órgãos pélvicos femininos/dados_AC_D_Inf_Org_Pelv_Fem.parquet')
# arrow::write_parquet(dados_AL_D_Inf_Org_Pelv_Fem %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Inflamatória órgãos pélvicos femininos/dados_AL_D_Inf_Org_Pelv_Fem.parquet')
# arrow::write_parquet(dados_AM_D_Inf_Org_Pelv_Fem %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Inflamatória órgãos pélvicos femininos/dados_AM_D_Inf_Org_Pelv_Fem.parquet')
# arrow::write_parquet(dados_AP_D_Inf_Org_Pelv_Fem %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Inflamatória órgãos pélvicos femininos/dados_AP_D_Inf_Org_Pelv_Fem.parquet')
# arrow::write_parquet(dados_BA_D_Inf_Org_Pelv_Fem %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Inflamatória órgãos pélvicos femininos/dados_BA_D_Inf_Org_Pelv_Fem.parquet')
# arrow::write_parquet(dados_CE_D_Inf_Org_Pelv_Fem %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Inflamatória órgãos pélvicos femininos/dados_CE_D_Inf_Org_Pelv_Fem.parquet')
# arrow::write_parquet(dados_DF_D_Inf_Org_Pelv_Fem %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Inflamatória órgãos pélvicos femininos/dados_DF_D_Inf_Org_Pelv_Fem.parquet')
# arrow::write_parquet(dados_ES_D_Inf_Org_Pelv_Fem %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Inflamatória órgãos pélvicos femininos/dados_ES_D_Inf_Org_Pelv_Fem.parquet')
# arrow::write_parquet(dados_GO_D_Inf_Org_Pelv_Fem %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Inflamatória órgãos pélvicos femininos/dados_GO_D_Inf_Org_Pelv_Fem.parquet')
# arrow::write_parquet(dados_MA_D_Inf_Org_Pelv_Fem %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Inflamatória órgãos pélvicos femininos/dados_MA_D_Inf_Org_Pelv_Fem.parquet')
# arrow::write_parquet(dados_MG_D_Inf_Org_Pelv_Fem %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Inflamatória órgãos pélvicos femininos/dados_MG_D_Inf_Org_Pelv_Fem.parquet')
# arrow::write_parquet(dados_MS_D_Inf_Org_Pelv_Fem %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Inflamatória órgãos pélvicos femininos/dados_MS_D_Inf_Org_Pelv_Fem.parquet')
# arrow::write_parquet(dados_MT_D_Inf_Org_Pelv_Fem %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Inflamatória órgãos pélvicos femininos/dados_MT_D_Inf_Org_Pelv_Fem.parquet')
# arrow::write_parquet(dados_PA_D_Inf_Org_Pelv_Fem %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Inflamatória órgãos pélvicos femininos/dados_PA_D_Inf_Org_Pelv_Fem.parquet')
# arrow::write_parquet(dados_PB_D_Inf_Org_Pelv_Fem %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Inflamatória órgãos pélvicos femininos/dados_PB_D_Inf_Org_Pelv_Fem.parquet')
# arrow::write_parquet(dados_PE_D_Inf_Org_Pelv_Fem %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Inflamatória órgãos pélvicos femininos/dados_PE_D_Inf_Org_Pelv_Fem.parquet')
# arrow::write_parquet(dados_PI_D_Inf_Org_Pelv_Fem %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Inflamatória órgãos pélvicos femininos/dados_PI_D_Inf_Org_Pelv_Fem.parquet')
# arrow::write_parquet(dados_PR_D_Inf_Org_Pelv_Fem %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Inflamatória órgãos pélvicos femininos/dados_PR_D_Inf_Org_Pelv_Fem.parquet')
# arrow::write_parquet(dados_RJ_D_Inf_Org_Pelv_Fem %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Inflamatória órgãos pélvicos femininos/dados_RJ_D_Inf_Org_Pelv_Fem.parquet')
# arrow::write_parquet(dados_RN_D_Inf_Org_Pelv_Fem %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Inflamatória órgãos pélvicos femininos/dados_RN_D_Inf_Org_Pelv_Fem.parquet')
# arrow::write_parquet(dados_RO_D_Inf_Org_Pelv_Fem %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Inflamatória órgãos pélvicos femininos/dados_RO_D_Inf_Org_Pelv_Fem.parquet')
# arrow::write_parquet(dados_RR_D_Inf_Org_Pelv_Fem %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Inflamatória órgãos pélvicos femininos/dados_RR_D_Inf_Org_Pelv_Fem.parquet')
# arrow::write_parquet(dados_RS_D_Inf_Org_Pelv_Fem %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Inflamatória órgãos pélvicos femininos/dados_RS_D_Inf_Org_Pelv_Fem.parquet')
# arrow::write_parquet(dados_SC_D_Inf_Org_Pelv_Fem %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Inflamatória órgãos pélvicos femininos/dados_SC_D_Inf_Org_Pelv_Fem.parquet')
# arrow::write_parquet(dados_SE_D_Inf_Org_Pelv_Fem %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Inflamatória órgãos pélvicos femininos/dados_SE_D_Inf_Org_Pelv_Fem.parquet')
# arrow::write_parquet(dados_SP_D_Inf_Org_Pelv_Fem %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Inflamatória órgãos pélvicos femininos/dados_SP_D_Inf_Org_Pelv_Fem.parquet')
# arrow::write_parquet(dados_TO_D_Inf_Org_Pelv_Fem %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Inflamatória órgãos pélvicos femininos/dados_TO_D_Inf_Org_Pelv_Fem.parquet')

dados_D_Inf_Org_Pelv_Fem = 
  rbind(dados_AC_D_Inf_Org_Pelv_Fem,dados_AL_D_Inf_Org_Pelv_Fem,dados_AM_D_Inf_Org_Pelv_Fem,dados_AP_D_Inf_Org_Pelv_Fem,
        dados_BA_D_Inf_Org_Pelv_Fem,dados_CE_D_Inf_Org_Pelv_Fem,dados_DF_D_Inf_Org_Pelv_Fem,dados_ES_D_Inf_Org_Pelv_Fem,
        dados_GO_D_Inf_Org_Pelv_Fem,dados_MA_D_Inf_Org_Pelv_Fem,dados_MG_D_Inf_Org_Pelv_Fem,dados_MS_D_Inf_Org_Pelv_Fem,
        dados_MT_D_Inf_Org_Pelv_Fem,dados_PA_D_Inf_Org_Pelv_Fem,dados_PB_D_Inf_Org_Pelv_Fem,dados_PE_D_Inf_Org_Pelv_Fem,
        dados_PI_D_Inf_Org_Pelv_Fem,dados_PR_D_Inf_Org_Pelv_Fem,dados_RJ_D_Inf_Org_Pelv_Fem,dados_RN_D_Inf_Org_Pelv_Fem,
        dados_RO_D_Inf_Org_Pelv_Fem,dados_RR_D_Inf_Org_Pelv_Fem,dados_RS_D_Inf_Org_Pelv_Fem,dados_SC_D_Inf_Org_Pelv_Fem,
        dados_SE_D_Inf_Org_Pelv_Fem,dados_SP_D_Inf_Org_Pelv_Fem,dados_TO_D_Inf_Org_Pelv_Fem)
# arrow::write_parquet(dados_D_Inf_Org_Pelv_Fem %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Inflamatória órgãos pélvicos femininos/dados_Doença_Inflamatória_órgãos_pélvicos_femininos.parquet")

####====================================
#### Doença Pulmonar obstrutiva crônica
####====================================
dados_AC_D_Pulm_Obs_Cron = FiltraDadosCID(dados_AC, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_AL_D_Pulm_Obs_Cron = FiltraDadosCID(dados_AL, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_AM_D_Pulm_Obs_Cron = FiltraDadosCID(dados_AM, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_AP_D_Pulm_Obs_Cron = FiltraDadosCID(dados_AP, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_BA_D_Pulm_Obs_Cron = FiltraDadosCID(dados_BA, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_CE_D_Pulm_Obs_Cron = FiltraDadosCID(dados_CE, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_DF_D_Pulm_Obs_Cron = FiltraDadosCID(dados_DF, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_ES_D_Pulm_Obs_Cron = FiltraDadosCID(dados_ES, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_GO_D_Pulm_Obs_Cron = FiltraDadosCID(dados_GO, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_MA_D_Pulm_Obs_Cron = FiltraDadosCID(dados_MA, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_MG_D_Pulm_Obs_Cron = FiltraDadosCID(dados_MG, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_MS_D_Pulm_Obs_Cron = FiltraDadosCID(dados_MS, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_MT_D_Pulm_Obs_Cron = FiltraDadosCID(dados_MT, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_PA_D_Pulm_Obs_Cron = FiltraDadosCID(dados_PA, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_PB_D_Pulm_Obs_Cron = FiltraDadosCID(dados_PB, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_PE_D_Pulm_Obs_Cron = FiltraDadosCID(dados_PE, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_PI_D_Pulm_Obs_Cron = FiltraDadosCID(dados_PI, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_PR_D_Pulm_Obs_Cron = FiltraDadosCID(dados_PR, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_RJ_D_Pulm_Obs_Cron = FiltraDadosCID(dados_RJ, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_RN_D_Pulm_Obs_Cron = FiltraDadosCID(dados_RN, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_RO_D_Pulm_Obs_Cron = FiltraDadosCID(dados_RO, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_RR_D_Pulm_Obs_Cron = FiltraDadosCID(dados_RR, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_RS_D_Pulm_Obs_Cron = FiltraDadosCID(dados_RS, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_SC_D_Pulm_Obs_Cron = FiltraDadosCID(dados_SC, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_SE_D_Pulm_Obs_Cron = FiltraDadosCID(dados_SE, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_SP_D_Pulm_Obs_Cron = FiltraDadosCID(dados_SP, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_TO_D_Pulm_Obs_Cron = FiltraDadosCID(dados_TO, c("J20","J21","J40","J41","J42","J43","J44","J47"))

# arrow::write_parquet(dados_AC_D_Pulm_Obs_Cron %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Pulmonar obstrutiva crônica/dados_AC_D_Pulm_Obs_Cron.parquet')
# arrow::write_parquet(dados_AL_D_Pulm_Obs_Cron %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Pulmonar obstrutiva crônica/dados_AL_D_Pulm_Obs_Cron.parquet')
# arrow::write_parquet(dados_AM_D_Pulm_Obs_Cron %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Pulmonar obstrutiva crônica/dados_AM_D_Pulm_Obs_Cron.parquet')
# arrow::write_parquet(dados_AP_D_Pulm_Obs_Cron %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Pulmonar obstrutiva crônica/dados_AP_D_Pulm_Obs_Cron.parquet')
# arrow::write_parquet(dados_BA_D_Pulm_Obs_Cron %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Pulmonar obstrutiva crônica/dados_BA_D_Pulm_Obs_Cron.parquet')
# arrow::write_parquet(dados_CE_D_Pulm_Obs_Cron %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Pulmonar obstrutiva crônica/dados_CE_D_Pulm_Obs_Cron.parquet')
# arrow::write_parquet(dados_DF_D_Pulm_Obs_Cron %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Pulmonar obstrutiva crônica/dados_DF_D_Pulm_Obs_Cron.parquet')
# arrow::write_parquet(dados_ES_D_Pulm_Obs_Cron %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Pulmonar obstrutiva crônica/dados_ES_D_Pulm_Obs_Cron.parquet')
# arrow::write_parquet(dados_GO_D_Pulm_Obs_Cron %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Pulmonar obstrutiva crônica/dados_GO_D_Pulm_Obs_Cron.parquet')
# arrow::write_parquet(dados_MA_D_Pulm_Obs_Cron %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Pulmonar obstrutiva crônica/dados_MA_D_Pulm_Obs_Cron.parquet')
# arrow::write_parquet(dados_MG_D_Pulm_Obs_Cron %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Pulmonar obstrutiva crônica/dados_MG_D_Pulm_Obs_Cron.parquet')
# arrow::write_parquet(dados_MS_D_Pulm_Obs_Cron %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Pulmonar obstrutiva crônica/dados_MS_D_Pulm_Obs_Cron.parquet')
# arrow::write_parquet(dados_MT_D_Pulm_Obs_Cron %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Pulmonar obstrutiva crônica/dados_MT_D_Pulm_Obs_Cron.parquet')
# arrow::write_parquet(dados_PA_D_Pulm_Obs_Cron %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Pulmonar obstrutiva crônica/dados_PA_D_Pulm_Obs_Cron.parquet')
# arrow::write_parquet(dados_PB_D_Pulm_Obs_Cron %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Pulmonar obstrutiva crônica/dados_PB_D_Pulm_Obs_Cron.parquet')
# arrow::write_parquet(dados_PE_D_Pulm_Obs_Cron %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Pulmonar obstrutiva crônica/dados_PE_D_Pulm_Obs_Cron.parquet')
# arrow::write_parquet(dados_PI_D_Pulm_Obs_Cron %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Pulmonar obstrutiva crônica/dados_PI_D_Pulm_Obs_Cron.parquet')
# arrow::write_parquet(dados_PR_D_Pulm_Obs_Cron %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Pulmonar obstrutiva crônica/dados_PR_D_Pulm_Obs_Cron.parquet')
# arrow::write_parquet(dados_RJ_D_Pulm_Obs_Cron %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Pulmonar obstrutiva crônica/dados_RJ_D_Pulm_Obs_Cron.parquet')
# arrow::write_parquet(dados_RN_D_Pulm_Obs_Cron %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Pulmonar obstrutiva crônica/dados_RN_D_Pulm_Obs_Cron.parquet')
# arrow::write_parquet(dados_RO_D_Pulm_Obs_Cron %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Pulmonar obstrutiva crônica/dados_RO_D_Pulm_Obs_Cron.parquet')
# arrow::write_parquet(dados_RR_D_Pulm_Obs_Cron %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Pulmonar obstrutiva crônica/dados_RR_D_Pulm_Obs_Cron.parquet')
# arrow::write_parquet(dados_RS_D_Pulm_Obs_Cron %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Pulmonar obstrutiva crônica/dados_RS_D_Pulm_Obs_Cron.parquet')
# arrow::write_parquet(dados_SC_D_Pulm_Obs_Cron %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Pulmonar obstrutiva crônica/dados_SC_D_Pulm_Obs_Cron.parquet')
# arrow::write_parquet(dados_SE_D_Pulm_Obs_Cron %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Pulmonar obstrutiva crônica/dados_SE_D_Pulm_Obs_Cron.parquet')
# arrow::write_parquet(dados_SP_D_Pulm_Obs_Cron %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Pulmonar obstrutiva crônica/dados_SP_D_Pulm_Obs_Cron.parquet')
# arrow::write_parquet(dados_TO_D_Pulm_Obs_Cron %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Pulmonar obstrutiva crônica/dados_TO_D_Pulm_Obs_Cron.parquet')

dados_D_Pulm_Obs_Cron = 
  rbind(dados_AC_D_Pulm_Obs_Cron,dados_AL_D_Pulm_Obs_Cron,dados_AM_D_Pulm_Obs_Cron,dados_AP_D_Pulm_Obs_Cron,
        dados_BA_D_Pulm_Obs_Cron,dados_CE_D_Pulm_Obs_Cron,dados_DF_D_Pulm_Obs_Cron,dados_ES_D_Pulm_Obs_Cron,
        dados_GO_D_Pulm_Obs_Cron,dados_MA_D_Pulm_Obs_Cron,dados_MG_D_Pulm_Obs_Cron,dados_MS_D_Pulm_Obs_Cron,
        dados_MT_D_Pulm_Obs_Cron,dados_PA_D_Pulm_Obs_Cron,dados_PB_D_Pulm_Obs_Cron,dados_PE_D_Pulm_Obs_Cron,
        dados_PI_D_Pulm_Obs_Cron,dados_PR_D_Pulm_Obs_Cron,dados_RJ_D_Pulm_Obs_Cron,dados_RN_D_Pulm_Obs_Cron,
        dados_RO_D_Pulm_Obs_Cron,dados_RR_D_Pulm_Obs_Cron,dados_RS_D_Pulm_Obs_Cron,dados_SC_D_Pulm_Obs_Cron,
        dados_SE_D_Pulm_Obs_Cron,dados_SP_D_Pulm_Obs_Cron,dados_TO_D_Pulm_Obs_Cron)
# arrow::write_parquet(dados_D_Pulm_Obs_Cron %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Pulmonar obstrutiva crônica/dados_Doença_Pulmonar_obstrutiva_crônica.parquet")

####============================
#### Doenças Cerebro-vasculares
####============================
dados_AC_D_Cerebrovasc = FiltraDadosCID(dados_AC, c("I60","I61","I62","I63","I64","I65","I66","I67","I68","I69"))
dados_AL_D_Cerebrovasc = FiltraDadosCID(dados_AL, c("I60","I61","I62","I63","I64","I65","I66","I67","I68","I69"))
dados_AM_D_Cerebrovasc = FiltraDadosCID(dados_AM, c("I60","I61","I62","I63","I64","I65","I66","I67","I68","I69"))
dados_AP_D_Cerebrovasc = FiltraDadosCID(dados_AP, c("I60","I61","I62","I63","I64","I65","I66","I67","I68","I69"))
dados_BA_D_Cerebrovasc = FiltraDadosCID(dados_BA, c("I60","I61","I62","I63","I64","I65","I66","I67","I68","I69"))
dados_CE_D_Cerebrovasc = FiltraDadosCID(dados_CE, c("I60","I61","I62","I63","I64","I65","I66","I67","I68","I69"))
dados_DF_D_Cerebrovasc = FiltraDadosCID(dados_DF, c("I60","I61","I62","I63","I64","I65","I66","I67","I68","I69"))
dados_ES_D_Cerebrovasc = FiltraDadosCID(dados_ES, c("I60","I61","I62","I63","I64","I65","I66","I67","I68","I69"))
dados_GO_D_Cerebrovasc = FiltraDadosCID(dados_GO, c("I60","I61","I62","I63","I64","I65","I66","I67","I68","I69"))
dados_MA_D_Cerebrovasc = FiltraDadosCID(dados_MA, c("I60","I61","I62","I63","I64","I65","I66","I67","I68","I69"))
dados_MG_D_Cerebrovasc = FiltraDadosCID(dados_MG, c("I60","I61","I62","I63","I64","I65","I66","I67","I68","I69"))
dados_MS_D_Cerebrovasc = FiltraDadosCID(dados_MS, c("I60","I61","I62","I63","I64","I65","I66","I67","I68","I69"))
dados_MT_D_Cerebrovasc = FiltraDadosCID(dados_MT, c("I60","I61","I62","I63","I64","I65","I66","I67","I68","I69"))
dados_PA_D_Cerebrovasc = FiltraDadosCID(dados_PA, c("I60","I61","I62","I63","I64","I65","I66","I67","I68","I69"))
dados_PB_D_Cerebrovasc = FiltraDadosCID(dados_PB, c("I60","I61","I62","I63","I64","I65","I66","I67","I68","I69"))
dados_PE_D_Cerebrovasc = FiltraDadosCID(dados_PE, c("I60","I61","I62","I63","I64","I65","I66","I67","I68","I69"))
dados_PI_D_Cerebrovasc = FiltraDadosCID(dados_PI, c("I60","I61","I62","I63","I64","I65","I66","I67","I68","I69"))
dados_PR_D_Cerebrovasc = FiltraDadosCID(dados_PR, c("I60","I61","I62","I63","I64","I65","I66","I67","I68","I69"))
dados_RJ_D_Cerebrovasc = FiltraDadosCID(dados_RJ, c("I60","I61","I62","I63","I64","I65","I66","I67","I68","I69"))
dados_RN_D_Cerebrovasc = FiltraDadosCID(dados_RN, c("I60","I61","I62","I63","I64","I65","I66","I67","I68","I69"))
dados_RO_D_Cerebrovasc = FiltraDadosCID(dados_RO, c("I60","I61","I62","I63","I64","I65","I66","I67","I68","I69"))
dados_RR_D_Cerebrovasc = FiltraDadosCID(dados_RR, c("I60","I61","I62","I63","I64","I65","I66","I67","I68","I69"))
dados_RS_D_Cerebrovasc = FiltraDadosCID(dados_RS, c("I60","I61","I62","I63","I64","I65","I66","I67","I68","I69"))
dados_SC_D_Cerebrovasc = FiltraDadosCID(dados_SC, c("I60","I61","I62","I63","I64","I65","I66","I67","I68","I69"))
dados_SE_D_Cerebrovasc = FiltraDadosCID(dados_SE, c("I60","I61","I62","I63","I64","I65","I66","I67","I68","I69"))
dados_SP_D_Cerebrovasc = FiltraDadosCID(dados_SP, c("I60","I61","I62","I63","I64","I65","I66","I67","I68","I69"))
dados_TO_D_Cerebrovasc = FiltraDadosCID(dados_TO, c("I60","I61","I62","I63","I64","I65","I66","I67","I68","I69"))

# arrow::write_parquet(dados_AC_D_Cerebrovasc %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças Cerebro-vasculares/dados_AC_D_Cerebrovasc.parquet')
# arrow::write_parquet(dados_AL_D_Cerebrovasc %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças Cerebro-vasculares/dados_AL_D_Cerebrovasc.parquet')
# arrow::write_parquet(dados_AM_D_Cerebrovasc %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças Cerebro-vasculares/dados_AM_D_Cerebrovasc.parquet')
# arrow::write_parquet(dados_AP_D_Cerebrovasc %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças Cerebro-vasculares/dados_AP_D_Cerebrovasc.parquet')
# arrow::write_parquet(dados_BA_D_Cerebrovasc %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças Cerebro-vasculares/dados_BA_D_Cerebrovasc.parquet')
# arrow::write_parquet(dados_CE_D_Cerebrovasc %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças Cerebro-vasculares/dados_CE_D_Cerebrovasc.parquet')
# arrow::write_parquet(dados_DF_D_Cerebrovasc %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças Cerebro-vasculares/dados_DF_D_Cerebrovasc.parquet')
# arrow::write_parquet(dados_ES_D_Cerebrovasc %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças Cerebro-vasculares/dados_ES_D_Cerebrovasc.parquet')
# arrow::write_parquet(dados_GO_D_Cerebrovasc %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças Cerebro-vasculares/dados_GO_D_Cerebrovasc.parquet')
# arrow::write_parquet(dados_MA_D_Cerebrovasc %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças Cerebro-vasculares/dados_MA_D_Cerebrovasc.parquet')
# arrow::write_parquet(dados_MG_D_Cerebrovasc %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças Cerebro-vasculares/dados_MG_D_Cerebrovasc.parquet')
# arrow::write_parquet(dados_MS_D_Cerebrovasc %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças Cerebro-vasculares/dados_MS_D_Cerebrovasc.parquet')
# arrow::write_parquet(dados_MT_D_Cerebrovasc %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças Cerebro-vasculares/dados_MT_D_Cerebrovasc.parquet')
# arrow::write_parquet(dados_PA_D_Cerebrovasc %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças Cerebro-vasculares/dados_PA_D_Cerebrovasc.parquet')
# arrow::write_parquet(dados_PB_D_Cerebrovasc %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças Cerebro-vasculares/dados_PB_D_Cerebrovasc.parquet')
# arrow::write_parquet(dados_PE_D_Cerebrovasc %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças Cerebro-vasculares/dados_PE_D_Cerebrovasc.parquet')
# arrow::write_parquet(dados_PI_D_Cerebrovasc %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças Cerebro-vasculares/dados_PI_D_Cerebrovasc.parquet')
# arrow::write_parquet(dados_PR_D_Cerebrovasc %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças Cerebro-vasculares/dados_PR_D_Cerebrovasc.parquet')
# arrow::write_parquet(dados_RJ_D_Cerebrovasc %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças Cerebro-vasculares/dados_RJ_D_Cerebrovasc.parquet')
# arrow::write_parquet(dados_RN_D_Cerebrovasc %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças Cerebro-vasculares/dados_RN_D_Cerebrovasc.parquet')
# arrow::write_parquet(dados_RO_D_Cerebrovasc %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças Cerebro-vasculares/dados_RO_D_Cerebrovasc.parquet')
# arrow::write_parquet(dados_RR_D_Cerebrovasc %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças Cerebro-vasculares/dados_RR_D_Cerebrovasc.parquet')
# arrow::write_parquet(dados_RS_D_Cerebrovasc %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças Cerebro-vasculares/dados_RS_D_Cerebrovasc.parquet')
# arrow::write_parquet(dados_SC_D_Cerebrovasc %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças Cerebro-vasculares/dados_SC_D_Cerebrovasc.parquet')
# arrow::write_parquet(dados_SE_D_Cerebrovasc %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças Cerebro-vasculares/dados_SE_D_Cerebrovasc.parquet')
# arrow::write_parquet(dados_SP_D_Cerebrovasc %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças Cerebro-vasculares/dados_SP_D_Cerebrovasc.parquet')
# arrow::write_parquet(dados_TO_D_Cerebrovasc %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças Cerebro-vasculares/dados_TO_D_Cerebrovasc.parquet')

dados_D_Cerebrovasc = rbind(dados_AC_D_Cerebrovasc,dados_AL_D_Cerebrovasc,dados_AM_D_Cerebrovasc,dados_AP_D_Cerebrovasc,
                            dados_BA_D_Cerebrovasc,dados_CE_D_Cerebrovasc,dados_DF_D_Cerebrovasc,dados_ES_D_Cerebrovasc,
                            dados_GO_D_Cerebrovasc,dados_MA_D_Cerebrovasc,dados_MG_D_Cerebrovasc,dados_MS_D_Cerebrovasc,
                            dados_MT_D_Cerebrovasc,dados_PA_D_Cerebrovasc,dados_PB_D_Cerebrovasc,dados_PE_D_Cerebrovasc,
                            dados_PI_D_Cerebrovasc,dados_PR_D_Cerebrovasc,dados_RJ_D_Cerebrovasc,dados_RN_D_Cerebrovasc,
                            dados_RO_D_Cerebrovasc,dados_RR_D_Cerebrovasc,dados_RS_D_Cerebrovasc,dados_SC_D_Cerebrovasc,
                            dados_SE_D_Cerebrovasc,dados_SP_D_Cerebrovasc,dados_TO_D_Cerebrovasc)
# arrow::write_parquet(dados_D_Cerebrovasc %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças Cerebro-vasculares/dados_Doenças_Cerebrovasculares.parquet")

####==========================================================
#### Doenças preveníveis por imunização e condições evitáveis
####==========================================================
dados_AC_D_P_Imu_Cond_Evit = FiltraDadosCID(dados_AC, c("A15","A16","A17","A18","A19","A33","A34","A35","A36","A37","A50","A51","A52","A53","A95","B05","B16","B50","B51","B52","B53","B54","B77","G00"))
dados_AL_D_P_Imu_Cond_Evit = FiltraDadosCID(dados_AL, c("A15","A16","A17","A18","A19","A33","A34","A35","A36","A37","A50","A51","A52","A53","A95","B05","B16","B50","B51","B52","B53","B54","B77","G00"))
dados_AM_D_P_Imu_Cond_Evit = FiltraDadosCID(dados_AM, c("A15","A16","A17","A18","A19","A33","A34","A35","A36","A37","A50","A51","A52","A53","A95","B05","B16","B50","B51","B52","B53","B54","B77","G00"))
dados_AP_D_P_Imu_Cond_Evit = FiltraDadosCID(dados_AP, c("A15","A16","A17","A18","A19","A33","A34","A35","A36","A37","A50","A51","A52","A53","A95","B05","B16","B50","B51","B52","B53","B54","B77","G00"))
dados_BA_D_P_Imu_Cond_Evit = FiltraDadosCID(dados_BA, c("A15","A16","A17","A18","A19","A33","A34","A35","A36","A37","A50","A51","A52","A53","A95","B05","B16","B50","B51","B52","B53","B54","B77","G00"))
dados_CE_D_P_Imu_Cond_Evit = FiltraDadosCID(dados_CE, c("A15","A16","A17","A18","A19","A33","A34","A35","A36","A37","A50","A51","A52","A53","A95","B05","B16","B50","B51","B52","B53","B54","B77","G00"))
dados_DF_D_P_Imu_Cond_Evit = FiltraDadosCID(dados_DF, c("A15","A16","A17","A18","A19","A33","A34","A35","A36","A37","A50","A51","A52","A53","A95","B05","B16","B50","B51","B52","B53","B54","B77","G00"))
dados_ES_D_P_Imu_Cond_Evit = FiltraDadosCID(dados_ES, c("A15","A16","A17","A18","A19","A33","A34","A35","A36","A37","A50","A51","A52","A53","A95","B05","B16","B50","B51","B52","B53","B54","B77","G00"))
dados_GO_D_P_Imu_Cond_Evit = FiltraDadosCID(dados_GO, c("A15","A16","A17","A18","A19","A33","A34","A35","A36","A37","A50","A51","A52","A53","A95","B05","B16","B50","B51","B52","B53","B54","B77","G00"))
dados_MA_D_P_Imu_Cond_Evit = FiltraDadosCID(dados_MA, c("A15","A16","A17","A18","A19","A33","A34","A35","A36","A37","A50","A51","A52","A53","A95","B05","B16","B50","B51","B52","B53","B54","B77","G00"))
dados_MG_D_P_Imu_Cond_Evit = FiltraDadosCID(dados_MG, c("A15","A16","A17","A18","A19","A33","A34","A35","A36","A37","A50","A51","A52","A53","A95","B05","B16","B50","B51","B52","B53","B54","B77","G00"))
dados_MS_D_P_Imu_Cond_Evit = FiltraDadosCID(dados_MS, c("A15","A16","A17","A18","A19","A33","A34","A35","A36","A37","A50","A51","A52","A53","A95","B05","B16","B50","B51","B52","B53","B54","B77","G00"))
dados_MT_D_P_Imu_Cond_Evit = FiltraDadosCID(dados_MT, c("A15","A16","A17","A18","A19","A33","A34","A35","A36","A37","A50","A51","A52","A53","A95","B05","B16","B50","B51","B52","B53","B54","B77","G00"))
dados_PA_D_P_Imu_Cond_Evit = FiltraDadosCID(dados_PA, c("A15","A16","A17","A18","A19","A33","A34","A35","A36","A37","A50","A51","A52","A53","A95","B05","B16","B50","B51","B52","B53","B54","B77","G00"))
dados_PB_D_P_Imu_Cond_Evit = FiltraDadosCID(dados_PB, c("A15","A16","A17","A18","A19","A33","A34","A35","A36","A37","A50","A51","A52","A53","A95","B05","B16","B50","B51","B52","B53","B54","B77","G00"))
dados_PE_D_P_Imu_Cond_Evit = FiltraDadosCID(dados_PE, c("A15","A16","A17","A18","A19","A33","A34","A35","A36","A37","A50","A51","A52","A53","A95","B05","B16","B50","B51","B52","B53","B54","B77","G00"))
dados_PI_D_P_Imu_Cond_Evit = FiltraDadosCID(dados_PI, c("A15","A16","A17","A18","A19","A33","A34","A35","A36","A37","A50","A51","A52","A53","A95","B05","B16","B50","B51","B52","B53","B54","B77","G00"))
dados_PR_D_P_Imu_Cond_Evit = FiltraDadosCID(dados_PR, c("A15","A16","A17","A18","A19","A33","A34","A35","A36","A37","A50","A51","A52","A53","A95","B05","B16","B50","B51","B52","B53","B54","B77","G00"))
dados_RJ_D_P_Imu_Cond_Evit = FiltraDadosCID(dados_RJ, c("A15","A16","A17","A18","A19","A33","A34","A35","A36","A37","A50","A51","A52","A53","A95","B05","B16","B50","B51","B52","B53","B54","B77","G00"))
dados_RN_D_P_Imu_Cond_Evit = FiltraDadosCID(dados_RN, c("A15","A16","A17","A18","A19","A33","A34","A35","A36","A37","A50","A51","A52","A53","A95","B05","B16","B50","B51","B52","B53","B54","B77","G00"))
dados_RO_D_P_Imu_Cond_Evit = FiltraDadosCID(dados_RO, c("A15","A16","A17","A18","A19","A33","A34","A35","A36","A37","A50","A51","A52","A53","A95","B05","B16","B50","B51","B52","B53","B54","B77","G00"))
dados_RR_D_P_Imu_Cond_Evit = FiltraDadosCID(dados_RR, c("A15","A16","A17","A18","A19","A33","A34","A35","A36","A37","A50","A51","A52","A53","A95","B05","B16","B50","B51","B52","B53","B54","B77","G00"))
dados_RS_D_P_Imu_Cond_Evit = FiltraDadosCID(dados_RS, c("A15","A16","A17","A18","A19","A33","A34","A35","A36","A37","A50","A51","A52","A53","A95","B05","B16","B50","B51","B52","B53","B54","B77","G00"))
dados_SC_D_P_Imu_Cond_Evit = FiltraDadosCID(dados_SC, c("A15","A16","A17","A18","A19","A33","A34","A35","A36","A37","A50","A51","A52","A53","A95","B05","B16","B50","B51","B52","B53","B54","B77","G00"))
dados_SE_D_P_Imu_Cond_Evit = FiltraDadosCID(dados_SE, c("A15","A16","A17","A18","A19","A33","A34","A35","A36","A37","A50","A51","A52","A53","A95","B05","B16","B50","B51","B52","B53","B54","B77","G00"))
dados_SP_D_P_Imu_Cond_Evit = FiltraDadosCID(dados_SP, c("A15","A16","A17","A18","A19","A33","A34","A35","A36","A37","A50","A51","A52","A53","A95","B05","B16","B50","B51","B52","B53","B54","B77","G00"))
dados_TO_D_P_Imu_Cond_Evit = FiltraDadosCID(dados_TO, c("A15","A16","A17","A18","A19","A33","A34","A35","A36","A37","A50","A51","A52","A53","A95","B05","B16","B50","B51","B52","B53","B54","B77","G00"))

# arrow::write_parquet(dados_AC_D_P_Imu_Cond_Evit %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças preveníveis por imunização e condições evitáveis/dados_AC_D_P_Imu_Cond_Evit.parquet')
# arrow::write_parquet(dados_AL_D_P_Imu_Cond_Evit %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças preveníveis por imunização e condições evitáveis/dados_AL_D_P_Imu_Cond_Evit.parquet')
# arrow::write_parquet(dados_AM_D_P_Imu_Cond_Evit %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças preveníveis por imunização e condições evitáveis/dados_AM_D_P_Imu_Cond_Evit.parquet')
# arrow::write_parquet(dados_AP_D_P_Imu_Cond_Evit %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças preveníveis por imunização e condições evitáveis/dados_AP_D_P_Imu_Cond_Evit.parquet')
# arrow::write_parquet(dados_BA_D_P_Imu_Cond_Evit %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças preveníveis por imunização e condições evitáveis/dados_BA_D_P_Imu_Cond_Evit.parquet')
# arrow::write_parquet(dados_CE_D_P_Imu_Cond_Evit %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças preveníveis por imunização e condições evitáveis/dados_CE_D_P_Imu_Cond_Evit.parquet')
# arrow::write_parquet(dados_DF_D_P_Imu_Cond_Evit %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças preveníveis por imunização e condições evitáveis/dados_DF_D_P_Imu_Cond_Evit.parquet')
# arrow::write_parquet(dados_ES_D_P_Imu_Cond_Evit %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças preveníveis por imunização e condições evitáveis/dados_ES_D_P_Imu_Cond_Evit.parquet')
# arrow::write_parquet(dados_GO_D_P_Imu_Cond_Evit %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças preveníveis por imunização e condições evitáveis/dados_GO_D_P_Imu_Cond_Evit.parquet')
# arrow::write_parquet(dados_MA_D_P_Imu_Cond_Evit %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças preveníveis por imunização e condições evitáveis/dados_MA_D_P_Imu_Cond_Evit.parquet')
# arrow::write_parquet(dados_MG_D_P_Imu_Cond_Evit %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças preveníveis por imunização e condições evitáveis/dados_MG_D_P_Imu_Cond_Evit.parquet')
# arrow::write_parquet(dados_MS_D_P_Imu_Cond_Evit %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças preveníveis por imunização e condições evitáveis/dados_MS_D_P_Imu_Cond_Evit.parquet')
# arrow::write_parquet(dados_MT_D_P_Imu_Cond_Evit %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças preveníveis por imunização e condições evitáveis/dados_MT_D_P_Imu_Cond_Evit.parquet')
# arrow::write_parquet(dados_PA_D_P_Imu_Cond_Evit %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças preveníveis por imunização e condições evitáveis/dados_PA_D_P_Imu_Cond_Evit.parquet')
# arrow::write_parquet(dados_PB_D_P_Imu_Cond_Evit %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças preveníveis por imunização e condições evitáveis/dados_PB_D_P_Imu_Cond_Evit.parquet')
# arrow::write_parquet(dados_PE_D_P_Imu_Cond_Evit %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças preveníveis por imunização e condições evitáveis/dados_PE_D_P_Imu_Cond_Evit.parquet')
# arrow::write_parquet(dados_PI_D_P_Imu_Cond_Evit %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças preveníveis por imunização e condições evitáveis/dados_PI_D_P_Imu_Cond_Evit.parquet')
# arrow::write_parquet(dados_PR_D_P_Imu_Cond_Evit %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças preveníveis por imunização e condições evitáveis/dados_PR_D_P_Imu_Cond_Evit.parquet')
# arrow::write_parquet(dados_RJ_D_P_Imu_Cond_Evit %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças preveníveis por imunização e condições evitáveis/dados_RJ_D_P_Imu_Cond_Evit.parquet')
# arrow::write_parquet(dados_RN_D_P_Imu_Cond_Evit %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças preveníveis por imunização e condições evitáveis/dados_RN_D_P_Imu_Cond_Evit.parquet')
# arrow::write_parquet(dados_RO_D_P_Imu_Cond_Evit %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças preveníveis por imunização e condições evitáveis/dados_RO_D_P_Imu_Cond_Evit.parquet')
# arrow::write_parquet(dados_RR_D_P_Imu_Cond_Evit %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças preveníveis por imunização e condições evitáveis/dados_RR_D_P_Imu_Cond_Evit.parquet')
# arrow::write_parquet(dados_RS_D_P_Imu_Cond_Evit %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças preveníveis por imunização e condições evitáveis/dados_RS_D_P_Imu_Cond_Evit.parquet')
# arrow::write_parquet(dados_SC_D_P_Imu_Cond_Evit %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças preveníveis por imunização e condições evitáveis/dados_SC_D_P_Imu_Cond_Evit.parquet')
# arrow::write_parquet(dados_SE_D_P_Imu_Cond_Evit %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças preveníveis por imunização e condições evitáveis/dados_SE_D_P_Imu_Cond_Evit.parquet')
# arrow::write_parquet(dados_SP_D_P_Imu_Cond_Evit %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças preveníveis por imunização e condições evitáveis/dados_SP_D_P_Imu_Cond_Evit.parquet')
# arrow::write_parquet(dados_TO_D_P_Imu_Cond_Evit %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças preveníveis por imunização e condições evitáveis/dados_TO_D_P_Imu_Cond_Evit.parquet')

dados_D_P_Imu_Cond_Evit = 
  rbind(dados_AC_D_P_Imu_Cond_Evit,dados_AL_D_P_Imu_Cond_Evit,dados_AM_D_P_Imu_Cond_Evit,dados_AP_D_P_Imu_Cond_Evit,
        dados_BA_D_P_Imu_Cond_Evit,dados_CE_D_P_Imu_Cond_Evit,dados_DF_D_P_Imu_Cond_Evit,dados_ES_D_P_Imu_Cond_Evit,
        dados_GO_D_P_Imu_Cond_Evit,dados_MA_D_P_Imu_Cond_Evit,dados_MG_D_P_Imu_Cond_Evit,dados_MS_D_P_Imu_Cond_Evit,
        dados_MT_D_P_Imu_Cond_Evit,dados_PA_D_P_Imu_Cond_Evit,dados_PB_D_P_Imu_Cond_Evit,dados_PE_D_P_Imu_Cond_Evit,
        dados_PI_D_P_Imu_Cond_Evit,dados_PR_D_P_Imu_Cond_Evit,dados_RJ_D_P_Imu_Cond_Evit,dados_RN_D_P_Imu_Cond_Evit,
        dados_RO_D_P_Imu_Cond_Evit,dados_RR_D_P_Imu_Cond_Evit,dados_RS_D_P_Imu_Cond_Evit,dados_SC_D_P_Imu_Cond_Evit,
        dados_SE_D_P_Imu_Cond_Evit,dados_SP_D_P_Imu_Cond_Evit,dados_TO_D_P_Imu_Cond_Evit)
# arrow::write_parquet(dados_D_P_Imu_Cond_Evit %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças preveníveis por imunização e condições evitáveis/dados_Doenças_preveníveis_por_imunização_e_condições_evitáveis.parquet")

####===========================================
#### Doenças relacionadas ao Pré-Natal e Parto
####===========================================
dados_AC_D_Rel_PreNat_Parto = FiltraDadosCID(dados_AC, c("B20","B21","B22","B23","B24","O23","P00","P35","P70"))
dados_AL_D_Rel_PreNat_Parto = FiltraDadosCID(dados_AL, c("B20","B21","B22","B23","B24","O23","P00","P35","P70"))
dados_AM_D_Rel_PreNat_Parto = FiltraDadosCID(dados_AM, c("B20","B21","B22","B23","B24","O23","P00","P35","P70"))
dados_AP_D_Rel_PreNat_Parto = FiltraDadosCID(dados_AP, c("B20","B21","B22","B23","B24","O23","P00","P35","P70"))
dados_BA_D_Rel_PreNat_Parto = FiltraDadosCID(dados_BA, c("B20","B21","B22","B23","B24","O23","P00","P35","P70"))
dados_CE_D_Rel_PreNat_Parto = FiltraDadosCID(dados_CE, c("B20","B21","B22","B23","B24","O23","P00","P35","P70"))
dados_DF_D_Rel_PreNat_Parto = FiltraDadosCID(dados_DF, c("B20","B21","B22","B23","B24","O23","P00","P35","P70"))
dados_ES_D_Rel_PreNat_Parto = FiltraDadosCID(dados_ES, c("B20","B21","B22","B23","B24","O23","P00","P35","P70"))
dados_GO_D_Rel_PreNat_Parto = FiltraDadosCID(dados_GO, c("B20","B21","B22","B23","B24","O23","P00","P35","P70"))
dados_MA_D_Rel_PreNat_Parto = FiltraDadosCID(dados_MA, c("B20","B21","B22","B23","B24","O23","P00","P35","P70"))
dados_MG_D_Rel_PreNat_Parto = FiltraDadosCID(dados_MG, c("B20","B21","B22","B23","B24","O23","P00","P35","P70"))
dados_MS_D_Rel_PreNat_Parto = FiltraDadosCID(dados_MS, c("B20","B21","B22","B23","B24","O23","P00","P35","P70"))
dados_MT_D_Rel_PreNat_Parto = FiltraDadosCID(dados_MT, c("B20","B21","B22","B23","B24","O23","P00","P35","P70"))
dados_PA_D_Rel_PreNat_Parto = FiltraDadosCID(dados_PA, c("B20","B21","B22","B23","B24","O23","P00","P35","P70"))
dados_PB_D_Rel_PreNat_Parto = FiltraDadosCID(dados_PB, c("B20","B21","B22","B23","B24","O23","P00","P35","P70"))
dados_PE_D_Rel_PreNat_Parto = FiltraDadosCID(dados_PE, c("B20","B21","B22","B23","B24","O23","P00","P35","P70"))
dados_PI_D_Rel_PreNat_Parto = FiltraDadosCID(dados_PI, c("B20","B21","B22","B23","B24","O23","P00","P35","P70"))
dados_PR_D_Rel_PreNat_Parto = FiltraDadosCID(dados_PR, c("B20","B21","B22","B23","B24","O23","P00","P35","P70"))
dados_RJ_D_Rel_PreNat_Parto = FiltraDadosCID(dados_RJ, c("B20","B21","B22","B23","B24","O23","P00","P35","P70"))
dados_RN_D_Rel_PreNat_Parto = FiltraDadosCID(dados_RN, c("B20","B21","B22","B23","B24","O23","P00","P35","P70"))
dados_RO_D_Rel_PreNat_Parto = FiltraDadosCID(dados_RO, c("B20","B21","B22","B23","B24","O23","P00","P35","P70"))
dados_RR_D_Rel_PreNat_Parto = FiltraDadosCID(dados_RR, c("B20","B21","B22","B23","B24","O23","P00","P35","P70"))
dados_RS_D_Rel_PreNat_Parto = FiltraDadosCID(dados_RS, c("B20","B21","B22","B23","B24","O23","P00","P35","P70"))
dados_SC_D_Rel_PreNat_Parto = FiltraDadosCID(dados_SC, c("B20","B21","B22","B23","B24","O23","P00","P35","P70"))
dados_SE_D_Rel_PreNat_Parto = FiltraDadosCID(dados_SE, c("B20","B21","B22","B23","B24","O23","P00","P35","P70"))
dados_SP_D_Rel_PreNat_Parto = FiltraDadosCID(dados_SP, c("B20","B21","B22","B23","B24","O23","P00","P35","P70"))
dados_TO_D_Rel_PreNat_Parto = FiltraDadosCID(dados_TO, c("B20","B21","B22","B23","B24","O23","P00","P35","P70"))

# arrow::write_parquet(dados_AC_D_Rel_PreNat_Parto %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças relacionadas ao Pré-Natal e Parto/dados_AC_D_Rel_PreNat_Parto.parquet')
# arrow::write_parquet(dados_AL_D_Rel_PreNat_Parto %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças relacionadas ao Pré-Natal e Parto/dados_AL_D_Rel_PreNat_Parto.parquet')
# arrow::write_parquet(dados_AM_D_Rel_PreNat_Parto %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças relacionadas ao Pré-Natal e Parto/dados_AM_D_Rel_PreNat_Parto.parquet')
# arrow::write_parquet(dados_AP_D_Rel_PreNat_Parto %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças relacionadas ao Pré-Natal e Parto/dados_AP_D_Rel_PreNat_Parto.parquet')
# arrow::write_parquet(dados_BA_D_Rel_PreNat_Parto %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças relacionadas ao Pré-Natal e Parto/dados_BA_D_Rel_PreNat_Parto.parquet')
# arrow::write_parquet(dados_CE_D_Rel_PreNat_Parto %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças relacionadas ao Pré-Natal e Parto/dados_CE_D_Rel_PreNat_Parto.parquet')
# arrow::write_parquet(dados_DF_D_Rel_PreNat_Parto %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças relacionadas ao Pré-Natal e Parto/dados_DF_D_Rel_PreNat_Parto.parquet')
# arrow::write_parquet(dados_ES_D_Rel_PreNat_Parto %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças relacionadas ao Pré-Natal e Parto/dados_ES_D_Rel_PreNat_Parto.parquet')
# arrow::write_parquet(dados_GO_D_Rel_PreNat_Parto %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças relacionadas ao Pré-Natal e Parto/dados_GO_D_Rel_PreNat_Parto.parquet')
# arrow::write_parquet(dados_MA_D_Rel_PreNat_Parto %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças relacionadas ao Pré-Natal e Parto/dados_MA_D_Rel_PreNat_Parto.parquet')
# arrow::write_parquet(dados_MG_D_Rel_PreNat_Parto %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças relacionadas ao Pré-Natal e Parto/dados_MG_D_Rel_PreNat_Parto.parquet')
# arrow::write_parquet(dados_MS_D_Rel_PreNat_Parto %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças relacionadas ao Pré-Natal e Parto/dados_MS_D_Rel_PreNat_Parto.parquet')
# arrow::write_parquet(dados_MT_D_Rel_PreNat_Parto %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças relacionadas ao Pré-Natal e Parto/dados_MT_D_Rel_PreNat_Parto.parquet')
# arrow::write_parquet(dados_PA_D_Rel_PreNat_Parto %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças relacionadas ao Pré-Natal e Parto/dados_PA_D_Rel_PreNat_Parto.parquet')
# arrow::write_parquet(dados_PB_D_Rel_PreNat_Parto %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças relacionadas ao Pré-Natal e Parto/dados_PB_D_Rel_PreNat_Parto.parquet')
# arrow::write_parquet(dados_PE_D_Rel_PreNat_Parto %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças relacionadas ao Pré-Natal e Parto/dados_PE_D_Rel_PreNat_Parto.parquet')
# arrow::write_parquet(dados_PI_D_Rel_PreNat_Parto %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças relacionadas ao Pré-Natal e Parto/dados_PI_D_Rel_PreNat_Parto.parquet')
# arrow::write_parquet(dados_PR_D_Rel_PreNat_Parto %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças relacionadas ao Pré-Natal e Parto/dados_PR_D_Rel_PreNat_Parto.parquet')
# arrow::write_parquet(dados_RJ_D_Rel_PreNat_Parto %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças relacionadas ao Pré-Natal e Parto/dados_RJ_D_Rel_PreNat_Parto.parquet')
# arrow::write_parquet(dados_RN_D_Rel_PreNat_Parto %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças relacionadas ao Pré-Natal e Parto/dados_RN_D_Rel_PreNat_Parto.parquet')
# arrow::write_parquet(dados_RO_D_Rel_PreNat_Parto %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças relacionadas ao Pré-Natal e Parto/dados_RO_D_Rel_PreNat_Parto.parquet')
# arrow::write_parquet(dados_RR_D_Rel_PreNat_Parto %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças relacionadas ao Pré-Natal e Parto/dados_RR_D_Rel_PreNat_Parto.parquet')
# arrow::write_parquet(dados_RS_D_Rel_PreNat_Parto %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças relacionadas ao Pré-Natal e Parto/dados_RS_D_Rel_PreNat_Parto.parquet')
# arrow::write_parquet(dados_SC_D_Rel_PreNat_Parto %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças relacionadas ao Pré-Natal e Parto/dados_SC_D_Rel_PreNat_Parto.parquet')
# arrow::write_parquet(dados_SE_D_Rel_PreNat_Parto %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças relacionadas ao Pré-Natal e Parto/dados_SE_D_Rel_PreNat_Parto.parquet')
# arrow::write_parquet(dados_SP_D_Rel_PreNat_Parto %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças relacionadas ao Pré-Natal e Parto/dados_SP_D_Rel_PreNat_Parto.parquet')
# arrow::write_parquet(dados_TO_D_Rel_PreNat_Parto %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças relacionadas ao Pré-Natal e Parto/dados_TO_D_Rel_PreNat_Parto.parquet')

dados_D_Rel_PreNat_Parto = 
  rbind(dados_AC_D_Rel_PreNat_Parto,dados_AL_D_Rel_PreNat_Parto,dados_AM_D_Rel_PreNat_Parto,dados_AP_D_Rel_PreNat_Parto,
        dados_BA_D_Rel_PreNat_Parto,dados_CE_D_Rel_PreNat_Parto,dados_DF_D_Rel_PreNat_Parto,dados_ES_D_Rel_PreNat_Parto,
        dados_GO_D_Rel_PreNat_Parto,dados_MA_D_Rel_PreNat_Parto,dados_MG_D_Rel_PreNat_Parto,dados_MS_D_Rel_PreNat_Parto,
        dados_MT_D_Rel_PreNat_Parto,dados_PA_D_Rel_PreNat_Parto,dados_PB_D_Rel_PreNat_Parto,dados_PE_D_Rel_PreNat_Parto,
        dados_PI_D_Rel_PreNat_Parto,dados_PR_D_Rel_PreNat_Parto,dados_RJ_D_Rel_PreNat_Parto,dados_RN_D_Rel_PreNat_Parto,
        dados_RO_D_Rel_PreNat_Parto,dados_RR_D_Rel_PreNat_Parto,dados_RS_D_Rel_PreNat_Parto,dados_SC_D_Rel_PreNat_Parto,
        dados_SE_D_Rel_PreNat_Parto,dados_SP_D_Rel_PreNat_Parto,dados_TO_D_Rel_PreNat_Parto)
# arrow::write_parquet(dados_D_Rel_PreNat_Parto %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças relacionadas ao Pré-Natal e Parto/dados_Doenças_relacionadas_ao_PréNatal_e_Parto.parquet")

####============
#### Epilepsias
####============
dados_AC_Epilepsias = FiltraDadosCID(dados_AC, c("J45","J46"))
dados_AL_Epilepsias = FiltraDadosCID(dados_AL, c("J45","J46"))
dados_AM_Epilepsias = FiltraDadosCID(dados_AM, c("J45","J46"))
dados_AP_Epilepsias = FiltraDadosCID(dados_AP, c("J45","J46"))
dados_BA_Epilepsias = FiltraDadosCID(dados_BA, c("J45","J46"))
dados_CE_Epilepsias = FiltraDadosCID(dados_CE, c("J45","J46"))
dados_DF_Epilepsias = FiltraDadosCID(dados_DF, c("J45","J46"))
dados_ES_Epilepsias = FiltraDadosCID(dados_ES, c("J45","J46"))
dados_GO_Epilepsias = FiltraDadosCID(dados_GO, c("J45","J46"))
dados_MA_Epilepsias = FiltraDadosCID(dados_MA, c("J45","J46"))
dados_MG_Epilepsias = FiltraDadosCID(dados_MG, c("J45","J46"))
dados_MS_Epilepsias = FiltraDadosCID(dados_MS, c("J45","J46"))
dados_MT_Epilepsias = FiltraDadosCID(dados_MT, c("J45","J46"))
dados_PA_Epilepsias = FiltraDadosCID(dados_PA, c("J45","J46"))
dados_PB_Epilepsias = FiltraDadosCID(dados_PB, c("J45","J46"))
dados_PE_Epilepsias = FiltraDadosCID(dados_PE, c("J45","J46"))
dados_PI_Epilepsias = FiltraDadosCID(dados_PI, c("J45","J46"))
dados_PR_Epilepsias = FiltraDadosCID(dados_PR, c("J45","J46"))
dados_RJ_Epilepsias = FiltraDadosCID(dados_RJ, c("J45","J46"))
dados_RN_Epilepsias = FiltraDadosCID(dados_RN, c("J45","J46"))
dados_RO_Epilepsias = FiltraDadosCID(dados_RO, c("J45","J46"))
dados_RR_Epilepsias = FiltraDadosCID(dados_RR, c("J45","J46"))
dados_RS_Epilepsias = FiltraDadosCID(dados_RS, c("J45","J46"))
dados_SC_Epilepsias = FiltraDadosCID(dados_SC, c("J45","J46"))
dados_SE_Epilepsias = FiltraDadosCID(dados_SE, c("J45","J46"))
dados_SP_Epilepsias = FiltraDadosCID(dados_SP, c("J45","J46"))
dados_TO_Epilepsias = FiltraDadosCID(dados_TO, c("J45","J46"))

# arrow::write_parquet(dados_AC_Epilepsias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Epilepsias/dados_AC_Epilepsias.parquet')
# arrow::write_parquet(dados_AL_Epilepsias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Epilepsias/dados_AL_Epilepsias.parquet')
# arrow::write_parquet(dados_AM_Epilepsias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Epilepsias/dados_AM_Epilepsias.parquet')
# arrow::write_parquet(dados_AP_Epilepsias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Epilepsias/dados_AP_Epilepsias.parquet')
# arrow::write_parquet(dados_BA_Epilepsias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Epilepsias/dados_BA_Epilepsias.parquet')
# arrow::write_parquet(dados_CE_Epilepsias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Epilepsias/dados_CE_Epilepsias.parquet')
# arrow::write_parquet(dados_DF_Epilepsias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Epilepsias/dados_DF_Epilepsias.parquet')
# arrow::write_parquet(dados_ES_Epilepsias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Epilepsias/dados_ES_Epilepsias.parquet')
# arrow::write_parquet(dados_GO_Epilepsias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Epilepsias/dados_GO_Epilepsias.parquet')
# arrow::write_parquet(dados_MA_Epilepsias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Epilepsias/dados_MA_Epilepsias.parquet')
# arrow::write_parquet(dados_MG_Epilepsias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Epilepsias/dados_MG_Epilepsias.parquet')
# arrow::write_parquet(dados_MS_Epilepsias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Epilepsias/dados_MS_Epilepsias.parquet')
# arrow::write_parquet(dados_MT_Epilepsias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Epilepsias/dados_MT_Epilepsias.parquet')
# arrow::write_parquet(dados_PA_Epilepsias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Epilepsias/dados_PA_Epilepsias.parquet')
# arrow::write_parquet(dados_PB_Epilepsias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Epilepsias/dados_PB_Epilepsias.parquet')
# arrow::write_parquet(dados_PE_Epilepsias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Epilepsias/dados_PE_Epilepsias.parquet')
# arrow::write_parquet(dados_PI_Epilepsias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Epilepsias/dados_PI_Epilepsias.parquet')
# arrow::write_parquet(dados_PR_Epilepsias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Epilepsias/dados_PR_Epilepsias.parquet')
# arrow::write_parquet(dados_RJ_Epilepsias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Epilepsias/dados_RJ_Epilepsias.parquet')
# arrow::write_parquet(dados_RN_Epilepsias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Epilepsias/dados_RN_Epilepsias.parquet')
# arrow::write_parquet(dados_RO_Epilepsias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Epilepsias/dados_RO_Epilepsias.parquet')
# arrow::write_parquet(dados_RR_Epilepsias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Epilepsias/dados_RR_Epilepsias.parquet')
# arrow::write_parquet(dados_RS_Epilepsias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Epilepsias/dados_RS_Epilepsias.parquet')
# arrow::write_parquet(dados_SC_Epilepsias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Epilepsias/dados_SC_Epilepsias.parquet')
# arrow::write_parquet(dados_SE_Epilepsias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Epilepsias/dados_SE_Epilepsias.parquet')
# arrow::write_parquet(dados_SP_Epilepsias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Epilepsias/dados_SP_Epilepsias.parquet')
# arrow::write_parquet(dados_TO_Epilepsias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Epilepsias/dados_TO_Epilepsias.parquet')

dados_Epilepsias = rbind(dados_AC_Epilepsias,dados_AL_Epilepsias,dados_AM_Epilepsias,dados_AP_Epilepsias,
                         dados_BA_Epilepsias,dados_CE_Epilepsias,dados_DF_Epilepsias,dados_ES_Epilepsias,
                         dados_GO_Epilepsias,dados_MA_Epilepsias,dados_MG_Epilepsias,dados_MS_Epilepsias,
                         dados_MT_Epilepsias,dados_PA_Epilepsias,dados_PB_Epilepsias,dados_PE_Epilepsias,
                         dados_PI_Epilepsias,dados_PR_Epilepsias,dados_RJ_Epilepsias,dados_RN_Epilepsias,
                         dados_RO_Epilepsias,dados_RR_Epilepsias,dados_RS_Epilepsias,dados_SC_Epilepsias,
                         dados_SE_Epilepsias,dados_SP_Epilepsias,dados_TO_Epilepsias)
# arrow::write_parquet(dados_Epilepsias %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Epilepsias/dados_Epilepsias.parquet")

####============================================
#### Gastroenterites Infecciosas e complicações
####============================================
dados_AC_Gastro_Inf_Comp = FiltraDadosCID(dados_AC, c("A00","A01","A02","A03","A04","A05","A06","A07","A08","A09","E86"))
dados_AL_Gastro_Inf_Comp = FiltraDadosCID(dados_AL, c("A00","A01","A02","A03","A04","A05","A06","A07","A08","A09","E86"))
dados_AM_Gastro_Inf_Comp = FiltraDadosCID(dados_AM, c("A00","A01","A02","A03","A04","A05","A06","A07","A08","A09","E86"))
dados_AP_Gastro_Inf_Comp = FiltraDadosCID(dados_AP, c("A00","A01","A02","A03","A04","A05","A06","A07","A08","A09","E86"))
dados_BA_Gastro_Inf_Comp = FiltraDadosCID(dados_BA, c("A00","A01","A02","A03","A04","A05","A06","A07","A08","A09","E86"))
dados_CE_Gastro_Inf_Comp = FiltraDadosCID(dados_CE, c("A00","A01","A02","A03","A04","A05","A06","A07","A08","A09","E86"))
dados_DF_Gastro_Inf_Comp = FiltraDadosCID(dados_DF, c("A00","A01","A02","A03","A04","A05","A06","A07","A08","A09","E86"))
dados_ES_Gastro_Inf_Comp = FiltraDadosCID(dados_ES, c("A00","A01","A02","A03","A04","A05","A06","A07","A08","A09","E86"))
dados_GO_Gastro_Inf_Comp = FiltraDadosCID(dados_GO, c("A00","A01","A02","A03","A04","A05","A06","A07","A08","A09","E86"))
dados_MA_Gastro_Inf_Comp = FiltraDadosCID(dados_MA, c("A00","A01","A02","A03","A04","A05","A06","A07","A08","A09","E86"))
dados_MG_Gastro_Inf_Comp = FiltraDadosCID(dados_MG, c("A00","A01","A02","A03","A04","A05","A06","A07","A08","A09","E86"))
dados_MS_Gastro_Inf_Comp = FiltraDadosCID(dados_MS, c("A00","A01","A02","A03","A04","A05","A06","A07","A08","A09","E86"))
dados_MT_Gastro_Inf_Comp = FiltraDadosCID(dados_MT, c("A00","A01","A02","A03","A04","A05","A06","A07","A08","A09","E86"))
dados_PA_Gastro_Inf_Comp = FiltraDadosCID(dados_PA, c("A00","A01","A02","A03","A04","A05","A06","A07","A08","A09","E86"))
dados_PB_Gastro_Inf_Comp = FiltraDadosCID(dados_PB, c("A00","A01","A02","A03","A04","A05","A06","A07","A08","A09","E86"))
dados_PE_Gastro_Inf_Comp = FiltraDadosCID(dados_PE, c("A00","A01","A02","A03","A04","A05","A06","A07","A08","A09","E86"))
dados_PI_Gastro_Inf_Comp = FiltraDadosCID(dados_PI, c("A00","A01","A02","A03","A04","A05","A06","A07","A08","A09","E86"))
dados_PR_Gastro_Inf_Comp = FiltraDadosCID(dados_PR, c("A00","A01","A02","A03","A04","A05","A06","A07","A08","A09","E86"))
dados_RJ_Gastro_Inf_Comp = FiltraDadosCID(dados_RJ, c("A00","A01","A02","A03","A04","A05","A06","A07","A08","A09","E86"))
dados_RN_Gastro_Inf_Comp = FiltraDadosCID(dados_RN, c("A00","A01","A02","A03","A04","A05","A06","A07","A08","A09","E86"))
dados_RO_Gastro_Inf_Comp = FiltraDadosCID(dados_RO, c("A00","A01","A02","A03","A04","A05","A06","A07","A08","A09","E86"))
dados_RR_Gastro_Inf_Comp = FiltraDadosCID(dados_RR, c("A00","A01","A02","A03","A04","A05","A06","A07","A08","A09","E86"))
dados_RS_Gastro_Inf_Comp = FiltraDadosCID(dados_RS, c("A00","A01","A02","A03","A04","A05","A06","A07","A08","A09","E86"))
dados_SC_Gastro_Inf_Comp = FiltraDadosCID(dados_SC, c("A00","A01","A02","A03","A04","A05","A06","A07","A08","A09","E86"))
dados_SE_Gastro_Inf_Comp = FiltraDadosCID(dados_SE, c("A00","A01","A02","A03","A04","A05","A06","A07","A08","A09","E86"))
dados_SP_Gastro_Inf_Comp = FiltraDadosCID(dados_SP, c("A00","A01","A02","A03","A04","A05","A06","A07","A08","A09","E86"))
dados_TO_Gastro_Inf_Comp = FiltraDadosCID(dados_TO, c("A00","A01","A02","A03","A04","A05","A06","A07","A08","A09","E86"))

# arrow::write_parquet(dados_AC_Gastro_Inf_Comp %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Gastroenterites Infecciosas e complicações/dados_AC_Gastro_Inf_Comp.parquet')
# arrow::write_parquet(dados_AL_Gastro_Inf_Comp %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Gastroenterites Infecciosas e complicações/dados_AL_Gastro_Inf_Comp.parquet')
# arrow::write_parquet(dados_AM_Gastro_Inf_Comp %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Gastroenterites Infecciosas e complicações/dados_AM_Gastro_Inf_Comp.parquet')
# arrow::write_parquet(dados_AP_Gastro_Inf_Comp %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Gastroenterites Infecciosas e complicações/dados_AP_Gastro_Inf_Comp.parquet')
# arrow::write_parquet(dados_BA_Gastro_Inf_Comp %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Gastroenterites Infecciosas e complicações/dados_BA_Gastro_Inf_Comp.parquet')
# arrow::write_parquet(dados_CE_Gastro_Inf_Comp %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Gastroenterites Infecciosas e complicações/dados_CE_Gastro_Inf_Comp.parquet')
# arrow::write_parquet(dados_DF_Gastro_Inf_Comp %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Gastroenterites Infecciosas e complicações/dados_DF_Gastro_Inf_Comp.parquet')
# arrow::write_parquet(dados_ES_Gastro_Inf_Comp %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Gastroenterites Infecciosas e complicações/dados_ES_Gastro_Inf_Comp.parquet')
# arrow::write_parquet(dados_GO_Gastro_Inf_Comp %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Gastroenterites Infecciosas e complicações/dados_GO_Gastro_Inf_Comp.parquet')
# arrow::write_parquet(dados_MA_Gastro_Inf_Comp %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Gastroenterites Infecciosas e complicações/dados_MA_Gastro_Inf_Comp.parquet')
# arrow::write_parquet(dados_MG_Gastro_Inf_Comp %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Gastroenterites Infecciosas e complicações/dados_MG_Gastro_Inf_Comp.parquet')
# arrow::write_parquet(dados_MS_Gastro_Inf_Comp %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Gastroenterites Infecciosas e complicações/dados_MS_Gastro_Inf_Comp.parquet')
# arrow::write_parquet(dados_MT_Gastro_Inf_Comp %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Gastroenterites Infecciosas e complicações/dados_MT_Gastro_Inf_Comp.parquet')
# arrow::write_parquet(dados_PA_Gastro_Inf_Comp %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Gastroenterites Infecciosas e complicações/dados_PA_Gastro_Inf_Comp.parquet')
# arrow::write_parquet(dados_PB_Gastro_Inf_Comp %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Gastroenterites Infecciosas e complicações/dados_PB_Gastro_Inf_Comp.parquet')
# arrow::write_parquet(dados_PE_Gastro_Inf_Comp %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Gastroenterites Infecciosas e complicações/dados_PE_Gastro_Inf_Comp.parquet')
# arrow::write_parquet(dados_PI_Gastro_Inf_Comp %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Gastroenterites Infecciosas e complicações/dados_PI_Gastro_Inf_Comp.parquet')
# arrow::write_parquet(dados_PR_Gastro_Inf_Comp %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Gastroenterites Infecciosas e complicações/dados_PR_Gastro_Inf_Comp.parquet')
# arrow::write_parquet(dados_RJ_Gastro_Inf_Comp %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Gastroenterites Infecciosas e complicações/dados_RJ_Gastro_Inf_Comp.parquet')
# arrow::write_parquet(dados_RN_Gastro_Inf_Comp %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Gastroenterites Infecciosas e complicações/dados_RN_Gastro_Inf_Comp.parquet')
# arrow::write_parquet(dados_RO_Gastro_Inf_Comp %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Gastroenterites Infecciosas e complicações/dados_RO_Gastro_Inf_Comp.parquet')
# arrow::write_parquet(dados_RR_Gastro_Inf_Comp %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Gastroenterites Infecciosas e complicações/dados_RR_Gastro_Inf_Comp.parquet')
# arrow::write_parquet(dados_RS_Gastro_Inf_Comp %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Gastroenterites Infecciosas e complicações/dados_RS_Gastro_Inf_Comp.parquet')
# arrow::write_parquet(dados_SC_Gastro_Inf_Comp %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Gastroenterites Infecciosas e complicações/dados_SC_Gastro_Inf_Comp.parquet')
# arrow::write_parquet(dados_SE_Gastro_Inf_Comp %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Gastroenterites Infecciosas e complicações/dados_SE_Gastro_Inf_Comp.parquet')
# arrow::write_parquet(dados_SP_Gastro_Inf_Comp %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Gastroenterites Infecciosas e complicações/dados_SP_Gastro_Inf_Comp.parquet')
# arrow::write_parquet(dados_TO_Gastro_Inf_Comp %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Gastroenterites Infecciosas e complicações/dados_TO_Gastro_Inf_Comp.parquet')

dados_Gastro_Inf_Comp = 
  rbind(dados_AC_Gastro_Inf_Comp,dados_AL_Gastro_Inf_Comp,dados_AM_Gastro_Inf_Comp,dados_AP_Gastro_Inf_Comp,
        dados_BA_Gastro_Inf_Comp,dados_CE_Gastro_Inf_Comp,dados_DF_Gastro_Inf_Comp,dados_ES_Gastro_Inf_Comp,
        dados_GO_Gastro_Inf_Comp,dados_MA_Gastro_Inf_Comp,dados_MG_Gastro_Inf_Comp,dados_MS_Gastro_Inf_Comp,
        dados_MT_Gastro_Inf_Comp,dados_PA_Gastro_Inf_Comp,dados_PB_Gastro_Inf_Comp,dados_PE_Gastro_Inf_Comp,
        dados_PI_Gastro_Inf_Comp,dados_PR_Gastro_Inf_Comp,dados_RJ_Gastro_Inf_Comp,dados_RN_Gastro_Inf_Comp,
        dados_RO_Gastro_Inf_Comp,dados_RR_Gastro_Inf_Comp,dados_RS_Gastro_Inf_Comp,dados_SC_Gastro_Inf_Comp,
        dados_SE_Gastro_Inf_Comp,dados_SP_Gastro_Inf_Comp,dados_TO_Gastro_Inf_Comp)
# arrow::write_parquet(dados_Gastro_Inf_Comp %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Gastroenterites Infecciosas e complicações/dados_Gastroenterites_Infecciosas_e_complicações.parquet")

####=============
#### Hipertensão
####=============
dados_AC_Hipertensao = FiltraDadosCID(dados_AC, c("I10","I11"))
dados_AL_Hipertensao = FiltraDadosCID(dados_AL, c("I10","I11"))
dados_AM_Hipertensao = FiltraDadosCID(dados_AM, c("I10","I11"))
dados_AP_Hipertensao = FiltraDadosCID(dados_AP, c("I10","I11"))
dados_BA_Hipertensao = FiltraDadosCID(dados_BA, c("I10","I11"))
dados_CE_Hipertensao = FiltraDadosCID(dados_CE, c("I10","I11"))
dados_DF_Hipertensao = FiltraDadosCID(dados_DF, c("I10","I11"))
dados_ES_Hipertensao = FiltraDadosCID(dados_ES, c("I10","I11"))
dados_GO_Hipertensao = FiltraDadosCID(dados_GO, c("I10","I11"))
dados_MA_Hipertensao = FiltraDadosCID(dados_MA, c("I10","I11"))
dados_MG_Hipertensao = FiltraDadosCID(dados_MG, c("I10","I11"))
dados_MS_Hipertensao = FiltraDadosCID(dados_MS, c("I10","I11"))
dados_MT_Hipertensao = FiltraDadosCID(dados_MT, c("I10","I11"))
dados_PA_Hipertensao = FiltraDadosCID(dados_PA, c("I10","I11"))
dados_PB_Hipertensao = FiltraDadosCID(dados_PB, c("I10","I11"))
dados_PE_Hipertensao = FiltraDadosCID(dados_PE, c("I10","I11"))
dados_PI_Hipertensao = FiltraDadosCID(dados_PI, c("I10","I11"))
dados_PR_Hipertensao = FiltraDadosCID(dados_PR, c("I10","I11"))
dados_RJ_Hipertensao = FiltraDadosCID(dados_RJ, c("I10","I11"))
dados_RN_Hipertensao = FiltraDadosCID(dados_RN, c("I10","I11"))
dados_RO_Hipertensao = FiltraDadosCID(dados_RO, c("I10","I11"))
dados_RR_Hipertensao = FiltraDadosCID(dados_RR, c("I10","I11"))
dados_RS_Hipertensao = FiltraDadosCID(dados_RS, c("I10","I11"))
dados_SC_Hipertensao = FiltraDadosCID(dados_SC, c("I10","I11"))
dados_SE_Hipertensao = FiltraDadosCID(dados_SE, c("I10","I11"))
dados_SP_Hipertensao = FiltraDadosCID(dados_SP, c("I10","I11"))
dados_TO_Hipertensao = FiltraDadosCID(dados_TO, c("I10","I11"))

# arrow::write_parquet(dados_AC_Hipertensao %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Hipertensão/dados_AC_Hipertensao.parquet')
# arrow::write_parquet(dados_AL_Hipertensao %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Hipertensão/dados_AL_Hipertensao.parquet')
# arrow::write_parquet(dados_AM_Hipertensao %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Hipertensão/dados_AM_Hipertensao.parquet')
# arrow::write_parquet(dados_AP_Hipertensao %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Hipertensão/dados_AP_Hipertensao.parquet')
# arrow::write_parquet(dados_BA_Hipertensao %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Hipertensão/dados_BA_Hipertensao.parquet')
# arrow::write_parquet(dados_CE_Hipertensao %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Hipertensão/dados_CE_Hipertensao.parquet')
# arrow::write_parquet(dados_DF_Hipertensao %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Hipertensão/dados_DF_Hipertensao.parquet')
# arrow::write_parquet(dados_ES_Hipertensao %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Hipertensão/dados_ES_Hipertensao.parquet')
# arrow::write_parquet(dados_GO_Hipertensao %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Hipertensão/dados_GO_Hipertensao.parquet')
# arrow::write_parquet(dados_MA_Hipertensao %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Hipertensão/dados_MA_Hipertensao.parquet')
# arrow::write_parquet(dados_MG_Hipertensao %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Hipertensão/dados_MG_Hipertensao.parquet')
# arrow::write_parquet(dados_MS_Hipertensao %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Hipertensão/dados_MS_Hipertensao.parquet')
# arrow::write_parquet(dados_MT_Hipertensao %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Hipertensão/dados_MT_Hipertensao.parquet')
# arrow::write_parquet(dados_PA_Hipertensao %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Hipertensão/dados_PA_Hipertensao.parquet')
# arrow::write_parquet(dados_PB_Hipertensao %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Hipertensão/dados_PB_Hipertensao.parquet')
# arrow::write_parquet(dados_PE_Hipertensao %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Hipertensão/dados_PE_Hipertensao.parquet')
# arrow::write_parquet(dados_PI_Hipertensao %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Hipertensão/dados_PI_Hipertensao.parquet')
# arrow::write_parquet(dados_PR_Hipertensao %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Hipertensão/dados_PR_Hipertensao.parquet')
# arrow::write_parquet(dados_RJ_Hipertensao %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Hipertensão/dados_RJ_Hipertensao.parquet')
# arrow::write_parquet(dados_RN_Hipertensao %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Hipertensão/dados_RN_Hipertensao.parquet')
# arrow::write_parquet(dados_RO_Hipertensao %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Hipertensão/dados_RO_Hipertensao.parquet')
# arrow::write_parquet(dados_RR_Hipertensao %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Hipertensão/dados_RR_Hipertensao.parquet')
# arrow::write_parquet(dados_RS_Hipertensao %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Hipertensão/dados_RS_Hipertensao.parquet')
# arrow::write_parquet(dados_SC_Hipertensao %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Hipertensão/dados_SC_Hipertensao.parquet')
# arrow::write_parquet(dados_SE_Hipertensao %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Hipertensão/dados_SE_Hipertensao.parquet')
# arrow::write_parquet(dados_SP_Hipertensao %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Hipertensão/dados_SP_Hipertensao.parquet')
# arrow::write_parquet(dados_TO_Hipertensao %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Hipertensão/dados_TO_Hipertensao.parquet')

dados_Hipertensao = rbind(dados_AC_Hipertensao,dados_AL_Hipertensao,dados_AM_Hipertensao,dados_AP_Hipertensao,
                          dados_BA_Hipertensao,dados_CE_Hipertensao,dados_DF_Hipertensao,dados_ES_Hipertensao,
                          dados_GO_Hipertensao,dados_MA_Hipertensao,dados_MG_Hipertensao,dados_MS_Hipertensao,
                          dados_MT_Hipertensao,dados_PA_Hipertensao,dados_PB_Hipertensao,dados_PE_Hipertensao,
                          dados_PI_Hipertensao,dados_PR_Hipertensao,dados_RJ_Hipertensao,dados_RN_Hipertensao,
                          dados_RO_Hipertensao,dados_RR_Hipertensao,dados_RS_Hipertensao,dados_SC_Hipertensao,
                          dados_SE_Hipertensao,dados_SP_Hipertensao,dados_TO_Hipertensao)
# arrow::write_parquet(dados_Hipertensao %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Hipertensão/dados_Hipertensão.parquet")

####======================================
#### Infecção da pele e tecido subcutâneo
####======================================
dados_AC_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_AC, c("A46","L01","L02","L03","L04","L08"))
dados_AL_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_AL, c("A46","L01","L02","L03","L04","L08"))
dados_AM_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_AM, c("A46","L01","L02","L03","L04","L08"))
dados_AP_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_AP, c("A46","L01","L02","L03","L04","L08"))
dados_BA_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_BA, c("A46","L01","L02","L03","L04","L08"))
dados_CE_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_CE, c("A46","L01","L02","L03","L04","L08"))
dados_DF_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_DF, c("A46","L01","L02","L03","L04","L08"))
dados_ES_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_ES, c("A46","L01","L02","L03","L04","L08"))
dados_GO_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_GO, c("A46","L01","L02","L03","L04","L08"))
dados_MA_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_MA, c("A46","L01","L02","L03","L04","L08"))
dados_MG_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_MG, c("A46","L01","L02","L03","L04","L08"))
dados_MS_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_MS, c("A46","L01","L02","L03","L04","L08"))
dados_MT_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_MT, c("A46","L01","L02","L03","L04","L08"))
dados_PA_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_PA, c("A46","L01","L02","L03","L04","L08"))
dados_PB_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_PB, c("A46","L01","L02","L03","L04","L08"))
dados_PE_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_PE, c("A46","L01","L02","L03","L04","L08"))
dados_PI_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_PI, c("A46","L01","L02","L03","L04","L08"))
dados_PR_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_PR, c("A46","L01","L02","L03","L04","L08"))
dados_RJ_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_RJ, c("A46","L01","L02","L03","L04","L08"))
dados_RN_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_RN, c("A46","L01","L02","L03","L04","L08"))
dados_RO_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_RO, c("A46","L01","L02","L03","L04","L08"))
dados_RR_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_RR, c("A46","L01","L02","L03","L04","L08"))
dados_RS_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_RS, c("A46","L01","L02","L03","L04","L08"))
dados_SC_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_SC, c("A46","L01","L02","L03","L04","L08"))
dados_SE_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_SE, c("A46","L01","L02","L03","L04","L08"))
dados_SP_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_SP, c("A46","L01","L02","L03","L04","L08"))
dados_TO_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_TO, c("A46","L01","L02","L03","L04","L08"))

# arrow::write_parquet(dados_AC_Inf_Pele_Tec_Sub %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção da pele e tecido subcutâneo/dados_AC_Inf_Pele_Tec_Sub.parquet')
# arrow::write_parquet(dados_AL_Inf_Pele_Tec_Sub %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção da pele e tecido subcutâneo/dados_AL_Inf_Pele_Tec_Sub.parquet')
# arrow::write_parquet(dados_AM_Inf_Pele_Tec_Sub %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção da pele e tecido subcutâneo/dados_AM_Inf_Pele_Tec_Sub.parquet')
# arrow::write_parquet(dados_AP_Inf_Pele_Tec_Sub %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção da pele e tecido subcutâneo/dados_AP_Inf_Pele_Tec_Sub.parquet')
# arrow::write_parquet(dados_BA_Inf_Pele_Tec_Sub %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção da pele e tecido subcutâneo/dados_BA_Inf_Pele_Tec_Sub.parquet')
# arrow::write_parquet(dados_CE_Inf_Pele_Tec_Sub %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção da pele e tecido subcutâneo/dados_CE_Inf_Pele_Tec_Sub.parquet')
# arrow::write_parquet(dados_DF_Inf_Pele_Tec_Sub %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção da pele e tecido subcutâneo/dados_DF_Inf_Pele_Tec_Sub.parquet')
# arrow::write_parquet(dados_ES_Inf_Pele_Tec_Sub %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção da pele e tecido subcutâneo/dados_ES_Inf_Pele_Tec_Sub.parquet')
# arrow::write_parquet(dados_GO_Inf_Pele_Tec_Sub %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção da pele e tecido subcutâneo/dados_GO_Inf_Pele_Tec_Sub.parquet')
# arrow::write_parquet(dados_MA_Inf_Pele_Tec_Sub %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção da pele e tecido subcutâneo/dados_MA_Inf_Pele_Tec_Sub.parquet')
# arrow::write_parquet(dados_MG_Inf_Pele_Tec_Sub %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção da pele e tecido subcutâneo/dados_MG_Inf_Pele_Tec_Sub.parquet')
# arrow::write_parquet(dados_MS_Inf_Pele_Tec_Sub %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção da pele e tecido subcutâneo/dados_MS_Inf_Pele_Tec_Sub.parquet')
# arrow::write_parquet(dados_MT_Inf_Pele_Tec_Sub %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção da pele e tecido subcutâneo/dados_MT_Inf_Pele_Tec_Sub.parquet')
# arrow::write_parquet(dados_PA_Inf_Pele_Tec_Sub %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção da pele e tecido subcutâneo/dados_PA_Inf_Pele_Tec_Sub.parquet')
# arrow::write_parquet(dados_PB_Inf_Pele_Tec_Sub %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção da pele e tecido subcutâneo/dados_PB_Inf_Pele_Tec_Sub.parquet')
# arrow::write_parquet(dados_PE_Inf_Pele_Tec_Sub %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção da pele e tecido subcutâneo/dados_PE_Inf_Pele_Tec_Sub.parquet')
# arrow::write_parquet(dados_PI_Inf_Pele_Tec_Sub %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção da pele e tecido subcutâneo/dados_PI_Inf_Pele_Tec_Sub.parquet')
# arrow::write_parquet(dados_PR_Inf_Pele_Tec_Sub %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção da pele e tecido subcutâneo/dados_PR_Inf_Pele_Tec_Sub.parquet')
# arrow::write_parquet(dados_RJ_Inf_Pele_Tec_Sub %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção da pele e tecido subcutâneo/dados_RJ_Inf_Pele_Tec_Sub.parquet')
# arrow::write_parquet(dados_RN_Inf_Pele_Tec_Sub %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção da pele e tecido subcutâneo/dados_RN_Inf_Pele_Tec_Sub.parquet')
# arrow::write_parquet(dados_RO_Inf_Pele_Tec_Sub %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção da pele e tecido subcutâneo/dados_RO_Inf_Pele_Tec_Sub.parquet')
# arrow::write_parquet(dados_RR_Inf_Pele_Tec_Sub %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção da pele e tecido subcutâneo/dados_RR_Inf_Pele_Tec_Sub.parquet')
# arrow::write_parquet(dados_RS_Inf_Pele_Tec_Sub %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção da pele e tecido subcutâneo/dados_RS_Inf_Pele_Tec_Sub.parquet')
# arrow::write_parquet(dados_SC_Inf_Pele_Tec_Sub %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção da pele e tecido subcutâneo/dados_SC_Inf_Pele_Tec_Sub.parquet')
# arrow::write_parquet(dados_SE_Inf_Pele_Tec_Sub %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção da pele e tecido subcutâneo/dados_SE_Inf_Pele_Tec_Sub.parquet')
# arrow::write_parquet(dados_SP_Inf_Pele_Tec_Sub %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção da pele e tecido subcutâneo/dados_SP_Inf_Pele_Tec_Sub.parquet')
# arrow::write_parquet(dados_TO_Inf_Pele_Tec_Sub %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção da pele e tecido subcutâneo/dados_TO_Inf_Pele_Tec_Sub.parquet')

dados_Inf_Pele_Tec_Sub = 
  rbind(dados_AC_Inf_Pele_Tec_Sub,dados_AL_Inf_Pele_Tec_Sub,dados_AM_Inf_Pele_Tec_Sub,dados_AP_Inf_Pele_Tec_Sub,
        dados_BA_Inf_Pele_Tec_Sub,dados_CE_Inf_Pele_Tec_Sub,dados_DF_Inf_Pele_Tec_Sub,dados_ES_Inf_Pele_Tec_Sub,
        dados_GO_Inf_Pele_Tec_Sub,dados_MA_Inf_Pele_Tec_Sub,dados_MG_Inf_Pele_Tec_Sub,dados_MS_Inf_Pele_Tec_Sub,
        dados_MT_Inf_Pele_Tec_Sub,dados_PA_Inf_Pele_Tec_Sub,dados_PB_Inf_Pele_Tec_Sub,dados_PE_Inf_Pele_Tec_Sub,
        dados_PI_Inf_Pele_Tec_Sub,dados_PR_Inf_Pele_Tec_Sub,dados_RJ_Inf_Pele_Tec_Sub,dados_RN_Inf_Pele_Tec_Sub,
        dados_RO_Inf_Pele_Tec_Sub,dados_RR_Inf_Pele_Tec_Sub,dados_RS_Inf_Pele_Tec_Sub,dados_SC_Inf_Pele_Tec_Sub,
        dados_SE_Inf_Pele_Tec_Sub,dados_SP_Inf_Pele_Tec_Sub,dados_TO_Inf_Pele_Tec_Sub)
# arrow::write_parquet(dados_Inf_Pele_Tec_Sub %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção da pele e tecido subcutâneo/dados_Infecção_da_pele_e_tecido_subcutâneo.parquet")

####=====================================
#### Infecção no Rim e no Trato Urinário
####=====================================
dados_AC_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_AC, c("N00","N10","N11","N12","N15","N30","N34","N39"))
dados_AL_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_AL, c("N00","N10","N11","N12","N15","N30","N34","N39"))
dados_AM_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_AM, c("N00","N10","N11","N12","N15","N30","N34","N39"))
dados_AP_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_AP, c("N00","N10","N11","N12","N15","N30","N34","N39"))
dados_BA_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_BA, c("N00","N10","N11","N12","N15","N30","N34","N39"))
dados_CE_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_CE, c("N00","N10","N11","N12","N15","N30","N34","N39"))
dados_DF_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_DF, c("N00","N10","N11","N12","N15","N30","N34","N39"))
dados_ES_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_ES, c("N00","N10","N11","N12","N15","N30","N34","N39"))
dados_GO_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_GO, c("N00","N10","N11","N12","N15","N30","N34","N39"))
dados_MA_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_MA, c("N00","N10","N11","N12","N15","N30","N34","N39"))
dados_MG_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_MG, c("N00","N10","N11","N12","N15","N30","N34","N39"))
dados_MS_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_MS, c("N00","N10","N11","N12","N15","N30","N34","N39"))
dados_MT_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_MT, c("N00","N10","N11","N12","N15","N30","N34","N39"))
dados_PA_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_PA, c("N00","N10","N11","N12","N15","N30","N34","N39"))
dados_PB_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_PB, c("N00","N10","N11","N12","N15","N30","N34","N39"))
dados_PE_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_PE, c("N00","N10","N11","N12","N15","N30","N34","N39"))
dados_PI_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_PI, c("N00","N10","N11","N12","N15","N30","N34","N39"))
dados_PR_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_PR, c("N00","N10","N11","N12","N15","N30","N34","N39"))
dados_RJ_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_RJ, c("N00","N10","N11","N12","N15","N30","N34","N39"))
dados_RN_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_RN, c("N00","N10","N11","N12","N15","N30","N34","N39"))
dados_RO_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_RO, c("N00","N10","N11","N12","N15","N30","N34","N39"))
dados_RR_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_RR, c("N00","N10","N11","N12","N15","N30","N34","N39"))
dados_RS_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_RS, c("N00","N10","N11","N12","N15","N30","N34","N39"))
dados_SC_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_SC, c("N00","N10","N11","N12","N15","N30","N34","N39"))
dados_SE_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_SE, c("N00","N10","N11","N12","N15","N30","N34","N39"))
dados_SP_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_SP, c("N00","N10","N11","N12","N15","N30","N34","N39"))
dados_TO_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_TO, c("N00","N10","N11","N12","N15","N30","N34","N39"))

# arrow::write_parquet(dados_AC_Inf_Rim_Tr_Urin %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção no Rim e no Trato Urinário/dados_AC_Inf_Rim_Tr_Urin.parquet')
# arrow::write_parquet(dados_AL_Inf_Rim_Tr_Urin %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção no Rim e no Trato Urinário/dados_AL_Inf_Rim_Tr_Urin.parquet')
# arrow::write_parquet(dados_AM_Inf_Rim_Tr_Urin %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção no Rim e no Trato Urinário/dados_AM_Inf_Rim_Tr_Urin.parquet')
# arrow::write_parquet(dados_AP_Inf_Rim_Tr_Urin %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção no Rim e no Trato Urinário/dados_AP_Inf_Rim_Tr_Urin.parquet')
# arrow::write_parquet(dados_BA_Inf_Rim_Tr_Urin %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção no Rim e no Trato Urinário/dados_BA_Inf_Rim_Tr_Urin.parquet')
# arrow::write_parquet(dados_CE_Inf_Rim_Tr_Urin %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção no Rim e no Trato Urinário/dados_CE_Inf_Rim_Tr_Urin.parquet')
# arrow::write_parquet(dados_DF_Inf_Rim_Tr_Urin %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção no Rim e no Trato Urinário/dados_DF_Inf_Rim_Tr_Urin.parquet')
# arrow::write_parquet(dados_ES_Inf_Rim_Tr_Urin %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção no Rim e no Trato Urinário/dados_ES_Inf_Rim_Tr_Urin.parquet')
# arrow::write_parquet(dados_GO_Inf_Rim_Tr_Urin %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção no Rim e no Trato Urinário/dados_GO_Inf_Rim_Tr_Urin.parquet')
# arrow::write_parquet(dados_MA_Inf_Rim_Tr_Urin %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção no Rim e no Trato Urinário/dados_MA_Inf_Rim_Tr_Urin.parquet')
# arrow::write_parquet(dados_MG_Inf_Rim_Tr_Urin %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção no Rim e no Trato Urinário/dados_MG_Inf_Rim_Tr_Urin.parquet')
# arrow::write_parquet(dados_MS_Inf_Rim_Tr_Urin %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção no Rim e no Trato Urinário/dados_MS_Inf_Rim_Tr_Urin.parquet')
# arrow::write_parquet(dados_MT_Inf_Rim_Tr_Urin %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção no Rim e no Trato Urinário/dados_MT_Inf_Rim_Tr_Urin.parquet')
# arrow::write_parquet(dados_PA_Inf_Rim_Tr_Urin %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção no Rim e no Trato Urinário/dados_PA_Inf_Rim_Tr_Urin.parquet')
# arrow::write_parquet(dados_PB_Inf_Rim_Tr_Urin %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção no Rim e no Trato Urinário/dados_PB_Inf_Rim_Tr_Urin.parquet')
# arrow::write_parquet(dados_PE_Inf_Rim_Tr_Urin %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção no Rim e no Trato Urinário/dados_PE_Inf_Rim_Tr_Urin.parquet')
# arrow::write_parquet(dados_PI_Inf_Rim_Tr_Urin %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção no Rim e no Trato Urinário/dados_PI_Inf_Rim_Tr_Urin.parquet')
# arrow::write_parquet(dados_PR_Inf_Rim_Tr_Urin %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção no Rim e no Trato Urinário/dados_PR_Inf_Rim_Tr_Urin.parquet')
# arrow::write_parquet(dados_RJ_Inf_Rim_Tr_Urin %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção no Rim e no Trato Urinário/dados_RJ_Inf_Rim_Tr_Urin.parquet')
# arrow::write_parquet(dados_RN_Inf_Rim_Tr_Urin %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção no Rim e no Trato Urinário/dados_RN_Inf_Rim_Tr_Urin.parquet')
# arrow::write_parquet(dados_RO_Inf_Rim_Tr_Urin %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção no Rim e no Trato Urinário/dados_RO_Inf_Rim_Tr_Urin.parquet')
# arrow::write_parquet(dados_RR_Inf_Rim_Tr_Urin %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção no Rim e no Trato Urinário/dados_RR_Inf_Rim_Tr_Urin.parquet')
# arrow::write_parquet(dados_RS_Inf_Rim_Tr_Urin %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção no Rim e no Trato Urinário/dados_RS_Inf_Rim_Tr_Urin.parquet')
# arrow::write_parquet(dados_SC_Inf_Rim_Tr_Urin %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção no Rim e no Trato Urinário/dados_SC_Inf_Rim_Tr_Urin.parquet')
# arrow::write_parquet(dados_SE_Inf_Rim_Tr_Urin %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção no Rim e no Trato Urinário/dados_SE_Inf_Rim_Tr_Urin.parquet')
# arrow::write_parquet(dados_SP_Inf_Rim_Tr_Urin %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção no Rim e no Trato Urinário/dados_SP_Inf_Rim_Tr_Urin.parquet')
# arrow::write_parquet(dados_TO_Inf_Rim_Tr_Urin %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção no Rim e no Trato Urinário/dados_TO_Inf_Rim_Tr_Urin.parquet')

dados_Inf_Rim_Tr_Urin = 
  rbind(dados_AC_Inf_Rim_Tr_Urin,dados_AL_Inf_Rim_Tr_Urin,dados_AM_Inf_Rim_Tr_Urin,dados_AP_Inf_Rim_Tr_Urin,
        dados_BA_Inf_Rim_Tr_Urin,dados_CE_Inf_Rim_Tr_Urin,dados_DF_Inf_Rim_Tr_Urin,dados_ES_Inf_Rim_Tr_Urin,
        dados_GO_Inf_Rim_Tr_Urin,dados_MA_Inf_Rim_Tr_Urin,dados_MG_Inf_Rim_Tr_Urin,dados_MS_Inf_Rim_Tr_Urin,
        dados_MT_Inf_Rim_Tr_Urin,dados_PA_Inf_Rim_Tr_Urin,dados_PB_Inf_Rim_Tr_Urin,dados_PE_Inf_Rim_Tr_Urin,
        dados_PI_Inf_Rim_Tr_Urin,dados_PR_Inf_Rim_Tr_Urin,dados_RJ_Inf_Rim_Tr_Urin,dados_RN_Inf_Rim_Tr_Urin,
        dados_RO_Inf_Rim_Tr_Urin,dados_RR_Inf_Rim_Tr_Urin,dados_RS_Inf_Rim_Tr_Urin,dados_SC_Inf_Rim_Tr_Urin,
        dados_SE_Inf_Rim_Tr_Urin,dados_SP_Inf_Rim_Tr_Urin,dados_TO_Inf_Rim_Tr_Urin)
# arrow::write_parquet(dados_Inf_Rim_Tr_Urin %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção no Rim e no Trato Urinário/dados_Infecção_no_Rim_e_no_Trato_Urinário.parquet")

####=======================================
#### Infecções de ouvido, nariz e garganta
####=======================================
dados_AC_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_AC, c("H66","I00","I01","I02","J00","J01","J02","J03","J06","J31"))
dados_AL_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_AL, c("H66","I00","I01","I02","J00","J01","J02","J03","J06","J31"))
dados_AM_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_AM, c("H66","I00","I01","I02","J00","J01","J02","J03","J06","J31"))
dados_AP_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_AP, c("H66","I00","I01","I02","J00","J01","J02","J03","J06","J31"))
dados_BA_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_BA, c("H66","I00","I01","I02","J00","J01","J02","J03","J06","J31"))
dados_CE_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_CE, c("H66","I00","I01","I02","J00","J01","J02","J03","J06","J31"))
dados_DF_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_DF, c("H66","I00","I01","I02","J00","J01","J02","J03","J06","J31"))
dados_ES_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_ES, c("H66","I00","I01","I02","J00","J01","J02","J03","J06","J31"))
dados_GO_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_GO, c("H66","I00","I01","I02","J00","J01","J02","J03","J06","J31"))
dados_MA_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_MA, c("H66","I00","I01","I02","J00","J01","J02","J03","J06","J31"))
dados_MG_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_MG, c("H66","I00","I01","I02","J00","J01","J02","J03","J06","J31"))
dados_MS_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_MS, c("H66","I00","I01","I02","J00","J01","J02","J03","J06","J31"))
dados_MT_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_MT, c("H66","I00","I01","I02","J00","J01","J02","J03","J06","J31"))
dados_PA_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_PA, c("H66","I00","I01","I02","J00","J01","J02","J03","J06","J31"))
dados_PB_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_PB, c("H66","I00","I01","I02","J00","J01","J02","J03","J06","J31"))
dados_PE_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_PE, c("H66","I00","I01","I02","J00","J01","J02","J03","J06","J31"))
dados_PI_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_PI, c("H66","I00","I01","I02","J00","J01","J02","J03","J06","J31"))
dados_PR_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_PR, c("H66","I00","I01","I02","J00","J01","J02","J03","J06","J31"))
dados_RJ_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_RJ, c("H66","I00","I01","I02","J00","J01","J02","J03","J06","J31"))
dados_RN_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_RN, c("H66","I00","I01","I02","J00","J01","J02","J03","J06","J31"))
dados_RO_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_RO, c("H66","I00","I01","I02","J00","J01","J02","J03","J06","J31"))
dados_RR_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_RR, c("H66","I00","I01","I02","J00","J01","J02","J03","J06","J31"))
dados_RS_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_RS, c("H66","I00","I01","I02","J00","J01","J02","J03","J06","J31"))
dados_SC_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_SC, c("H66","I00","I01","I02","J00","J01","J02","J03","J06","J31"))
dados_SE_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_SE, c("H66","I00","I01","I02","J00","J01","J02","J03","J06","J31"))
dados_SP_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_SP, c("H66","I00","I01","I02","J00","J01","J02","J03","J06","J31"))
dados_TO_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_TO, c("H66","I00","I01","I02","J00","J01","J02","J03","J06","J31"))

# arrow::write_parquet(dados_AC_Inf_Ouv_Nariz_Garg %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecções de ouvido, nariz e garganta/dados_AC_Inf_Ouv_Nariz_Garg.parquet')
# arrow::write_parquet(dados_AL_Inf_Ouv_Nariz_Garg %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecções de ouvido, nariz e garganta/dados_AL_Inf_Ouv_Nariz_Garg.parquet')
# arrow::write_parquet(dados_AM_Inf_Ouv_Nariz_Garg %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecções de ouvido, nariz e garganta/dados_AM_Inf_Ouv_Nariz_Garg.parquet')
# arrow::write_parquet(dados_AP_Inf_Ouv_Nariz_Garg %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecções de ouvido, nariz e garganta/dados_AP_Inf_Ouv_Nariz_Garg.parquet')
# arrow::write_parquet(dados_BA_Inf_Ouv_Nariz_Garg %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecções de ouvido, nariz e garganta/dados_BA_Inf_Ouv_Nariz_Garg.parquet')
# arrow::write_parquet(dados_CE_Inf_Ouv_Nariz_Garg %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecções de ouvido, nariz e garganta/dados_CE_Inf_Ouv_Nariz_Garg.parquet')
# arrow::write_parquet(dados_DF_Inf_Ouv_Nariz_Garg %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecções de ouvido, nariz e garganta/dados_DF_Inf_Ouv_Nariz_Garg.parquet')
# arrow::write_parquet(dados_ES_Inf_Ouv_Nariz_Garg %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecções de ouvido, nariz e garganta/dados_ES_Inf_Ouv_Nariz_Garg.parquet')
# arrow::write_parquet(dados_GO_Inf_Ouv_Nariz_Garg %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecções de ouvido, nariz e garganta/dados_GO_Inf_Ouv_Nariz_Garg.parquet')
# arrow::write_parquet(dados_MA_Inf_Ouv_Nariz_Garg %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecções de ouvido, nariz e garganta/dados_MA_Inf_Ouv_Nariz_Garg.parquet')
# arrow::write_parquet(dados_MG_Inf_Ouv_Nariz_Garg %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecções de ouvido, nariz e garganta/dados_MG_Inf_Ouv_Nariz_Garg.parquet')
# arrow::write_parquet(dados_MS_Inf_Ouv_Nariz_Garg %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecções de ouvido, nariz e garganta/dados_MS_Inf_Ouv_Nariz_Garg.parquet')
# arrow::write_parquet(dados_MT_Inf_Ouv_Nariz_Garg %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecções de ouvido, nariz e garganta/dados_MT_Inf_Ouv_Nariz_Garg.parquet')
# arrow::write_parquet(dados_PA_Inf_Ouv_Nariz_Garg %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecções de ouvido, nariz e garganta/dados_PA_Inf_Ouv_Nariz_Garg.parquet')
# arrow::write_parquet(dados_PB_Inf_Ouv_Nariz_Garg %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecções de ouvido, nariz e garganta/dados_PB_Inf_Ouv_Nariz_Garg.parquet')
# arrow::write_parquet(dados_PE_Inf_Ouv_Nariz_Garg %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecções de ouvido, nariz e garganta/dados_PE_Inf_Ouv_Nariz_Garg.parquet')
# arrow::write_parquet(dados_PI_Inf_Ouv_Nariz_Garg %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecções de ouvido, nariz e garganta/dados_PI_Inf_Ouv_Nariz_Garg.parquet')
# arrow::write_parquet(dados_PR_Inf_Ouv_Nariz_Garg %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecções de ouvido, nariz e garganta/dados_PR_Inf_Ouv_Nariz_Garg.parquet')
# arrow::write_parquet(dados_RJ_Inf_Ouv_Nariz_Garg %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecções de ouvido, nariz e garganta/dados_RJ_Inf_Ouv_Nariz_Garg.parquet')
# arrow::write_parquet(dados_RN_Inf_Ouv_Nariz_Garg %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecções de ouvido, nariz e garganta/dados_RN_Inf_Ouv_Nariz_Garg.parquet')
# arrow::write_parquet(dados_RO_Inf_Ouv_Nariz_Garg %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecções de ouvido, nariz e garganta/dados_RO_Inf_Ouv_Nariz_Garg.parquet')
# arrow::write_parquet(dados_RR_Inf_Ouv_Nariz_Garg %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecções de ouvido, nariz e garganta/dados_RR_Inf_Ouv_Nariz_Garg.parquet')
# arrow::write_parquet(dados_RS_Inf_Ouv_Nariz_Garg %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecções de ouvido, nariz e garganta/dados_RS_Inf_Ouv_Nariz_Garg.parquet')
# arrow::write_parquet(dados_SC_Inf_Ouv_Nariz_Garg %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecções de ouvido, nariz e garganta/dados_SC_Inf_Ouv_Nariz_Garg.parquet')
# arrow::write_parquet(dados_SE_Inf_Ouv_Nariz_Garg %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecções de ouvido, nariz e garganta/dados_SE_Inf_Ouv_Nariz_Garg.parquet')
# arrow::write_parquet(dados_SP_Inf_Ouv_Nariz_Garg %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecções de ouvido, nariz e garganta/dados_SP_Inf_Ouv_Nariz_Garg.parquet')
# arrow::write_parquet(dados_TO_Inf_Ouv_Nariz_Garg %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecções de ouvido, nariz e garganta/dados_TO_Inf_Ouv_Nariz_Garg.parquet')

dados_Inf_Ouv_Nariz_Garg = 
  rbind(dados_AC_Inf_Ouv_Nariz_Garg,dados_AL_Inf_Ouv_Nariz_Garg,dados_AM_Inf_Ouv_Nariz_Garg,dados_AP_Inf_Ouv_Nariz_Garg,
        dados_BA_Inf_Ouv_Nariz_Garg,dados_CE_Inf_Ouv_Nariz_Garg,dados_DF_Inf_Ouv_Nariz_Garg,dados_ES_Inf_Ouv_Nariz_Garg,
        dados_GO_Inf_Ouv_Nariz_Garg,dados_MA_Inf_Ouv_Nariz_Garg,dados_MG_Inf_Ouv_Nariz_Garg,dados_MS_Inf_Ouv_Nariz_Garg,
        dados_MT_Inf_Ouv_Nariz_Garg,dados_PA_Inf_Ouv_Nariz_Garg,dados_PB_Inf_Ouv_Nariz_Garg,dados_PE_Inf_Ouv_Nariz_Garg,
        dados_PI_Inf_Ouv_Nariz_Garg,dados_PR_Inf_Ouv_Nariz_Garg,dados_RJ_Inf_Ouv_Nariz_Garg,dados_RN_Inf_Ouv_Nariz_Garg,
        dados_RO_Inf_Ouv_Nariz_Garg,dados_RR_Inf_Ouv_Nariz_Garg,dados_RS_Inf_Ouv_Nariz_Garg,dados_SC_Inf_Ouv_Nariz_Garg,
        dados_SE_Inf_Ouv_Nariz_Garg,dados_SP_Inf_Ouv_Nariz_Garg,dados_TO_Inf_Ouv_Nariz_Garg)
# arrow::write_parquet(dados_Inf_Ouv_Nariz_Garg %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecções de ouvido, nariz e garganta/dados_Infecções_de_ouvido_nariz_e_garganta.parquet")

####========================
#### Insuficiência Cardíaca
####========================
dados_AC_Insuf_Card = FiltraDadosCID(dados_AC, c("I50","J81"))
dados_AL_Insuf_Card = FiltraDadosCID(dados_AL, c("I50","J81"))
dados_AM_Insuf_Card = FiltraDadosCID(dados_AM, c("I50","J81"))
dados_AP_Insuf_Card = FiltraDadosCID(dados_AP, c("I50","J81"))
dados_BA_Insuf_Card = FiltraDadosCID(dados_BA, c("I50","J81"))
dados_CE_Insuf_Card = FiltraDadosCID(dados_CE, c("I50","J81"))
dados_DF_Insuf_Card = FiltraDadosCID(dados_DF, c("I50","J81"))
dados_ES_Insuf_Card = FiltraDadosCID(dados_ES, c("I50","J81"))
dados_GO_Insuf_Card = FiltraDadosCID(dados_GO, c("I50","J81"))
dados_MA_Insuf_Card = FiltraDadosCID(dados_MA, c("I50","J81"))
dados_MG_Insuf_Card = FiltraDadosCID(dados_MG, c("I50","J81"))
dados_MS_Insuf_Card = FiltraDadosCID(dados_MS, c("I50","J81"))
dados_MT_Insuf_Card = FiltraDadosCID(dados_MT, c("I50","J81"))
dados_PA_Insuf_Card = FiltraDadosCID(dados_PA, c("I50","J81"))
dados_PB_Insuf_Card = FiltraDadosCID(dados_PB, c("I50","J81"))
dados_PE_Insuf_Card = FiltraDadosCID(dados_PE, c("I50","J81"))
dados_PI_Insuf_Card = FiltraDadosCID(dados_PI, c("I50","J81"))
dados_PR_Insuf_Card = FiltraDadosCID(dados_PR, c("I50","J81"))
dados_RJ_Insuf_Card = FiltraDadosCID(dados_RJ, c("I50","J81"))
dados_RN_Insuf_Card = FiltraDadosCID(dados_RN, c("I50","J81"))
dados_RO_Insuf_Card = FiltraDadosCID(dados_RO, c("I50","J81"))
dados_RR_Insuf_Card = FiltraDadosCID(dados_RR, c("I50","J81"))
dados_RS_Insuf_Card = FiltraDadosCID(dados_RS, c("I50","J81"))
dados_SC_Insuf_Card = FiltraDadosCID(dados_SC, c("I50","J81"))
dados_SE_Insuf_Card = FiltraDadosCID(dados_SE, c("I50","J81"))
dados_SP_Insuf_Card = FiltraDadosCID(dados_SP, c("I50","J81"))
dados_TO_Insuf_Card = FiltraDadosCID(dados_TO, c("I50","J81"))

# arrow::write_parquet(dados_AC_Insuf_Card %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Insuficiência Cardíaca/dados_AC_Insuf_Card.parquet')
# arrow::write_parquet(dados_AL_Insuf_Card %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Insuficiência Cardíaca/dados_AL_Insuf_Card.parquet')
# arrow::write_parquet(dados_AM_Insuf_Card %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Insuficiência Cardíaca/dados_AM_Insuf_Card.parquet')
# arrow::write_parquet(dados_AP_Insuf_Card %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Insuficiência Cardíaca/dados_AP_Insuf_Card.parquet')
# arrow::write_parquet(dados_BA_Insuf_Card %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Insuficiência Cardíaca/dados_BA_Insuf_Card.parquet')
# arrow::write_parquet(dados_CE_Insuf_Card %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Insuficiência Cardíaca/dados_CE_Insuf_Card.parquet')
# arrow::write_parquet(dados_DF_Insuf_Card %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Insuficiência Cardíaca/dados_DF_Insuf_Card.parquet')
# arrow::write_parquet(dados_ES_Insuf_Card %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Insuficiência Cardíaca/dados_ES_Insuf_Card.parquet')
# arrow::write_parquet(dados_GO_Insuf_Card %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Insuficiência Cardíaca/dados_GO_Insuf_Card.parquet')
# arrow::write_parquet(dados_MA_Insuf_Card %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Insuficiência Cardíaca/dados_MA_Insuf_Card.parquet')
# arrow::write_parquet(dados_MG_Insuf_Card %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Insuficiência Cardíaca/dados_MG_Insuf_Card.parquet')
# arrow::write_parquet(dados_MS_Insuf_Card %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Insuficiência Cardíaca/dados_MS_Insuf_Card.parquet')
# arrow::write_parquet(dados_MT_Insuf_Card %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Insuficiência Cardíaca/dados_MT_Insuf_Card.parquet')
# arrow::write_parquet(dados_PA_Insuf_Card %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Insuficiência Cardíaca/dados_PA_Insuf_Card.parquet')
# arrow::write_parquet(dados_PB_Insuf_Card %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Insuficiência Cardíaca/dados_PB_Insuf_Card.parquet')
# arrow::write_parquet(dados_PE_Insuf_Card %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Insuficiência Cardíaca/dados_PE_Insuf_Card.parquet')
# arrow::write_parquet(dados_PI_Insuf_Card %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Insuficiência Cardíaca/dados_PI_Insuf_Card.parquet')
# arrow::write_parquet(dados_PR_Insuf_Card %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Insuficiência Cardíaca/dados_PR_Insuf_Card.parquet')
# arrow::write_parquet(dados_RJ_Insuf_Card %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Insuficiência Cardíaca/dados_RJ_Insuf_Card.parquet')
# arrow::write_parquet(dados_RN_Insuf_Card %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Insuficiência Cardíaca/dados_RN_Insuf_Card.parquet')
# arrow::write_parquet(dados_RO_Insuf_Card %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Insuficiência Cardíaca/dados_RO_Insuf_Card.parquet')
# arrow::write_parquet(dados_RR_Insuf_Card %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Insuficiência Cardíaca/dados_RR_Insuf_Card.parquet')
# arrow::write_parquet(dados_RS_Insuf_Card %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Insuficiência Cardíaca/dados_RS_Insuf_Card.parquet')
# arrow::write_parquet(dados_SC_Insuf_Card %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Insuficiência Cardíaca/dados_SC_Insuf_Card.parquet')
# arrow::write_parquet(dados_SE_Insuf_Card %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Insuficiência Cardíaca/dados_SE_Insuf_Card.parquet')
# arrow::write_parquet(dados_SP_Insuf_Card %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Insuficiência Cardíaca/dados_SP_Insuf_Card.parquet')
# arrow::write_parquet(dados_TO_Insuf_Card %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Insuficiência Cardíaca/dados_TO_Insuf_Card.parquet')

dados_Insuf_Card = rbind(dados_AC_Insuf_Card,dados_AL_Insuf_Card,dados_AM_Insuf_Card,dados_AP_Insuf_Card,
                         dados_BA_Insuf_Card,dados_CE_Insuf_Card,dados_DF_Insuf_Card,dados_ES_Insuf_Card,
                         dados_GO_Insuf_Card,dados_MA_Insuf_Card,dados_MG_Insuf_Card,dados_MS_Insuf_Card,
                         dados_MT_Insuf_Card,dados_PA_Insuf_Card,dados_PB_Insuf_Card,dados_PE_Insuf_Card,
                         dados_PI_Insuf_Card,dados_PR_Insuf_Card,dados_RJ_Insuf_Card,dados_RN_Insuf_Card,
                         dados_RO_Insuf_Card,dados_RR_Insuf_Card,dados_RS_Insuf_Card,dados_SC_Insuf_Card,
                         dados_SE_Insuf_Card,dados_SP_Insuf_Card,dados_TO_Insuf_Card)
# arrow::write_parquet(dados_Insuf_Card %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Insuficiência Cardíaca/dados_Insuficiência_Cardíaca.parquet")

####============================
#### Neoplasia maligna do útero
####============================
dados_AC_Neo_Maligna_Utero = FiltraDadosCID(dados_AC, c("C53","C54","C55"))
dados_AL_Neo_Maligna_Utero = FiltraDadosCID(dados_AL, c("C53","C54","C55"))
dados_AM_Neo_Maligna_Utero = FiltraDadosCID(dados_AM, c("C53","C54","C55"))
dados_AP_Neo_Maligna_Utero = FiltraDadosCID(dados_AP, c("C53","C54","C55"))
dados_BA_Neo_Maligna_Utero = FiltraDadosCID(dados_BA, c("C53","C54","C55"))
dados_CE_Neo_Maligna_Utero = FiltraDadosCID(dados_CE, c("C53","C54","C55"))
dados_DF_Neo_Maligna_Utero = FiltraDadosCID(dados_DF, c("C53","C54","C55"))
dados_ES_Neo_Maligna_Utero = FiltraDadosCID(dados_ES, c("C53","C54","C55"))
dados_GO_Neo_Maligna_Utero = FiltraDadosCID(dados_GO, c("C53","C54","C55"))
dados_MA_Neo_Maligna_Utero = FiltraDadosCID(dados_MA, c("C53","C54","C55"))
dados_MG_Neo_Maligna_Utero = FiltraDadosCID(dados_MG, c("C53","C54","C55"))
dados_MS_Neo_Maligna_Utero = FiltraDadosCID(dados_MS, c("C53","C54","C55"))
dados_MT_Neo_Maligna_Utero = FiltraDadosCID(dados_MT, c("C53","C54","C55"))
dados_PA_Neo_Maligna_Utero = FiltraDadosCID(dados_PA, c("C53","C54","C55"))
dados_PB_Neo_Maligna_Utero = FiltraDadosCID(dados_PB, c("C53","C54","C55"))
dados_PE_Neo_Maligna_Utero = FiltraDadosCID(dados_PE, c("C53","C54","C55"))
dados_PI_Neo_Maligna_Utero = FiltraDadosCID(dados_PI, c("C53","C54","C55"))
dados_PR_Neo_Maligna_Utero = FiltraDadosCID(dados_PR, c("C53","C54","C55"))
dados_RJ_Neo_Maligna_Utero = FiltraDadosCID(dados_RJ, c("C53","C54","C55"))
dados_RN_Neo_Maligna_Utero = FiltraDadosCID(dados_RN, c("C53","C54","C55"))
dados_RO_Neo_Maligna_Utero = FiltraDadosCID(dados_RO, c("C53","C54","C55"))
dados_RR_Neo_Maligna_Utero = FiltraDadosCID(dados_RR, c("C53","C54","C55"))
dados_RS_Neo_Maligna_Utero = FiltraDadosCID(dados_RS, c("C53","C54","C55"))
dados_SC_Neo_Maligna_Utero = FiltraDadosCID(dados_SC, c("C53","C54","C55"))
dados_SE_Neo_Maligna_Utero = FiltraDadosCID(dados_SE, c("C53","C54","C55"))
dados_SP_Neo_Maligna_Utero = FiltraDadosCID(dados_SP, c("C53","C54","C55"))
dados_TO_Neo_Maligna_Utero = FiltraDadosCID(dados_TO, c("C53","C54","C55"))

# arrow::write_parquet(dados_AC_Neo_Maligna_Utero %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Neoplasia maligna do útero/dados_AC_Neo_Maligna_Utero.parquet')
# arrow::write_parquet(dados_AL_Neo_Maligna_Utero %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Neoplasia maligna do útero/dados_AL_Neo_Maligna_Utero.parquet')
# arrow::write_parquet(dados_AM_Neo_Maligna_Utero %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Neoplasia maligna do útero/dados_AM_Neo_Maligna_Utero.parquet')
# arrow::write_parquet(dados_AP_Neo_Maligna_Utero %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Neoplasia maligna do útero/dados_AP_Neo_Maligna_Utero.parquet')
# arrow::write_parquet(dados_BA_Neo_Maligna_Utero %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Neoplasia maligna do útero/dados_BA_Neo_Maligna_Utero.parquet')
# arrow::write_parquet(dados_CE_Neo_Maligna_Utero %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Neoplasia maligna do útero/dados_CE_Neo_Maligna_Utero.parquet')
# arrow::write_parquet(dados_DF_Neo_Maligna_Utero %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Neoplasia maligna do útero/dados_DF_Neo_Maligna_Utero.parquet')
# arrow::write_parquet(dados_ES_Neo_Maligna_Utero %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Neoplasia maligna do útero/dados_ES_Neo_Maligna_Utero.parquet')
# arrow::write_parquet(dados_GO_Neo_Maligna_Utero %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Neoplasia maligna do útero/dados_GO_Neo_Maligna_Utero.parquet')
# arrow::write_parquet(dados_MA_Neo_Maligna_Utero %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Neoplasia maligna do útero/dados_MA_Neo_Maligna_Utero.parquet')
# arrow::write_parquet(dados_MG_Neo_Maligna_Utero %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Neoplasia maligna do útero/dados_MG_Neo_Maligna_Utero.parquet')
# arrow::write_parquet(dados_MS_Neo_Maligna_Utero %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Neoplasia maligna do útero/dados_MS_Neo_Maligna_Utero.parquet')
# arrow::write_parquet(dados_MT_Neo_Maligna_Utero %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Neoplasia maligna do útero/dados_MT_Neo_Maligna_Utero.parquet')
# arrow::write_parquet(dados_PA_Neo_Maligna_Utero %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Neoplasia maligna do útero/dados_PA_Neo_Maligna_Utero.parquet')
# arrow::write_parquet(dados_PB_Neo_Maligna_Utero %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Neoplasia maligna do útero/dados_PB_Neo_Maligna_Utero.parquet')
# arrow::write_parquet(dados_PE_Neo_Maligna_Utero %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Neoplasia maligna do útero/dados_PE_Neo_Maligna_Utero.parquet')
# arrow::write_parquet(dados_PI_Neo_Maligna_Utero %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Neoplasia maligna do útero/dados_PI_Neo_Maligna_Utero.parquet')
# arrow::write_parquet(dados_PR_Neo_Maligna_Utero %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Neoplasia maligna do útero/dados_PR_Neo_Maligna_Utero.parquet')
# arrow::write_parquet(dados_RJ_Neo_Maligna_Utero %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Neoplasia maligna do útero/dados_RJ_Neo_Maligna_Utero.parquet')
# arrow::write_parquet(dados_RN_Neo_Maligna_Utero %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Neoplasia maligna do útero/dados_RN_Neo_Maligna_Utero.parquet')
# arrow::write_parquet(dados_RO_Neo_Maligna_Utero %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Neoplasia maligna do útero/dados_RO_Neo_Maligna_Utero.parquet')
# arrow::write_parquet(dados_RR_Neo_Maligna_Utero %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Neoplasia maligna do útero/dados_RR_Neo_Maligna_Utero.parquet')
# arrow::write_parquet(dados_RS_Neo_Maligna_Utero %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Neoplasia maligna do útero/dados_RS_Neo_Maligna_Utero.parquet')
# arrow::write_parquet(dados_SC_Neo_Maligna_Utero %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Neoplasia maligna do útero/dados_SC_Neo_Maligna_Utero.parquet')
# arrow::write_parquet(dados_SE_Neo_Maligna_Utero %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Neoplasia maligna do útero/dados_SE_Neo_Maligna_Utero.parquet')
# arrow::write_parquet(dados_SP_Neo_Maligna_Utero %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Neoplasia maligna do útero/dados_SP_Neo_Maligna_Utero.parquet')
# arrow::write_parquet(dados_TO_Neo_Maligna_Utero %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Neoplasia maligna do útero/dados_TO_Neo_Maligna_Utero.parquet')

dados_Neo_Maligna_Utero = 
  rbind(dados_AC_Neo_Maligna_Utero,dados_AL_Neo_Maligna_Utero,dados_AM_Neo_Maligna_Utero,dados_AP_Neo_Maligna_Utero,
        dados_BA_Neo_Maligna_Utero,dados_CE_Neo_Maligna_Utero,dados_DF_Neo_Maligna_Utero,dados_ES_Neo_Maligna_Utero,
        dados_GO_Neo_Maligna_Utero,dados_MA_Neo_Maligna_Utero,dados_MG_Neo_Maligna_Utero,dados_MS_Neo_Maligna_Utero,
        dados_MT_Neo_Maligna_Utero,dados_PA_Neo_Maligna_Utero,dados_PB_Neo_Maligna_Utero,dados_PE_Neo_Maligna_Utero,
        dados_PI_Neo_Maligna_Utero,dados_PR_Neo_Maligna_Utero,dados_RJ_Neo_Maligna_Utero,dados_RN_Neo_Maligna_Utero,
        dados_RO_Neo_Maligna_Utero,dados_RR_Neo_Maligna_Utero,dados_RS_Neo_Maligna_Utero,dados_SC_Neo_Maligna_Utero,
        dados_SE_Neo_Maligna_Utero,dados_SP_Neo_Maligna_Utero,dados_TO_Neo_Maligna_Utero)
# arrow::write_parquet(dados_Neo_Maligna_Utero %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Neoplasia maligna do útero/dados_Neoplasia_maligna_do_útero.parquet")

####============
#### Pneumonias
####============
dados_AC_Pneumonias = FiltraDadosCID(dados_AC, c("J13","J14","J15","J17","J18"))
dados_AL_Pneumonias = FiltraDadosCID(dados_AL, c("J13","J14","J15","J17","J18"))
dados_AM_Pneumonias = FiltraDadosCID(dados_AM, c("J13","J14","J15","J17","J18"))
dados_AP_Pneumonias = FiltraDadosCID(dados_AP, c("J13","J14","J15","J17","J18"))
dados_BA_Pneumonias = FiltraDadosCID(dados_BA, c("J13","J14","J15","J17","J18"))
dados_CE_Pneumonias = FiltraDadosCID(dados_CE, c("J13","J14","J15","J17","J18"))
dados_DF_Pneumonias = FiltraDadosCID(dados_DF, c("J13","J14","J15","J17","J18"))
dados_ES_Pneumonias = FiltraDadosCID(dados_ES, c("J13","J14","J15","J17","J18"))
dados_GO_Pneumonias = FiltraDadosCID(dados_GO, c("J13","J14","J15","J17","J18"))
dados_MA_Pneumonias = FiltraDadosCID(dados_MA, c("J13","J14","J15","J17","J18"))
dados_MG_Pneumonias = FiltraDadosCID(dados_MG, c("J13","J14","J15","J17","J18"))
dados_MS_Pneumonias = FiltraDadosCID(dados_MS, c("J13","J14","J15","J17","J18"))
dados_MT_Pneumonias = FiltraDadosCID(dados_MT, c("J13","J14","J15","J17","J18"))
dados_PA_Pneumonias = FiltraDadosCID(dados_PA, c("J13","J14","J15","J17","J18"))
dados_PB_Pneumonias = FiltraDadosCID(dados_PB, c("J13","J14","J15","J17","J18"))
dados_PE_Pneumonias = FiltraDadosCID(dados_PE, c("J13","J14","J15","J17","J18"))
dados_PI_Pneumonias = FiltraDadosCID(dados_PI, c("J13","J14","J15","J17","J18"))
dados_PR_Pneumonias = FiltraDadosCID(dados_PR, c("J13","J14","J15","J17","J18"))
dados_RJ_Pneumonias = FiltraDadosCID(dados_RJ, c("J13","J14","J15","J17","J18"))
dados_RN_Pneumonias = FiltraDadosCID(dados_RN, c("J13","J14","J15","J17","J18"))
dados_RO_Pneumonias = FiltraDadosCID(dados_RO, c("J13","J14","J15","J17","J18"))
dados_RR_Pneumonias = FiltraDadosCID(dados_RR, c("J13","J14","J15","J17","J18"))
dados_RS_Pneumonias = FiltraDadosCID(dados_RS, c("J13","J14","J15","J17","J18"))
dados_SC_Pneumonias = FiltraDadosCID(dados_SC, c("J13","J14","J15","J17","J18"))
dados_SE_Pneumonias = FiltraDadosCID(dados_SE, c("J13","J14","J15","J17","J18"))
dados_SP_Pneumonias = FiltraDadosCID(dados_SP, c("J13","J14","J15","J17","J18"))
dados_TO_Pneumonias = FiltraDadosCID(dados_TO, c("J13","J14","J15","J17","J18"))

# arrow::write_parquet(dados_AC_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_AC_Pneumonias.parquet')
# arrow::write_parquet(dados_AL_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_AL_Pneumonias.parquet')
# arrow::write_parquet(dados_AM_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_AM_Pneumonias.parquet')
# arrow::write_parquet(dados_AP_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_AP_Pneumonias.parquet')
# arrow::write_parquet(dados_BA_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_BA_Pneumonias.parquet')
# arrow::write_parquet(dados_CE_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_CE_Pneumonias.parquet')
# arrow::write_parquet(dados_DF_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_DF_Pneumonias.parquet')
# arrow::write_parquet(dados_ES_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_ES_Pneumonias.parquet')
# arrow::write_parquet(dados_GO_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_GO_Pneumonias.parquet')
# arrow::write_parquet(dados_MA_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_MA_Pneumonias.parquet')
# arrow::write_parquet(dados_MG_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_MG_Pneumonias.parquet')
# arrow::write_parquet(dados_MS_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_MS_Pneumonias.parquet')
# arrow::write_parquet(dados_MT_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_MT_Pneumonias.parquet')
# arrow::write_parquet(dados_PA_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_PA_Pneumonias.parquet')
# arrow::write_parquet(dados_PB_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_PB_Pneumonias.parquet')
# arrow::write_parquet(dados_PE_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_PE_Pneumonias.parquet')
# arrow::write_parquet(dados_PI_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_PI_Pneumonias.parquet')
# arrow::write_parquet(dados_PR_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_PR_Pneumonias.parquet')
# arrow::write_parquet(dados_RJ_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_RJ_Pneumonias.parquet')
# arrow::write_parquet(dados_RN_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_RN_Pneumonias.parquet')
# arrow::write_parquet(dados_RO_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_RO_Pneumonias.parquet')
# arrow::write_parquet(dados_RR_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_RR_Pneumonias.parquet')
# arrow::write_parquet(dados_RS_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_RS_Pneumonias.parquet')
# arrow::write_parquet(dados_SC_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_SC_Pneumonias.parquet')
# arrow::write_parquet(dados_SE_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_SE_Pneumonias.parquet')
# arrow::write_parquet(dados_SP_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_SP_Pneumonias.parquet')
# arrow::write_parquet(dados_TO_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_TO_Pneumonias.parquet')

dados_Pneumonias = rbind(dados_AC_Pneumonias,dados_AL_Pneumonias,dados_AM_Pneumonias,dados_AP_Pneumonias,
                         dados_BA_Pneumonias,dados_CE_Pneumonias,dados_DF_Pneumonias,dados_ES_Pneumonias,
                         dados_GO_Pneumonias,dados_MA_Pneumonias,dados_MG_Pneumonias,dados_MS_Pneumonias,
                         dados_MT_Pneumonias,dados_PA_Pneumonias,dados_PB_Pneumonias,dados_PE_Pneumonias,
                         dados_PI_Pneumonias,dados_PR_Pneumonias,dados_RJ_Pneumonias,dados_RN_Pneumonias,
                         dados_RO_Pneumonias,dados_RR_Pneumonias,dados_RS_Pneumonias,dados_SC_Pneumonias,
                         dados_SE_Pneumonias,dados_SP_Pneumonias,dados_TO_Pneumonias)
# arrow::write_parquet(dados_Pneumonias %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_Pneumonias.parquet")

####========================================================
#### Úlcera gastrointestinal com hemorragia e/ou perfuração
####========================================================
dados_AC_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_AC, c("K25","K26","K27","K28"))
dados_AL_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_AL, c("K25","K26","K27","K28"))
dados_AM_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_AM, c("K25","K26","K27","K28"))
dados_AP_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_AP, c("K25","K26","K27","K28"))
dados_BA_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_BA, c("K25","K26","K27","K28"))
dados_CE_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_CE, c("K25","K26","K27","K28"))
dados_DF_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_DF, c("K25","K26","K27","K28"))
dados_ES_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_ES, c("K25","K26","K27","K28"))
dados_GO_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_GO, c("K25","K26","K27","K28"))
dados_MA_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_MA, c("K25","K26","K27","K28"))
dados_MG_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_MG, c("K25","K26","K27","K28"))
dados_MS_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_MS, c("K25","K26","K27","K28"))
dados_MT_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_MT, c("K25","K26","K27","K28"))
dados_PA_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_PA, c("K25","K26","K27","K28"))
dados_PB_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_PB, c("K25","K26","K27","K28"))
dados_PE_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_PE, c("K25","K26","K27","K28"))
dados_PI_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_PI, c("K25","K26","K27","K28"))
dados_PR_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_PR, c("K25","K26","K27","K28"))
dados_RJ_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_RJ, c("K25","K26","K27","K28"))
dados_RN_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_RN, c("K25","K26","K27","K28"))
dados_RO_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_RO, c("K25","K26","K27","K28"))
dados_RR_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_RR, c("K25","K26","K27","K28"))
dados_RS_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_RS, c("K25","K26","K27","K28"))
dados_SC_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_SC, c("K25","K26","K27","K28"))
dados_SE_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_SE, c("K25","K26","K27","K28"))
dados_SP_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_SP, c("K25","K26","K27","K28"))
dados_TO_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_TO, c("K25","K26","K27","K28"))

# arrow::write_parquet(dados_AC_Ulc_Gastro_Hem_Perf %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Úlcera gastrointestinal com hemorragia eou perfuração/dados_AC_Ulc_Gastro_Hem_Perf.parquet')
# arrow::write_parquet(dados_AL_Ulc_Gastro_Hem_Perf %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Úlcera gastrointestinal com hemorragia eou perfuração/dados_AL_Ulc_Gastro_Hem_Perf.parquet')
# arrow::write_parquet(dados_AM_Ulc_Gastro_Hem_Perf %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Úlcera gastrointestinal com hemorragia eou perfuração/dados_AM_Ulc_Gastro_Hem_Perf.parquet')
# arrow::write_parquet(dados_AP_Ulc_Gastro_Hem_Perf %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Úlcera gastrointestinal com hemorragia eou perfuração/dados_AP_Ulc_Gastro_Hem_Perf.parquet')
# arrow::write_parquet(dados_BA_Ulc_Gastro_Hem_Perf %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Úlcera gastrointestinal com hemorragia eou perfuração/dados_BA_Ulc_Gastro_Hem_Perf.parquet')
# arrow::write_parquet(dados_CE_Ulc_Gastro_Hem_Perf %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Úlcera gastrointestinal com hemorragia eou perfuração/dados_CE_Ulc_Gastro_Hem_Perf.parquet')
# arrow::write_parquet(dados_DF_Ulc_Gastro_Hem_Perf %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Úlcera gastrointestinal com hemorragia eou perfuração/dados_DF_Ulc_Gastro_Hem_Perf.parquet')
# arrow::write_parquet(dados_ES_Ulc_Gastro_Hem_Perf %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Úlcera gastrointestinal com hemorragia eou perfuração/dados_ES_Ulc_Gastro_Hem_Perf.parquet')
# arrow::write_parquet(dados_GO_Ulc_Gastro_Hem_Perf %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Úlcera gastrointestinal com hemorragia eou perfuração/dados_GO_Ulc_Gastro_Hem_Perf.parquet')
# arrow::write_parquet(dados_MA_Ulc_Gastro_Hem_Perf %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Úlcera gastrointestinal com hemorragia eou perfuração/dados_MA_Ulc_Gastro_Hem_Perf.parquet')
# arrow::write_parquet(dados_MG_Ulc_Gastro_Hem_Perf %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Úlcera gastrointestinal com hemorragia eou perfuração/dados_MG_Ulc_Gastro_Hem_Perf.parquet')
# arrow::write_parquet(dados_MS_Ulc_Gastro_Hem_Perf %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Úlcera gastrointestinal com hemorragia eou perfuração/dados_MS_Ulc_Gastro_Hem_Perf.parquet')
# arrow::write_parquet(dados_MT_Ulc_Gastro_Hem_Perf %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Úlcera gastrointestinal com hemorragia eou perfuração/dados_MT_Ulc_Gastro_Hem_Perf.parquet')
# arrow::write_parquet(dados_PA_Ulc_Gastro_Hem_Perf %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Úlcera gastrointestinal com hemorragia eou perfuração/dados_PA_Ulc_Gastro_Hem_Perf.parquet')
# arrow::write_parquet(dados_PB_Ulc_Gastro_Hem_Perf %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Úlcera gastrointestinal com hemorragia eou perfuração/dados_PB_Ulc_Gastro_Hem_Perf.parquet')
# arrow::write_parquet(dados_PE_Ulc_Gastro_Hem_Perf %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Úlcera gastrointestinal com hemorragia eou perfuração/dados_PE_Ulc_Gastro_Hem_Perf.parquet')
# arrow::write_parquet(dados_PI_Ulc_Gastro_Hem_Perf %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Úlcera gastrointestinal com hemorragia eou perfuração/dados_PI_Ulc_Gastro_Hem_Perf.parquet')
# arrow::write_parquet(dados_PR_Ulc_Gastro_Hem_Perf %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Úlcera gastrointestinal com hemorragia eou perfuração/dados_PR_Ulc_Gastro_Hem_Perf.parquet')
# arrow::write_parquet(dados_RJ_Ulc_Gastro_Hem_Perf %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Úlcera gastrointestinal com hemorragia eou perfuração/dados_RJ_Ulc_Gastro_Hem_Perf.parquet')
# arrow::write_parquet(dados_RN_Ulc_Gastro_Hem_Perf %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Úlcera gastrointestinal com hemorragia eou perfuração/dados_RN_Ulc_Gastro_Hem_Perf.parquet')
# arrow::write_parquet(dados_RO_Ulc_Gastro_Hem_Perf %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Úlcera gastrointestinal com hemorragia eou perfuração/dados_RO_Ulc_Gastro_Hem_Perf.parquet')
# arrow::write_parquet(dados_RR_Ulc_Gastro_Hem_Perf %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Úlcera gastrointestinal com hemorragia eou perfuração/dados_RR_Ulc_Gastro_Hem_Perf.parquet')
# arrow::write_parquet(dados_RS_Ulc_Gastro_Hem_Perf %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Úlcera gastrointestinal com hemorragia eou perfuração/dados_RS_Ulc_Gastro_Hem_Perf.parquet')
# arrow::write_parquet(dados_SC_Ulc_Gastro_Hem_Perf %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Úlcera gastrointestinal com hemorragia eou perfuração/dados_SC_Ulc_Gastro_Hem_Perf.parquet')
# arrow::write_parquet(dados_SE_Ulc_Gastro_Hem_Perf %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Úlcera gastrointestinal com hemorragia eou perfuração/dados_SE_Ulc_Gastro_Hem_Perf.parquet')
# arrow::write_parquet(dados_SP_Ulc_Gastro_Hem_Perf %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Úlcera gastrointestinal com hemorragia eou perfuração/dados_SP_Ulc_Gastro_Hem_Perf.parquet')
# arrow::write_parquet(dados_TO_Ulc_Gastro_Hem_Perf %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Úlcera gastrointestinal com hemorragia eou perfuração/dados_TO_Ulc_Gastro_Hem_Perf.parquet')

dados_Ulc_Gastro_Hem_Perf = 
  rbind(dados_AC_Ulc_Gastro_Hem_Perf,dados_AL_Ulc_Gastro_Hem_Perf,dados_AM_Ulc_Gastro_Hem_Perf,dados_AP_Ulc_Gastro_Hem_Perf,
        dados_BA_Ulc_Gastro_Hem_Perf,dados_CE_Ulc_Gastro_Hem_Perf,dados_DF_Ulc_Gastro_Hem_Perf,dados_ES_Ulc_Gastro_Hem_Perf,
        dados_GO_Ulc_Gastro_Hem_Perf,dados_MA_Ulc_Gastro_Hem_Perf,dados_MG_Ulc_Gastro_Hem_Perf,dados_MS_Ulc_Gastro_Hem_Perf,
        dados_MT_Ulc_Gastro_Hem_Perf,dados_PA_Ulc_Gastro_Hem_Perf,dados_PB_Ulc_Gastro_Hem_Perf,dados_PE_Ulc_Gastro_Hem_Perf,
        dados_PI_Ulc_Gastro_Hem_Perf,dados_PR_Ulc_Gastro_Hem_Perf,dados_RJ_Ulc_Gastro_Hem_Perf,dados_RN_Ulc_Gastro_Hem_Perf,
        dados_RO_Ulc_Gastro_Hem_Perf,dados_RR_Ulc_Gastro_Hem_Perf,dados_RS_Ulc_Gastro_Hem_Perf,dados_SC_Ulc_Gastro_Hem_Perf,
        dados_SE_Ulc_Gastro_Hem_Perf,dados_SP_Ulc_Gastro_Hem_Perf,dados_TO_Ulc_Gastro_Hem_Perf)
# arrow::write_parquet(dados_Ulc_Gastro_Hem_Perf %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Úlcera gastrointestinal com hemorragia eou perfuração/dados_Úlcera_gastrointestinal_com_hemorragia_eou_perfuração.parquet")
