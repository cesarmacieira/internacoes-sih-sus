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
  colnames(tabela) = c("Freq. Absoluta (N)", "Freq. Relativa (%)")
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
  names(t1) = c("N válidos", "Média", "Variância", "D.P.", "E.P.", "Mínimo", "1ºQ", "2ºQ", "3ºQ", "Máximo")
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

arrow::write_parquet(dados_AC_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_AC_Anemia.parquet')
arrow::write_parquet(dados_AL_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_AL_Anemia.parquet')
arrow::write_parquet(dados_AM_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_AM_Anemia.parquet')
arrow::write_parquet(dados_AP_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_AP_Anemia.parquet')
arrow::write_parquet(dados_BA_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_BA_Anemia.parquet')
arrow::write_parquet(dados_CE_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_CE_Anemia.parquet')
arrow::write_parquet(dados_DF_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_DF_Anemia.parquet')
arrow::write_parquet(dados_ES_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_ES_Anemia.parquet')
arrow::write_parquet(dados_GO_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_GO_Anemia.parquet')
arrow::write_parquet(dados_MA_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_MA_Anemia.parquet')
arrow::write_parquet(dados_MG_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_MG_Anemia.parquet')
arrow::write_parquet(dados_MS_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_MS_Anemia.parquet')
arrow::write_parquet(dados_MT_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_MT_Anemia.parquet')
arrow::write_parquet(dados_PA_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_PA_Anemia.parquet')
arrow::write_parquet(dados_PB_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_PB_Anemia.parquet')
arrow::write_parquet(dados_PE_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_PE_Anemia.parquet')
arrow::write_parquet(dados_PI_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_PI_Anemia.parquet')
arrow::write_parquet(dados_PR_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_PR_Anemia.parquet')
arrow::write_parquet(dados_RJ_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_RJ_Anemia.parquet')
arrow::write_parquet(dados_RN_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_RN_Anemia.parquet')
arrow::write_parquet(dados_RO_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_RO_Anemia.parquet')
arrow::write_parquet(dados_RR_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_RR_Anemia.parquet')
arrow::write_parquet(dados_RS_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_RS_Anemia.parquet')
arrow::write_parquet(dados_SC_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_SC_Anemia.parquet')
arrow::write_parquet(dados_SE_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_SE_Anemia.parquet')
arrow::write_parquet(dados_SP_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_SP_Anemia.parquet')
arrow::write_parquet(dados_TO_Anemia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_TO_Anemia.parquet')

dados_Anemia = rbind(dados_AC_Anemia,dados_AL_Anemia,dados_AM_Anemia,dados_AP_Anemia,
                     dados_BA_Anemia,dados_CE_Anemia,dados_DF_Anemia,dados_ES,
                     dados_GO_Anemia,dados_MA_Anemia,dados_MG_Anemia,dados_MS_Anemia,
                     dados_MT_Anemia,dados_PA_Anemia,dados_PB_Anemia,dados_PE,
                     dados_PI_Anemia,dados_PR_Anemia,dados_RJ_Anemia,dados_RN_Anemia,
                     dados_RO_Anemia,dados_RR_Anemia,dados_RS_Anemia,dados_SC,
                     dados_SE_Anemia,dados_SP_Anemia,dados_TO)
arrow::write_parquet(dados_Anemia %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_Anemia.parquet")

####=================================
#### Anemia por deficiência de ferro
####=================================
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

arrow::write_parquet(dados_AC_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_AC_Angina.parquet')
arrow::write_parquet(dados_AL_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_AL_Angina.parquet')
arrow::write_parquet(dados_AM_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_AM_Angina.parquet')
arrow::write_parquet(dados_AP_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_AP_Angina.parquet')
arrow::write_parquet(dados_BA_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_BA_Angina.parquet')
arrow::write_parquet(dados_CE_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_CE_Angina.parquet')
arrow::write_parquet(dados_DF_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_DF_Angina.parquet')
arrow::write_parquet(dados_ES_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_ES_Angina.parquet')
arrow::write_parquet(dados_GO_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_GO_Angina.parquet')
arrow::write_parquet(dados_MA_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_MA_Angina.parquet')
arrow::write_parquet(dados_MG_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_MG_Angina.parquet')
arrow::write_parquet(dados_MS_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_MS_Angina.parquet')
arrow::write_parquet(dados_MT_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_MT_Angina.parquet')
arrow::write_parquet(dados_PA_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_PA_Angina.parquet')
arrow::write_parquet(dados_PB_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_PB_Angina.parquet')
arrow::write_parquet(dados_PE_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_PE_Angina.parquet')
arrow::write_parquet(dados_PI_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_PI_Angina.parquet')
arrow::write_parquet(dados_PR_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_PR_Angina.parquet')
arrow::write_parquet(dados_RJ_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_RJ_Angina.parquet')
arrow::write_parquet(dados_RN_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_RN_Angina.parquet')
arrow::write_parquet(dados_RO_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_RO_Angina.parquet')
arrow::write_parquet(dados_RR_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_RR_Angina.parquet')
arrow::write_parquet(dados_RS_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_RS_Angina.parquet')
arrow::write_parquet(dados_SC_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_SC_Angina.parquet')
arrow::write_parquet(dados_SE_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_SE_Angina.parquet')
arrow::write_parquet(dados_SP_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_SP_Angina.parquet')
arrow::write_parquet(dados_TO_Angina %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_TO_Angina.parquet')

dados_Angina = rbind(dados_AC_Angina,dados_AL_Angina,dados_AM_Angina,dados_AP_Angina,
                     dados_BA_Angina,dados_CE_Angina,dados_DF_Angina,dados_ES,
                     dados_GO_Angina,dados_MA_Angina,dados_MG_Angina,dados_MS_Angina,
                     dados_MT_Angina,dados_PA_Angina,dados_PB_Angina,dados_PE,
                     dados_PI_Angina,dados_PR_Angina,dados_RJ_Angina,dados_RN_Angina,
                     dados_RO_Angina,dados_RR_Angina,dados_RS_Angina,dados_SC,
                     dados_SE_Angina,dados_SP_Angina,dados_TO)
arrow::write_parquet(dados_Angina %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_Angina.parquet")

####============
#### Pneumonias
####============
dados_AC_Pneumonias = FiltraDadosCID(dados_AC, c("J13", "J14", "J15", "J17", "J18"))
dados_AL_Pneumonias = FiltraDadosCID(dados_AL, c("J13", "J14", "J15", "J17", "J18"))
dados_AM_Pneumonias = FiltraDadosCID(dados_AM, c("J13", "J14", "J15", "J17", "J18"))
dados_AP_Pneumonias = FiltraDadosCID(dados_AP, c("J13", "J14", "J15", "J17", "J18"))
dados_BA_Pneumonias = FiltraDadosCID(dados_BA, c("J13", "J14", "J15", "J17", "J18"))
dados_CE_Pneumonias = FiltraDadosCID(dados_CE, c("J13", "J14", "J15", "J17", "J18"))
dados_DF_Pneumonias = FiltraDadosCID(dados_DF, c("J13", "J14", "J15", "J17", "J18"))
dados_ES_Pneumonias = FiltraDadosCID(dados_ES, c("J13", "J14", "J15", "J17", "J18"))
dados_GO_Pneumonias = FiltraDadosCID(dados_GO, c("J13", "J14", "J15", "J17", "J18"))
dados_MA_Pneumonias = FiltraDadosCID(dados_MA, c("J13", "J14", "J15", "J17", "J18"))
dados_MG_Pneumonias = FiltraDadosCID(dados_MG, c("J13", "J14", "J15", "J17", "J18"))
dados_MS_Pneumonias = FiltraDadosCID(dados_MS, c("J13", "J14", "J15", "J17", "J18"))
dados_MT_Pneumonias = FiltraDadosCID(dados_MT, c("J13", "J14", "J15", "J17", "J18"))
dados_PA_Pneumonias = FiltraDadosCID(dados_PA, c("J13", "J14", "J15", "J17", "J18"))
dados_PB_Pneumonias = FiltraDadosCID(dados_PB, c("J13", "J14", "J15", "J17", "J18"))
dados_PE_Pneumonias = FiltraDadosCID(dados_PE, c("J13", "J14", "J15", "J17", "J18"))
dados_PI_Pneumonias = FiltraDadosCID(dados_PI, c("J13", "J14", "J15", "J17", "J18"))
dados_PR_Pneumonias = FiltraDadosCID(dados_PR, c("J13", "J14", "J15", "J17", "J18"))
dados_RJ_Pneumonias = FiltraDadosCID(dados_RJ, c("J13", "J14", "J15", "J17", "J18"))
dados_RN_Pneumonias = FiltraDadosCID(dados_RN, c("J13", "J14", "J15", "J17", "J18"))
dados_RO_Pneumonias = FiltraDadosCID(dados_RO, c("J13", "J14", "J15", "J17", "J18"))
dados_RR_Pneumonias = FiltraDadosCID(dados_RR, c("J13", "J14", "J15", "J17", "J18"))
dados_RS_Pneumonias = FiltraDadosCID(dados_RS, c("J13", "J14", "J15", "J17", "J18"))
dados_SC_Pneumonias = FiltraDadosCID(dados_SC, c("J13", "J14", "J15", "J17", "J18"))
dados_SE_Pneumonias = FiltraDadosCID(dados_SE, c("J13", "J14", "J15", "J17", "J18"))
dados_SP_Pneumonias = FiltraDadosCID(dados_SP, c("J13", "J14", "J15", "J17", "J18"))
dados_TO_Pneumonias = FiltraDadosCID(dados_TO, c("J13", "J14", "J15", "J17", "J18"))

arrow::write_parquet(dados_AC_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_AC_Pneumonias.parquet')
arrow::write_parquet(dados_AL_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_AL_Pneumonias.parquet')
arrow::write_parquet(dados_AM_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_AM_Pneumonias.parquet')
arrow::write_parquet(dados_AP_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_AP_Pneumonias.parquet')
arrow::write_parquet(dados_BA_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_BA_Pneumonias.parquet')
arrow::write_parquet(dados_CE_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_CE_Pneumonias.parquet')
arrow::write_parquet(dados_DF_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_DF_Pneumonias.parquet')
arrow::write_parquet(dados_ES_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_ES_Pneumonias.parquet')
arrow::write_parquet(dados_GO_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_GO_Pneumonias.parquet')
arrow::write_parquet(dados_MA_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_MA_Pneumonias.parquet')
arrow::write_parquet(dados_MG_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_MG_Pneumonias.parquet')
arrow::write_parquet(dados_MS_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_MS_Pneumonias.parquet')
arrow::write_parquet(dados_MT_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_MT_Pneumonias.parquet')
arrow::write_parquet(dados_PA_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_PA_Pneumonias.parquet')
arrow::write_parquet(dados_PB_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_PB_Pneumonias.parquet')
arrow::write_parquet(dados_PE_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_PE_Pneumonias.parquet')
arrow::write_parquet(dados_PI_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_PI_Pneumonias.parquet')
arrow::write_parquet(dados_PR_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_PR_Pneumonias.parquet')
arrow::write_parquet(dados_RJ_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_RJ_Pneumonias.parquet')
arrow::write_parquet(dados_RN_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_RN_Pneumonias.parquet')
arrow::write_parquet(dados_RO_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_RO_Pneumonias.parquet')
arrow::write_parquet(dados_RR_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_RR_Pneumonias.parquet')
arrow::write_parquet(dados_RS_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_RS_Pneumonias.parquet')
arrow::write_parquet(dados_SC_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_SC_Pneumonias.parquet')
arrow::write_parquet(dados_SE_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_SE_Pneumonias.parquet')
arrow::write_parquet(dados_SP_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_SP_Pneumonias.parquet')
arrow::write_parquet(dados_TO_Pneumonias %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_TO_Pneumonias.parquet')

dados_Pneumonias = rbind(dados_AC_Pneumonias,dados_AL_Pneumonias,dados_AM_Pneumonias,dados_AP_Pneumonias,
                         dados_BA_Pneumonias,dados_CE_Pneumonias,dados_DF_Pneumonias,dados_ES,
                         dados_GO_Pneumonias,dados_MA_Pneumonias,dados_MG_Pneumonias,dados_MS_Pneumonias,
                         dados_MT_Pneumonias,dados_PA_Pneumonias,dados_PB_Pneumonias,dados_PE,
                         dados_PI_Pneumonias,dados_PR_Pneumonias,dados_RJ_Pneumonias,dados_RN_Pneumonias,
                         dados_RO_Pneumonias,dados_RR_Pneumonias,dados_RS_Pneumonias,dados_SC,
                         dados_SE_Pneumonias,dados_SP_Pneumonias,dados_TO)
arrow::write_parquet(dados_Pneumonias %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_Pneumonias.parquet")
