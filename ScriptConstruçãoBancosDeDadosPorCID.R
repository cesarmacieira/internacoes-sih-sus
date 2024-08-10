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
dados_SP = arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/SP/dados_empilhados_SP.parquet')
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

####===========
#### Pneumonia
####===========
dados_AC_Pneumonia = FiltraDadosCID(dados_AC, c("J13", "J14", "J15", "J17", "J18"))
dados_AL_Pneumonia = FiltraDadosCID(dados_AL, c("J13", "J14", "J15", "J17", "J18"))
dados_AM_Pneumonia = FiltraDadosCID(dados_AM, c("J13", "J14", "J15", "J17", "J18"))
dados_AP_Pneumonia = FiltraDadosCID(dados_AP, c("J13", "J14", "J15", "J17", "J18"))
dados_BA_Pneumonia = FiltraDadosCID(dados_BA, c("J13", "J14", "J15", "J17", "J18"))
dados_CE_Pneumonia = FiltraDadosCID(dados_CE, c("J13", "J14", "J15", "J17", "J18"))
dados_DF_Pneumonia = FiltraDadosCID(dados_DF, c("J13", "J14", "J15", "J17", "J18"))
dados_ES_Pneumonia = FiltraDadosCID(dados_ES, c("J13", "J14", "J15", "J17", "J18"))
dados_GO_Pneumonia = FiltraDadosCID(dados_GO, c("J13", "J14", "J15", "J17", "J18"))
dados_MA_Pneumonia = FiltraDadosCID(dados_MA, c("J13", "J14", "J15", "J17", "J18"))
dados_MG_Pneumonia = FiltraDadosCID(dados_MG, c("J13", "J14", "J15", "J17", "J18"))
dados_MS_Pneumonia = FiltraDadosCID(dados_MS, c("J13", "J14", "J15", "J17", "J18"))
dados_MT_Pneumonia = FiltraDadosCID(dados_MT, c("J13", "J14", "J15", "J17", "J18"))
dados_PA_Pneumonia = FiltraDadosCID(dados_PA, c("J13", "J14", "J15", "J17", "J18"))
dados_PB_Pneumonia = FiltraDadosCID(dados_PB, c("J13", "J14", "J15", "J17", "J18"))
dados_PE_Pneumonia = FiltraDadosCID(dados_PE, c("J13", "J14", "J15", "J17", "J18"))
dados_PI_Pneumonia = FiltraDadosCID(dados_PI, c("J13", "J14", "J15", "J17", "J18"))
dados_PR_Pneumonia = FiltraDadosCID(dados_PR, c("J13", "J14", "J15", "J17", "J18"))
dados_RJ_Pneumonia = FiltraDadosCID(dados_RJ, c("J13", "J14", "J15", "J17", "J18"))
dados_RN_Pneumonia = FiltraDadosCID(dados_RN, c("J13", "J14", "J15", "J17", "J18"))
dados_RO_Pneumonia = FiltraDadosCID(dados_RO, c("J13", "J14", "J15", "J17", "J18"))
dados_RR_Pneumonia = FiltraDadosCID(dados_RR, c("J13", "J14", "J15", "J17", "J18"))
dados_RS_Pneumonia = FiltraDadosCID(dados_RS, c("J13", "J14", "J15", "J17", "J18"))
dados_SC_Pneumonia = FiltraDadosCID(dados_SC, c("J13", "J14", "J15", "J17", "J18"))
dados_SE_Pneumonia = FiltraDadosCID(dados_SE, c("J13", "J14", "J15", "J17", "J18"))
dados_SP_Pneumonia = FiltraDadosCID(dados_SP, c("J13", "J14", "J15", "J17", "J18"))
dados_TO_Pneumonia = FiltraDadosCID(dados_TO, c("J13", "J14", "J15", "J17", "J18"))

arrow::write_parquet(dados_AC_Pneumonia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Pneumonia/dados_AC_Pneumonia.parquet')
arrow::write_parquet(dados_AL_Pneumonia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Pneumonia/dados_AL_Pneumonia.parquet')
arrow::write_parquet(dados_AM_Pneumonia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Pneumonia/dados_AM_Pneumonia.parquet')
arrow::write_parquet(dados_AP_Pneumonia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Pneumonia/dados_AP_Pneumonia.parquet')
arrow::write_parquet(dados_BA_Pneumonia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Pneumonia/dados_BA_Pneumonia.parquet')
arrow::write_parquet(dados_CE_Pneumonia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Pneumonia/dados_CE_Pneumonia.parquet')
arrow::write_parquet(dados_DF_Pneumonia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Pneumonia/dados_DF_Pneumonia.parquet')
arrow::write_parquet(dados_ES_Pneumonia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Pneumonia/dados_ES_Pneumonia.parquet')
arrow::write_parquet(dados_GO_Pneumonia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Pneumonia/dados_GO_Pneumonia.parquet')
arrow::write_parquet(dados_MA_Pneumonia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Pneumonia/dados_MA_Pneumonia.parquet')
arrow::write_parquet(dados_MG_Pneumonia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Pneumonia/dados_MG_Pneumonia.parquet')
arrow::write_parquet(dados_MS_Pneumonia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Pneumonia/dados_MS_Pneumonia.parquet')
arrow::write_parquet(dados_MT_Pneumonia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Pneumonia/dados_MT_Pneumonia.parquet')
arrow::write_parquet(dados_PA_Pneumonia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Pneumonia/dados_PA_Pneumonia.parquet')
arrow::write_parquet(dados_PB_Pneumonia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Pneumonia/dados_PB_Pneumonia.parquet')
arrow::write_parquet(dados_PE_Pneumonia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Pneumonia/dados_PE_Pneumonia.parquet')
arrow::write_parquet(dados_PI_Pneumonia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Pneumonia/dados_PI_Pneumonia.parquet')
arrow::write_parquet(dados_PR_Pneumonia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Pneumonia/dados_PR_Pneumonia.parquet')
arrow::write_parquet(dados_RJ_Pneumonia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Pneumonia/dados_RJ_Pneumonia.parquet')
arrow::write_parquet(dados_RN_Pneumonia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Pneumonia/dados_RN_Pneumonia.parquet')
arrow::write_parquet(dados_RO_Pneumonia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Pneumonia/dados_RO_Pneumonia.parquet')
arrow::write_parquet(dados_RR_Pneumonia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Pneumonia/dados_RR_Pneumonia.parquet')
arrow::write_parquet(dados_RS_Pneumonia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Pneumonia/dados_RS_Pneumonia.parquet')
arrow::write_parquet(dados_SC_Pneumonia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Pneumonia/dados_SC_Pneumonia.parquet')
arrow::write_parquet(dados_SE_Pneumonia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Pneumonia/dados_SE_Pneumonia.parquet')
arrow::write_parquet(dados_SP_Pneumonia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Pneumonia/dados_SP_Pneumonia.parquet')
arrow::write_parquet(dados_TO_Pneumonia %>% as.data.frame(),'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Pneumonia/dados_TO_Pneumonia.parquet')

dados_Pneumonia = rbind(dados_AC_Pneumonia,dados_AL_Pneumonia,dados_AM_Pneumonia,dados_AP_Pneumonia,
                        dados_BA_Pneumonia,dados_CE_Pneumonia,dados_DF_Pneumonia,dados_ES,
                        dados_GO_Pneumonia,dados_MA_Pneumonia,dados_MG_Pneumonia,dados_MS_Pneumonia,
                        dados_MT_Pneumonia,dados_PA_Pneumonia,dados_PB_Pneumonia,dados_PE,
                        dados_PI_Pneumonia,dados_PR_Pneumonia,dados_RJ_Pneumonia,dados_RN_Pneumonia,
                        dados_RO_Pneumonia,dados_RR_Pneumonia,dados_RS_Pneumonia,dados_SC,
                        dados_SE_Pneumonia,dados_SP_Pneumonia,dados_TO)
arrow::write_parquet(dados_Pneumonia %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/dados_Pneumonia.parquet")