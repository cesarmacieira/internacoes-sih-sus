####===============================================================================
#### Trabalho Internações SIH SUS - Construção dos bancos de dados por internações
####===============================================================================
####=============================
#### Preparando o R para análise
####=============================
rm(list=ls(all=T))#Limpar ambiente/histórico
tryCatch({setwd("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus")},
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
dados_AC = tryCatch({arrow::read_parquet("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/AC/dados_empilhados_AC.parquet")},
                    error = function(e) { arrow::read_parquet("D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus") })
dados_AL = tryCatch({arrow::read_parquet("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/AL/dados_empilhados_AL.parquet")},
                    error = function(e) { arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/AL/dados_empilhados_AL.parquet') })
dados_AM = tryCatch({arrow::read_parquet("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/AM/dados_empilhados_AM.parquet")},
                    error = function(e) { arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/AM/dados_empilhados_AM.parquet') })
dados_AP = tryCatch({arrow::read_parquet("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/AP/dados_empilhados_AP.parquet")},
                    error = function(e) { arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/AP/dados_empilhados_AP.parquet') })
dados_BA = tryCatch({arrow::read_parquet("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/BA/dados_empilhados_BA.parquet")},
                    error = function(e) { arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/BA/dados_empilhados_BA.parquet') })
dados_CE = tryCatch({arrow::read_parquet("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/CE/dados_empilhados_CE.parquet")},
                    error = function(e) { arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/CE/dados_empilhados_CE.parquet') })
dados_DF = tryCatch({arrow::read_parquet("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/DF/dados_empilhados_DF.parquet")},
                    error = function(e) { arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/DF/dados_empilhados_DF.parquet') })
dados_ES = tryCatch({arrow::read_parquet("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/ES/dados_empilhados_ES.parquet")},
                    error = function(e) { arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/ES/dados_empilhados_ES.parquet') })
dados_GO = tryCatch({arrow::read_parquet("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/GO/dados_empilhados_GO.parquet")},
                    error = function(e) { arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/GO/dados_empilhados_GO.parquet') })
dados_MA = tryCatch({arrow::read_parquet("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/MA/dados_empilhados_MA.parquet")},
                    error = function(e) { arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/MA/dados_empilhados_MA.parquet') })
dados_MG = tryCatch({arrow::read_parquet("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/MG/dados_empilhados_MG.parquet")},
                    error = function(e) { arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/MG/dados_empilhados_MG.parquet') })
dados_MS = tryCatch({arrow::read_parquet("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/MS/dados_empilhados_MS.parquet")},
                    error = function(e) { arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/MS/dados_empilhados_MS.parquet') })
dados_MT = tryCatch({arrow::read_parquet("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/MT/dados_empilhados_MT.parquet")},
                    error = function(e) { arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/MT/dados_empilhados_MT.parquet') })
dados_PA = tryCatch({arrow::read_parquet("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/PA/dados_empilhados_PA.parquet")},
                    error = function(e) { arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/PA/dados_empilhados_PA.parquet') })
dados_PB = tryCatch({arrow::read_parquet("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/PB/dados_empilhados_PB.parquet")},
                    error = function(e) { arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/PB/dados_empilhados_PB.parquet') })
dados_PE = tryCatch({arrow::read_parquet("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/PE/dados_empilhados_PE.parquet")},
                    error = function(e) { arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/PE/dados_empilhados_PE.parquet') })
dados_PI = tryCatch({arrow::read_parquet("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/PI/dados_empilhados_PI.parquet")},
                    error = function(e) { arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/PI/dados_empilhados_PI.parquet') })
dados_PR = tryCatch({arrow::read_parquet("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/PR/dados_empilhados_PR.parquet")},
                    error = function(e) { arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/PR/dados_empilhados_PR.parquet') })
dados_RJ = tryCatch({arrow::read_parquet("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/RJ/dados_empilhados_RJ.parquet")},
                    error = function(e) { arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/RJ/dados_empilhados_RJ.parquet') })
dados_RN = tryCatch({arrow::read_parquet("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/RN/dados_empilhados_RN.parquet")},
                    error = function(e) { arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/RN/dados_empilhados_RN.parquet') })
dados_RO = tryCatch({arrow::read_parquet("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/RO/dados_empilhados_RO.parquet")},
                    error = function(e) { arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/RO/dados_empilhados_RO.parquet') })
dados_RR = tryCatch({arrow::read_parquet("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/RR/dados_empilhados_RR.parquet")},
                    error = function(e) { arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/RR/dados_empilhados_RR.parquet') })
dados_RS = tryCatch({arrow::read_parquet("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/RS/dados_empilhados_RS.parquet")},
                    error = function(e) { arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/RS/dados_empilhados_RS.parquet') })
dados_SC = tryCatch({arrow::read_parquet("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/SC/dados_empilhados_SC.parquet")},
                    error = function(e) { arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/SC/dados_empilhados_SC.parquet') })
dados_SE = tryCatch({arrow::read_parquet("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/SE/dados_empilhados_SE.parquet")},
                    error = function(e) { arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/SE/dados_empilhados_SE.parquet') })
dados_SP1 = tryCatch({arrow::read_parquet("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/SP/dados_empilhados_SP1.parquet")},
                     error = function(e) { arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/SP/dados_empilhados_SP1.parquet') })
dados_SP2 = tryCatch({arrow::read_parquet("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/SP/dados_empilhados_SP2.parquet")},
                     error = function(e) { arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/SP/dados_empilhados_SP2.parquet') })
dados_SP = rbind(dados_SP1,dados_SP2)
dados_TO = tryCatch({arrow::read_parquet("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/TO/dados_empilhados_TO.parquet")},
                    error = function(e) { arrow::read_parquet('D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/TO/dados_empilhados_TO.parquet') })

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
#60 <= idade <= 80 anos
#80 < idade

FiltraDadosCID = function(dados, diag_list){
  dados_agg = dados %>%  
    mutate(DIAG_ABREV = str_sub(DIAG_PRINC, 1, 3),
           IDADE_ANOS = case_when(COD_IDADE == "1" ~ IDADE / 24 / 365.25, COD_IDADE == "2" ~ IDADE / 365.25,
                                  COD_IDADE == "3" ~ IDADE / 12, COD_IDADE == "4" ~ as.numeric(IDADE),
                                  COD_IDADE == "5" ~ IDADE + 100),
           FAIXA_ETARIA = case_when(IDADE_ANOS < 15 ~ '0 <= idade < 15 anos',
                                    IDADE_ANOS >= 15 & IDADE_ANOS < 60 ~ '15 <= idade < 60 anos',
                                    IDADE_ANOS >= 60 & IDADE_ANOS <= 80 ~ '60 <= idade <= 80 anos',
                                    IDADE_ANOS > 80 ~ '80 anos < idade')) %>% 
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

estados = c("AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT","PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO")

for (uf in estados) {
  tryCatch({arrow::write_parquet(
    get(paste0("dados_", uf, "_Anemia")) %>% as.data.frame(),
    paste0("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Anemia/dados_", uf, "_Anemia.parquet"))},
    error = function(e) { arrow::write_parquet(
      get(paste0("dados_", uf, "_Anemia")) %>% as.data.frame(),
      paste0("D:/NESCON/internacoes-sih-sus/Dados por CID/Anemia/dados_", uf, "_Anemia.parquet")) })
}

dados_Anemia = rbind(dados_AC_Anemia,dados_AL_Anemia,dados_AM_Anemia,dados_AP_Anemia,
                     dados_BA_Anemia,dados_CE_Anemia,dados_DF_Anemia,dados_ES_Anemia,
                     dados_GO_Anemia,dados_MA_Anemia,dados_MG_Anemia,dados_MS_Anemia,
                     dados_MT_Anemia,dados_PA_Anemia,dados_PB_Anemia,dados_PE_Anemia,
                     dados_PI_Anemia,dados_PR_Anemia,dados_RJ_Anemia,dados_RN_Anemia,
                     dados_RO_Anemia,dados_RR_Anemia,dados_RS_Anemia,dados_SC_Anemia,
                     dados_SE_Anemia,dados_SP_Anemia,dados_TO_Anemia)
tryCatch({arrow::write_parquet(dados_Anemia %>% as.data.frame(), "C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Anemia/dados_Anemia.parquet")},
         error = function(e) { arrow::write_parquet(dados_Anemia %>% as.data.frame(), 'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Anemia/dados_Anemia.parquet') })
tryCatch({write.xlsx(dados_Anemia %>% as.data.frame(), "C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Anemia/dados_Anemia.xlsx")},
         error = function(e) { write.xlsx(dados_Anemia %>% as.data.frame(), "D:/NESCON/internacoes-sih-sus/Dados por CID/Anemia/dados_Anemia.xlsx") })

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

estados = c("AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT","PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO")

for (uf in estados) {
  tryCatch({arrow::write_parquet(
    get(paste0("dados_", uf, "_Angina")) %>% as.data.frame(),
    paste0("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Angina/dados_", uf, "_Angina.parquet"))},
    error = function(e) { arrow::write_parquet(
      get(paste0("dados_", uf, "_Angina")) %>% as.data.frame(),
      paste0("D:/NESCON/internacoes-sih-sus/Dados por CID/Angina/dados_", uf, "_Angina.parquet")) })
}

dados_Angina = rbind(dados_AC_Angina,dados_AL_Angina,dados_AM_Angina,dados_AP_Angina,
                     dados_BA_Angina,dados_CE_Angina,dados_DF_Angina,dados_ES_Angina,
                     dados_GO_Angina,dados_MA_Angina,dados_MG_Angina,dados_MS_Angina,
                     dados_MT_Angina,dados_PA_Angina,dados_PB_Angina,dados_PE_Angina,
                     dados_PI_Angina,dados_PR_Angina,dados_RJ_Angina,dados_RN_Angina,
                     dados_RO_Angina,dados_RR_Angina,dados_RS_Angina,dados_SC_Angina,
                     dados_SE_Angina,dados_SP_Angina,dados_TO_Angina)
tryCatch({arrow::write_parquet(dados_Angina %>% as.data.frame(), "C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Angina/dados_Angina.parquet")},
         error = function(e) { arrow::write_parquet(dados_Angina %>% as.data.frame(), 'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Angina/dados_Angina.parquet') })
tryCatch({write.xlsx(dados_Angina %>% as.data.frame(), "C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Angina/dados_Angina.xlsx")},
         error = function(e) { write.xlsx(dados_Angina %>% as.data.frame(), "D:/NESCON/internacoes-sih-sus/Dados por CID/Angina/dados_Angina.xlsx") })

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

estados = c("AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT","PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO")

for (uf in estados) {
  tryCatch({arrow::write_parquet(
    get(paste0("dados_", uf, "_Asma")) %>% as.data.frame(),
    paste0("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Asma/dados_", uf, "_Asma.parquet"))},
    error = function(e) { arrow::write_parquet(
      get(paste0("dados_", uf, "_Asma")) %>% as.data.frame(),
      paste0("D:/NESCON/internacoes-sih-sus/Dados por CID/Asma/dados_", uf, "_Asma.parquet")) })
}

dados_Asma = rbind(dados_AC_Asma,dados_AL_Asma,dados_AM_Asma,dados_AP_Asma,
                   dados_BA_Asma,dados_CE_Asma,dados_DF_Asma,dados_ES_Asma,
                   dados_GO_Asma,dados_MA_Asma,dados_MG_Asma,dados_MS_Asma,
                   dados_MT_Asma,dados_PA_Asma,dados_PB_Asma,dados_PE_Asma,
                   dados_PI_Asma,dados_PR_Asma,dados_RJ_Asma,dados_RN_Asma,
                   dados_RO_Asma,dados_RR_Asma,dados_RS_Asma,dados_SC_Asma,
                   dados_SE_Asma,dados_SP_Asma,dados_TO_Asma)
tryCatch({arrow::write_parquet(dados_Asma %>% as.data.frame(), "C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Asma/dados_Asma.parquet")},
         error = function(e) { arrow::write_parquet(dados_Asma %>% as.data.frame(), 'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Asma/dados_Asma.parquet') })
tryCatch({write.xlsx(dados_Asma %>% as.data.frame(), "C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Asma/dados_Asma.xlsx")},
         error = function(e) { write.xlsx(dados_Asma %>% as.data.frame(), "D:/NESCON/internacoes-sih-sus/Dados por CID/Asma/dados_Asma.xlsx") })

####=====================
#### Condições evitáveis
####=====================
dados_AC_Condicoes_Evitaveis = FiltraDadosCID(dados_AC, c("I00", "I01", "I02", "A51", "A52", "A53", "A15", "A16", "A17", "A15", "A16"))
dados_AL_Condicoes_Evitaveis = FiltraDadosCID(dados_AL, c("I00", "I01", "I02", "A51", "A52", "A53", "A15", "A16", "A17", "A15", "A16"))
dados_AM_Condicoes_Evitaveis = FiltraDadosCID(dados_AM, c("I00", "I01", "I02", "A51", "A52", "A53", "A15", "A16", "A17", "A15", "A16"))
dados_AP_Condicoes_Evitaveis = FiltraDadosCID(dados_AP, c("I00", "I01", "I02", "A51", "A52", "A53", "A15", "A16", "A17", "A15", "A16"))
dados_BA_Condicoes_Evitaveis = FiltraDadosCID(dados_BA, c("I00", "I01", "I02", "A51", "A52", "A53", "A15", "A16", "A17", "A15", "A16"))
dados_CE_Condicoes_Evitaveis = FiltraDadosCID(dados_CE, c("I00", "I01", "I02", "A51", "A52", "A53", "A15", "A16", "A17", "A15", "A16"))
dados_DF_Condicoes_Evitaveis = FiltraDadosCID(dados_DF, c("I00", "I01", "I02", "A51", "A52", "A53", "A15", "A16", "A17", "A15", "A16"))
dados_ES_Condicoes_Evitaveis = FiltraDadosCID(dados_ES, c("I00", "I01", "I02", "A51", "A52", "A53", "A15", "A16", "A17", "A15", "A16"))
dados_GO_Condicoes_Evitaveis = FiltraDadosCID(dados_GO, c("I00", "I01", "I02", "A51", "A52", "A53", "A15", "A16", "A17", "A15", "A16"))
dados_MA_Condicoes_Evitaveis = FiltraDadosCID(dados_MA, c("I00", "I01", "I02", "A51", "A52", "A53", "A15", "A16", "A17", "A15", "A16"))
dados_MG_Condicoes_Evitaveis = FiltraDadosCID(dados_MG, c("I00", "I01", "I02", "A51", "A52", "A53", "A15", "A16", "A17", "A15", "A16"))
dados_MS_Condicoes_Evitaveis = FiltraDadosCID(dados_MS, c("I00", "I01", "I02", "A51", "A52", "A53", "A15", "A16", "A17", "A15", "A16"))
dados_MT_Condicoes_Evitaveis = FiltraDadosCID(dados_MT, c("I00", "I01", "I02", "A51", "A52", "A53", "A15", "A16", "A17", "A15", "A16"))
dados_PA_Condicoes_Evitaveis = FiltraDadosCID(dados_PA, c("I00", "I01", "I02", "A51", "A52", "A53", "A15", "A16", "A17", "A15", "A16"))
dados_PB_Condicoes_Evitaveis = FiltraDadosCID(dados_PB, c("I00", "I01", "I02", "A51", "A52", "A53", "A15", "A16", "A17", "A15", "A16"))
dados_PE_Condicoes_Evitaveis = FiltraDadosCID(dados_PE, c("I00", "I01", "I02", "A51", "A52", "A53", "A15", "A16", "A17", "A15", "A16"))
dados_PI_Condicoes_Evitaveis = FiltraDadosCID(dados_PI, c("I00", "I01", "I02", "A51", "A52", "A53", "A15", "A16", "A17", "A15", "A16"))
dados_PR_Condicoes_Evitaveis = FiltraDadosCID(dados_PR, c("I00", "I01", "I02", "A51", "A52", "A53", "A15", "A16", "A17", "A15", "A16"))
dados_RJ_Condicoes_Evitaveis = FiltraDadosCID(dados_RJ, c("I00", "I01", "I02", "A51", "A52", "A53", "A15", "A16", "A17", "A15", "A16"))
dados_RN_Condicoes_Evitaveis = FiltraDadosCID(dados_RN, c("I00", "I01", "I02", "A51", "A52", "A53", "A15", "A16", "A17", "A15", "A16"))
dados_RO_Condicoes_Evitaveis = FiltraDadosCID(dados_RO, c("I00", "I01", "I02", "A51", "A52", "A53", "A15", "A16", "A17", "A15", "A16"))
dados_RR_Condicoes_Evitaveis = FiltraDadosCID(dados_RR, c("I00", "I01", "I02", "A51", "A52", "A53", "A15", "A16", "A17", "A15", "A16"))
dados_RS_Condicoes_Evitaveis = FiltraDadosCID(dados_RS, c("I00", "I01", "I02", "A51", "A52", "A53", "A15", "A16", "A17", "A15", "A16"))
dados_SC_Condicoes_Evitaveis = FiltraDadosCID(dados_SC, c("I00", "I01", "I02", "A51", "A52", "A53", "A15", "A16", "A17", "A15", "A16"))
dados_SE_Condicoes_Evitaveis = FiltraDadosCID(dados_SE, c("I00", "I01", "I02", "A51", "A52", "A53", "A15", "A16", "A17", "A15", "A16"))
dados_SP_Condicoes_Evitaveis = FiltraDadosCID(dados_SP, c("I00", "I01", "I02", "A51", "A52", "A53", "A15", "A16", "A17", "A15", "A16"))
dados_TO_Condicoes_Evitaveis = FiltraDadosCID(dados_TO, c("I00", "I01", "I02", "A51", "A52", "A53", "A15", "A16", "A17", "A15", "A16"))
estados = c("AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT","PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO")
for (uf in estados) {
  tryCatch({arrow::write_parquet(
    get(paste0("dados_", uf, "_Condicoes_Evitaveis")) %>% as.data.frame(),
    paste0("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Condições evitáveis/dados_", uf, "_Condicoes_Evitaveis.parquet"))},
    error = function(e) { arrow::write_parquet(
      get(paste0("dados_", uf, "_Condicoes_Evitaveis")) %>% as.data.frame(),
      paste0("D:/NESCON/internacoes-sih-sus/Dados por CID/Condições evitáveis/dados_", uf, "_Condicoes_Evitaveis.parquet")) })
}
dados_Condicoes_Evitaveis = rbind(dados_AC_Condicoes_Evitaveis,dados_AL_Condicoes_Evitaveis,dados_AM_Condicoes_Evitaveis,dados_AP_Condicoes_Evitaveis,
                                dados_BA_Condicoes_Evitaveis,dados_CE_Condicoes_Evitaveis,dados_DF_Condicoes_Evitaveis,dados_ES_Condicoes_Evitaveis,
                                dados_GO_Condicoes_Evitaveis,dados_MA_Condicoes_Evitaveis,dados_MG_Condicoes_Evitaveis,dados_MS_Condicoes_Evitaveis,
                                dados_MT_Condicoes_Evitaveis,dados_PA_Condicoes_Evitaveis,dados_PB_Condicoes_Evitaveis,dados_PE_Condicoes_Evitaveis,
                                dados_PI_Condicoes_Evitaveis,dados_PR_Condicoes_Evitaveis,dados_RJ_Condicoes_Evitaveis,dados_RN_Condicoes_Evitaveis,
                                dados_RO_Condicoes_Evitaveis,dados_RR_Condicoes_Evitaveis,dados_RS_Condicoes_Evitaveis,dados_SC_Condicoes_Evitaveis,
                                dados_SE_Condicoes_Evitaveis,dados_SP_Condicoes_Evitaveis,dados_TO_Condicoes_Evitaveis)
tryCatch({arrow::write_parquet(dados_Condicoes_Evitaveis %>% as.data.frame(), "C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Condições evitáveis/dados_Condicoes_Evitaveis.parquet")},
         error = function(e) { arrow::write_parquet(dados_Condicoes_Evitaveis %>% as.data.frame(), 'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Condições evitáveis/dados_Condicoes_Evitaveis.parquet') })
tryCatch({write.xlsx(dados_Condicoes_Evitaveis %>% as.data.frame(), "C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Condições evitáveis/dados_Condicoes_Evitaveis.xlsx")},
         error = function(e) { write.xlsx(dados_Condicoes_Evitaveis %>% as.data.frame(), "D:/NESCON/internacoes-sih-sus/Dados por CID/Condições evitáveis/dados_Condicoes_Evitaveis.xlsx") })

####===========================
#### Deficiências nutricionais
####===========================
dados_AC_Def_nut = FiltraDadosCID(dados_AC, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E62","E63","E64"))
dados_AL_Def_nut = FiltraDadosCID(dados_AL, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E62","E63","E64"))
dados_AM_Def_nut = FiltraDadosCID(dados_AM, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E62","E63","E64"))
dados_AP_Def_nut = FiltraDadosCID(dados_AP, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E62","E63","E64"))
dados_BA_Def_nut = FiltraDadosCID(dados_BA, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E62","E63","E64"))
dados_CE_Def_nut = FiltraDadosCID(dados_CE, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E62","E63","E64"))
dados_DF_Def_nut = FiltraDadosCID(dados_DF, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E62","E63","E64"))
dados_ES_Def_nut = FiltraDadosCID(dados_ES, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E62","E63","E64"))
dados_GO_Def_nut = FiltraDadosCID(dados_GO, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E62","E63","E64"))
dados_MA_Def_nut = FiltraDadosCID(dados_MA, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E62","E63","E64"))
dados_MG_Def_nut = FiltraDadosCID(dados_MG, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E62","E63","E64"))
dados_MS_Def_nut = FiltraDadosCID(dados_MS, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E62","E63","E64"))
dados_MT_Def_nut = FiltraDadosCID(dados_MT, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E62","E63","E64"))
dados_PA_Def_nut = FiltraDadosCID(dados_PA, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E62","E63","E64"))
dados_PB_Def_nut = FiltraDadosCID(dados_PB, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E62","E63","E64"))
dados_PE_Def_nut = FiltraDadosCID(dados_PE, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E62","E63","E64"))
dados_PI_Def_nut = FiltraDadosCID(dados_PI, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E62","E63","E64"))
dados_PR_Def_nut = FiltraDadosCID(dados_PR, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E62","E63","E64"))
dados_RJ_Def_nut = FiltraDadosCID(dados_RJ, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E62","E63","E64"))
dados_RN_Def_nut = FiltraDadosCID(dados_RN, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E62","E63","E64"))
dados_RO_Def_nut = FiltraDadosCID(dados_RO, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E62","E63","E64"))
dados_RR_Def_nut = FiltraDadosCID(dados_RR, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E62","E63","E64"))
dados_RS_Def_nut = FiltraDadosCID(dados_RS, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E62","E63","E64"))
dados_SC_Def_nut = FiltraDadosCID(dados_SC, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E62","E63","E64"))
dados_SE_Def_nut = FiltraDadosCID(dados_SE, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E62","E63","E64"))
dados_SP_Def_nut = FiltraDadosCID(dados_SP, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E62","E63","E64"))
dados_TO_Def_nut = FiltraDadosCID(dados_TO, c("E40","E41","E42","E43","E44","E45","E46","E50","E51","E52","E53","E54","E55","E56","E58","E59","E60","E61","E62","E63","E64"))

estados = c("AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT","PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO")

for (uf in estados) {
  tryCatch({arrow::write_parquet(
    get(paste0("dados_", uf, "_Def_nut")) %>% as.data.frame(),
    paste0("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Deficiências nutricionais/dados_", uf, "_Def_nut.parquet"))},
    error = function(e) { arrow::write_parquet(
      get(paste0("dados_", uf, "_Def_nut")) %>% as.data.frame(),
      paste0("D:/NESCON/internacoes-sih-sus/Dados por CID/Deficiências nutricionais/dados_", uf, "_Def_nut.parquet")) })
}

dados_Def_nut = rbind(dados_AC_Def_nut,dados_AL_Def_nut,dados_AM_Def_nut,dados_AP_Def_nut,
                      dados_BA_Def_nut,dados_CE_Def_nut,dados_DF_Def_nut,dados_ES_Def_nut,
                      dados_GO_Def_nut,dados_MA_Def_nut,dados_MG_Def_nut,dados_MS_Def_nut,
                      dados_MT_Def_nut,dados_PA_Def_nut,dados_PB_Def_nut,dados_PE_Def_nut,
                      dados_PI_Def_nut,dados_PR_Def_nut,dados_RJ_Def_nut,dados_RN_Def_nut,
                      dados_RO_Def_nut,dados_RR_Def_nut,dados_RS_Def_nut,dados_SC_Def_nut,
                      dados_SE_Def_nut,dados_SP_Def_nut,dados_TO_Def_nut)
tryCatch({arrow::write_parquet(dados_Def_nut %>% as.data.frame(), "C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Deficiências nutricionais/dados_Def_nut.parquet")},
         error = function(e) { arrow::write_parquet(dados_Def_nut %>% as.data.frame(), 'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Deficiências nutricionais/dados_Def_nut.parquet') })
tryCatch({write.xlsx(dados_Def_nut %>% as.data.frame(), "C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Deficiências nutricionais/dados_Def_nut.xlsx")},
         error = function(e) { write.xlsx(dados_Def_nut %>% as.data.frame(), "D:/NESCON/internacoes-sih-sus/Dados por CID/Deficiências nutricionais/dados_Def_nut.xlsx") })

####==========
#### Diabetes
####==========
dados_AC_Diabetes = FiltraDadosCID(dados_AC, c("E10"))
dados_AL_Diabetes = FiltraDadosCID(dados_AL, c("E10"))
dados_AM_Diabetes = FiltraDadosCID(dados_AM, c("E10"))
dados_AP_Diabetes = FiltraDadosCID(dados_AP, c("E10"))
dados_BA_Diabetes = FiltraDadosCID(dados_BA, c("E10"))
dados_CE_Diabetes = FiltraDadosCID(dados_CE, c("E10"))
dados_DF_Diabetes = FiltraDadosCID(dados_DF, c("E10"))
dados_ES_Diabetes = FiltraDadosCID(dados_ES, c("E10"))
dados_GO_Diabetes = FiltraDadosCID(dados_GO, c("E10"))
dados_MA_Diabetes = FiltraDadosCID(dados_MA, c("E10"))
dados_MG_Diabetes = FiltraDadosCID(dados_MG, c("E10"))
dados_MS_Diabetes = FiltraDadosCID(dados_MS, c("E10"))
dados_MT_Diabetes = FiltraDadosCID(dados_MT, c("E10"))
dados_PA_Diabetes = FiltraDadosCID(dados_PA, c("E10"))
dados_PB_Diabetes = FiltraDadosCID(dados_PB, c("E10"))
dados_PE_Diabetes = FiltraDadosCID(dados_PE, c("E10"))
dados_PI_Diabetes = FiltraDadosCID(dados_PI, c("E10"))
dados_PR_Diabetes = FiltraDadosCID(dados_PR, c("E10"))
dados_RJ_Diabetes = FiltraDadosCID(dados_RJ, c("E10"))
dados_RN_Diabetes = FiltraDadosCID(dados_RN, c("E10"))
dados_RO_Diabetes = FiltraDadosCID(dados_RO, c("E10"))
dados_RR_Diabetes = FiltraDadosCID(dados_RR, c("E10"))
dados_RS_Diabetes = FiltraDadosCID(dados_RS, c("E10"))
dados_SC_Diabetes = FiltraDadosCID(dados_SC, c("E10"))
dados_SE_Diabetes = FiltraDadosCID(dados_SE, c("E10"))
dados_SP_Diabetes = FiltraDadosCID(dados_SP, c("E10"))
dados_TO_Diabetes = FiltraDadosCID(dados_TO, c("E10"))

estados = c("AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT","PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO")

for (uf in estados) {
  tryCatch({arrow::write_parquet(
    get(paste0("dados_", uf, "_Diabetes")) %>% as.data.frame(),
    paste0("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Diabetes/dados_", uf, "_Diabetes.parquet"))},
    error = function(e) { arrow::write_parquet(
      get(paste0("dados_", uf, "_Diabetes")) %>% as.data.frame(),
      paste0("D:/NESCON/internacoes-sih-sus/Dados por CID/Diabetes/dados_", uf, "_Diabetes.parquet")) })
}

dados_Diabetes = rbind(dados_AC_Diabetes,dados_AL_Diabetes,dados_AM_Diabetes,dados_AP_Diabetes,
                       dados_BA_Diabetes,dados_CE_Diabetes,dados_DF_Diabetes,dados_ES_Diabetes,
                       dados_GO_Diabetes,dados_MA_Diabetes,dados_MG_Diabetes,dados_MS_Diabetes,
                       dados_MT_Diabetes,dados_PA_Diabetes,dados_PB_Diabetes,dados_PE_Diabetes,
                       dados_PI_Diabetes,dados_PR_Diabetes,dados_RJ_Diabetes,dados_RN_Diabetes,
                       dados_RO_Diabetes,dados_RR_Diabetes,dados_RS_Diabetes,dados_SC_Diabetes,
                       dados_SE_Diabetes,dados_SP_Diabetes,dados_TO_Diabetes)
tryCatch({arrow::write_parquet(dados_Diabetes %>% as.data.frame(), "C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Diabetes/dados_Diabetes.parquet")},
         error = function(e) { arrow::write_parquet(dados_Diabetes %>% as.data.frame(), 'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Diabetes/dados_Diabetes.parquet') })
tryCatch({write.xlsx(dados_Diabetes %>% as.data.frame(), "C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Diabetes/dados_Diabetes.xlsx")},
         error = function(e) { write.xlsx(dados_Diabetes %>% as.data.frame(), "D:/NESCON/internacoes-sih-sus/Dados por CID/Diabetes/dados_Diabetes.xlsx") })

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
estados = c("AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT","PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO")
for (uf in estados) {
  tryCatch({arrow::write_parquet(
    get(paste0("dados_", uf, "_D_Inf_Org_Pelv_Fem")) %>% as.data.frame(),
    paste0("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Doença Inflamatória órgãos pélvicos femininos/dados_", uf, "_D_Inf_Org_Pelv_Fem.parquet"))},
    error = function(e) { arrow::write_parquet(
      get(paste0("dados_", uf, "_D_Inf_Org_Pelv_Fem")) %>% as.data.frame(),
      paste0("D:/NESCON/internacoes-sih-sus/Dados por CID/Doença Inflamatória órgãos pélvicos femininos/dados_", uf, "_D_Inf_Org_Pelv_Fem.parquet")) })
}
dados_D_Inf_Org_Pelv_Fem = rbind(dados_AC_D_Inf_Org_Pelv_Fem,dados_AL_D_Inf_Org_Pelv_Fem,dados_AM_D_Inf_Org_Pelv_Fem,dados_AP_D_Inf_Org_Pelv_Fem,
                              dados_BA_D_Inf_Org_Pelv_Fem,dados_CE_D_Inf_Org_Pelv_Fem,dados_DF_D_Inf_Org_Pelv_Fem,dados_ES_D_Inf_Org_Pelv_Fem,
                              dados_GO_D_Inf_Org_Pelv_Fem,dados_MA_D_Inf_Org_Pelv_Fem,dados_MG_D_Inf_Org_Pelv_Fem,dados_MS_D_Inf_Org_Pelv_Fem,
                              dados_MT_D_Inf_Org_Pelv_Fem,dados_PA_D_Inf_Org_Pelv_Fem,dados_PB_D_Inf_Org_Pelv_Fem,dados_PE_D_Inf_Org_Pelv_Fem,
                              dados_PI_D_Inf_Org_Pelv_Fem,dados_PR_D_Inf_Org_Pelv_Fem,dados_RJ_D_Inf_Org_Pelv_Fem,dados_RN_D_Inf_Org_Pelv_Fem,
                              dados_RO_D_Inf_Org_Pelv_Fem,dados_RR_D_Inf_Org_Pelv_Fem,dados_RS_D_Inf_Org_Pelv_Fem,dados_SC_D_Inf_Org_Pelv_Fem,
                              dados_SE_D_Inf_Org_Pelv_Fem,dados_SP_D_Inf_Org_Pelv_Fem,dados_TO_D_Inf_Org_Pelv_Fem)
tryCatch({arrow::write_parquet(dados_D_Inf_Org_Pelv_Fem %>% as.data.frame(), "C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Doença Inflamatória órgãos pélvicos femininos/dados_D_Inf_Org_Pelv_Fem.parquet")},
         error = function(e) { arrow::write_parquet(dados_D_Inf_Org_Pelv_Fem %>% as.data.frame(), 'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doença Inflamatória órgãos pélvicos femininos/dados_D_Inf_Org_Pelv_Fem.parquet') })
tryCatch({write.xlsx(dados_D_Inf_Org_Pelv_Fem %>% as.data.frame(), "C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Doença Inflamatória órgãos pélvicos femininos/dados_D_Inf_Org_Pelv_Fem.xlsx")},
         error = function(e) { write.xlsx(dados_D_Inf_Org_Pelv_Fem %>% as.data.frame(), "D:/NESCON/internacoes-sih-sus/Dados por CID/Doença Inflamatória órgãos pélvicos femininos/dados_D_Inf_Org_Pelv_Fem.xlsx") })

####============================
#### Doenças Cerebro-vasculares
####============================
dados_AC_D_Cerebrovasc = FiltraDadosCID(dados_AC, c("G45", "G46", "I63", "I64", "I65", "I66", "I67", "I69"))
dados_AL_D_Cerebrovasc = FiltraDadosCID(dados_AL, c("G45", "G46", "I63", "I64", "I65", "I66", "I67", "I69"))
dados_AM_D_Cerebrovasc = FiltraDadosCID(dados_AM, c("G45", "G46", "I63", "I64", "I65", "I66", "I67", "I69"))
dados_AP_D_Cerebrovasc = FiltraDadosCID(dados_AP, c("G45", "G46", "I63", "I64", "I65", "I66", "I67", "I69"))
dados_BA_D_Cerebrovasc = FiltraDadosCID(dados_BA, c("G45", "G46", "I63", "I64", "I65", "I66", "I67", "I69"))
dados_CE_D_Cerebrovasc = FiltraDadosCID(dados_CE, c("G45", "G46", "I63", "I64", "I65", "I66", "I67", "I69"))
dados_DF_D_Cerebrovasc = FiltraDadosCID(dados_DF, c("G45", "G46", "I63", "I64", "I65", "I66", "I67", "I69"))
dados_ES_D_Cerebrovasc = FiltraDadosCID(dados_ES, c("G45", "G46", "I63", "I64", "I65", "I66", "I67", "I69"))
dados_GO_D_Cerebrovasc = FiltraDadosCID(dados_GO, c("G45", "G46", "I63", "I64", "I65", "I66", "I67", "I69"))
dados_MA_D_Cerebrovasc = FiltraDadosCID(dados_MA, c("G45", "G46", "I63", "I64", "I65", "I66", "I67", "I69"))
dados_MG_D_Cerebrovasc = FiltraDadosCID(dados_MG, c("G45", "G46", "I63", "I64", "I65", "I66", "I67", "I69"))
dados_MS_D_Cerebrovasc = FiltraDadosCID(dados_MS, c("G45", "G46", "I63", "I64", "I65", "I66", "I67", "I69"))
dados_MT_D_Cerebrovasc = FiltraDadosCID(dados_MT, c("G45", "G46", "I63", "I64", "I65", "I66", "I67", "I69"))
dados_PA_D_Cerebrovasc = FiltraDadosCID(dados_PA, c("G45", "G46", "I63", "I64", "I65", "I66", "I67", "I69"))
dados_PB_D_Cerebrovasc = FiltraDadosCID(dados_PB, c("G45", "G46", "I63", "I64", "I65", "I66", "I67", "I69"))
dados_PE_D_Cerebrovasc = FiltraDadosCID(dados_PE, c("G45", "G46", "I63", "I64", "I65", "I66", "I67", "I69"))
dados_PI_D_Cerebrovasc = FiltraDadosCID(dados_PI, c("G45", "G46", "I63", "I64", "I65", "I66", "I67", "I69"))
dados_PR_D_Cerebrovasc = FiltraDadosCID(dados_PR, c("G45", "G46", "I63", "I64", "I65", "I66", "I67", "I69"))
dados_RJ_D_Cerebrovasc = FiltraDadosCID(dados_RJ, c("G45", "G46", "I63", "I64", "I65", "I66", "I67", "I69"))
dados_RN_D_Cerebrovasc = FiltraDadosCID(dados_RN, c("G45", "G46", "I63", "I64", "I65", "I66", "I67", "I69"))
dados_RO_D_Cerebrovasc = FiltraDadosCID(dados_RO, c("G45", "G46", "I63", "I64", "I65", "I66", "I67", "I69"))
dados_RR_D_Cerebrovasc = FiltraDadosCID(dados_RR, c("G45", "G46", "I63", "I64", "I65", "I66", "I67", "I69"))
dados_RS_D_Cerebrovasc = FiltraDadosCID(dados_RS, c("G45", "G46", "I63", "I64", "I65", "I66", "I67", "I69"))
dados_SC_D_Cerebrovasc = FiltraDadosCID(dados_SC, c("G45", "G46", "I63", "I64", "I65", "I66", "I67", "I69"))
dados_SE_D_Cerebrovasc = FiltraDadosCID(dados_SE, c("G45", "G46", "I63", "I64", "I65", "I66", "I67", "I69"))
dados_SP_D_Cerebrovasc = FiltraDadosCID(dados_SP, c("G45", "G46", "I63", "I64", "I65", "I66", "I67", "I69"))
dados_TO_D_Cerebrovasc = FiltraDadosCID(dados_TO, c("G45", "G46", "I63", "I64", "I65", "I66", "I67", "I69"))
estados = c("AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT","PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO")
for (uf in estados) {
  tryCatch({arrow::write_parquet(
    get(paste0("dados_", uf, "_D_Cerebrovasc")) %>% as.data.frame(),
    paste0("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Doenças Cerebro-vasculares/dados_", uf, "_D_Cerebrovasc.parquet"))},
    error = function(e) { arrow::write_parquet(
      get(paste0("dados_", uf, "_D_Cerebrovasc")) %>% as.data.frame(),
      paste0("D:/NESCON/internacoes-sih-sus/Dados por CID/Doenças Cerebro-vasculares/dados_", uf, "_D_Cerebrovasc.parquet")) })
}
dados_D_Cerebrovasc = rbind(dados_AC_D_Cerebrovasc,dados_AL_D_Cerebrovasc,dados_AM_D_Cerebrovasc,dados_AP_D_Cerebrovasc,
                            dados_BA_D_Cerebrovasc,dados_CE_D_Cerebrovasc,dados_DF_D_Cerebrovasc,dados_ES_D_Cerebrovasc,
                            dados_GO_D_Cerebrovasc,dados_MA_D_Cerebrovasc,dados_MG_D_Cerebrovasc,dados_MS_D_Cerebrovasc,
                            dados_MT_D_Cerebrovasc,dados_PA_D_Cerebrovasc,dados_PB_D_Cerebrovasc,dados_PE_D_Cerebrovasc,
                            dados_PI_D_Cerebrovasc,dados_PR_D_Cerebrovasc,dados_RJ_D_Cerebrovasc,dados_RN_D_Cerebrovasc,
                            dados_RO_D_Cerebrovasc,dados_RR_D_Cerebrovasc,dados_RS_D_Cerebrovasc,dados_SC_D_Cerebrovasc,
                            dados_SE_D_Cerebrovasc,dados_SP_D_Cerebrovasc,dados_TO_D_Cerebrovasc)
tryCatch({arrow::write_parquet(dados_D_Cerebrovasc %>% as.data.frame(), "C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Doenças Cerebro-vasculares/dados_D_Cerebrovasc.parquet")},
         error = function(e) { arrow::write_parquet(dados_D_Cerebrovasc %>% as.data.frame(), 'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças Cerebro-vasculares/dados_D_Cerebrovasc.parquet') })
tryCatch({write.xlsx(dados_D_Cerebrovasc %>% as.data.frame(), "C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Doenças Cerebro-vasculares/dados_D_Cerebrovasc.xlsx")},
         error = function(e) { write.xlsx(dados_D_Cerebrovasc %>% as.data.frame(), "D:/NESCON/internacoes-sih-sus/Dados por CID/Doenças Cerebro-vasculares/dados_D_Cerebrovasc.xlsx") })

####====================================
#### Doenças das vias aéreas inferiores
####====================================
dados_AC_D_Vias_Aereas_Inf = FiltraDadosCID(dados_AC, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_AL_D_Vias_Aereas_Inf = FiltraDadosCID(dados_AL, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_AM_D_Vias_Aereas_Inf = FiltraDadosCID(dados_AM, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_AP_D_Vias_Aereas_Inf = FiltraDadosCID(dados_AP, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_BA_D_Vias_Aereas_Inf = FiltraDadosCID(dados_BA, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_CE_D_Vias_Aereas_Inf = FiltraDadosCID(dados_CE, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_DF_D_Vias_Aereas_Inf = FiltraDadosCID(dados_DF, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_ES_D_Vias_Aereas_Inf = FiltraDadosCID(dados_ES, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_GO_D_Vias_Aereas_Inf = FiltraDadosCID(dados_GO, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_MA_D_Vias_Aereas_Inf = FiltraDadosCID(dados_MA, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_MG_D_Vias_Aereas_Inf = FiltraDadosCID(dados_MG, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_MS_D_Vias_Aereas_Inf = FiltraDadosCID(dados_MS, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_MT_D_Vias_Aereas_Inf = FiltraDadosCID(dados_MT, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_PA_D_Vias_Aereas_Inf = FiltraDadosCID(dados_PA, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_PB_D_Vias_Aereas_Inf = FiltraDadosCID(dados_PB, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_PE_D_Vias_Aereas_Inf = FiltraDadosCID(dados_PE, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_PI_D_Vias_Aereas_Inf = FiltraDadosCID(dados_PI, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_PR_D_Vias_Aereas_Inf = FiltraDadosCID(dados_PR, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_RJ_D_Vias_Aereas_Inf = FiltraDadosCID(dados_RJ, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_RN_D_Vias_Aereas_Inf = FiltraDadosCID(dados_RN, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_RO_D_Vias_Aereas_Inf = FiltraDadosCID(dados_RO, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_RR_D_Vias_Aereas_Inf = FiltraDadosCID(dados_RR, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_RS_D_Vias_Aereas_Inf = FiltraDadosCID(dados_RS, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_SC_D_Vias_Aereas_Inf = FiltraDadosCID(dados_SC, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_SE_D_Vias_Aereas_Inf = FiltraDadosCID(dados_SE, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_SP_D_Vias_Aereas_Inf = FiltraDadosCID(dados_SP, c("J20","J21","J40","J41","J42","J43","J44","J47"))
dados_TO_D_Vias_Aereas_Inf = FiltraDadosCID(dados_TO, c("J20","J21","J40","J41","J42","J43","J44","J47"))
estados = c("AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT","PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO")
for (uf in estados) {
  tryCatch({arrow::write_parquet(
    get(paste0("dados_", uf, "_D_Vias_Aereas_Inf")) %>% as.data.frame(),
    paste0("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Doenças das vias aéreas inferiores/dados_", uf, "_D_Vias_Aereas_Inf.parquet"))},
    error = function(e) { arrow::write_parquet(
      get(paste0("dados_", uf, "_D_Vias_Aereas_Inf")) %>% as.data.frame(),
      paste0("D:/NESCON/internacoes-sih-sus/Dados por CID/Doenças das vias aéreas inferiores/dados_", uf, "_D_Vias_Aereas_Inf.parquet")) })
}
dados_D_Vias_Aereas_Inf = rbind(dados_AC_D_Vias_Aereas_Inf,dados_AL_D_Vias_Aereas_Inf,dados_AM_D_Vias_Aereas_Inf,dados_AP_D_Vias_Aereas_Inf,
                            dados_BA_D_Vias_Aereas_Inf,dados_CE_D_Vias_Aereas_Inf,dados_DF_D_Vias_Aereas_Inf,dados_ES_D_Vias_Aereas_Inf,
                            dados_GO_D_Vias_Aereas_Inf,dados_MA_D_Vias_Aereas_Inf,dados_MG_D_Vias_Aereas_Inf,dados_MS_D_Vias_Aereas_Inf,
                            dados_MT_D_Vias_Aereas_Inf,dados_PA_D_Vias_Aereas_Inf,dados_PB_D_Vias_Aereas_Inf,dados_PE_D_Vias_Aereas_Inf,
                            dados_PI_D_Vias_Aereas_Inf,dados_PR_D_Vias_Aereas_Inf,dados_RJ_D_Vias_Aereas_Inf,dados_RN_D_Vias_Aereas_Inf,
                            dados_RO_D_Vias_Aereas_Inf,dados_RR_D_Vias_Aereas_Inf,dados_RS_D_Vias_Aereas_Inf,dados_SC_D_Vias_Aereas_Inf,
                            dados_SE_D_Vias_Aereas_Inf,dados_SP_D_Vias_Aereas_Inf,dados_TO_D_Vias_Aereas_Inf)
tryCatch({arrow::write_parquet(dados_D_Vias_Aereas_Inf %>% as.data.frame(), "C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Doenças das vias aéreas inferiores/dados_D_Vias_Aereas_Inf.parquet")},
         error = function(e) { arrow::write_parquet(dados_D_Vias_Aereas_Inf %>% as.data.frame(), 'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças das vias aéreas inferiores/dados_D_Vias_Aereas_Inf.parquet') })
tryCatch({write.xlsx(dados_D_Vias_Aereas_Inf %>% as.data.frame(), "C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Doenças das vias aéreas inferiores/dados_D_Vias_Aereas_Inf.xlsx")},
         error = function(e) { write.xlsx(dados_D_Vias_Aereas_Inf %>% as.data.frame(), "D:/NESCON/internacoes-sih-sus/Dados por CID/Doenças das vias aéreas inferiores/dados_D_Vias_Aereas_Inf.xlsx") })

####=====================
#### Doenças imunizáveis
####=====================
dados_AC_D_Imunizaveis = FiltraDadosCID(dados_AC, c("A37", "A36", "B16", "G00", "A17", "B26", "B06", "B05", "A33", "A34", "A35", "A19"))
dados_AL_D_Imunizaveis = FiltraDadosCID(dados_AL, c("A37", "A36", "B16", "G00", "A17", "B26", "B06", "B05", "A33", "A34", "A35", "A19"))
dados_AM_D_Imunizaveis = FiltraDadosCID(dados_AM, c("A37", "A36", "B16", "G00", "A17", "B26", "B06", "B05", "A33", "A34", "A35", "A19"))
dados_AP_D_Imunizaveis = FiltraDadosCID(dados_AP, c("A37", "A36", "B16", "G00", "A17", "B26", "B06", "B05", "A33", "A34", "A35", "A19"))
dados_BA_D_Imunizaveis = FiltraDadosCID(dados_BA, c("A37", "A36", "B16", "G00", "A17", "B26", "B06", "B05", "A33", "A34", "A35", "A19"))
dados_CE_D_Imunizaveis = FiltraDadosCID(dados_CE, c("A37", "A36", "B16", "G00", "A17", "B26", "B06", "B05", "A33", "A34", "A35", "A19"))
dados_DF_D_Imunizaveis = FiltraDadosCID(dados_DF, c("A37", "A36", "B16", "G00", "A17", "B26", "B06", "B05", "A33", "A34", "A35", "A19"))
dados_ES_D_Imunizaveis = FiltraDadosCID(dados_ES, c("A37", "A36", "B16", "G00", "A17", "B26", "B06", "B05", "A33", "A34", "A35", "A19"))
dados_GO_D_Imunizaveis = FiltraDadosCID(dados_GO, c("A37", "A36", "B16", "G00", "A17", "B26", "B06", "B05", "A33", "A34", "A35", "A19"))
dados_MA_D_Imunizaveis = FiltraDadosCID(dados_MA, c("A37", "A36", "B16", "G00", "A17", "B26", "B06", "B05", "A33", "A34", "A35", "A19"))
dados_MG_D_Imunizaveis = FiltraDadosCID(dados_MG, c("A37", "A36", "B16", "G00", "A17", "B26", "B06", "B05", "A33", "A34", "A35", "A19"))
dados_MS_D_Imunizaveis = FiltraDadosCID(dados_MS, c("A37", "A36", "B16", "G00", "A17", "B26", "B06", "B05", "A33", "A34", "A35", "A19"))
dados_MT_D_Imunizaveis = FiltraDadosCID(dados_MT, c("A37", "A36", "B16", "G00", "A17", "B26", "B06", "B05", "A33", "A34", "A35", "A19"))
dados_PA_D_Imunizaveis = FiltraDadosCID(dados_PA, c("A37", "A36", "B16", "G00", "A17", "B26", "B06", "B05", "A33", "A34", "A35", "A19"))
dados_PB_D_Imunizaveis = FiltraDadosCID(dados_PB, c("A37", "A36", "B16", "G00", "A17", "B26", "B06", "B05", "A33", "A34", "A35", "A19"))
dados_PE_D_Imunizaveis = FiltraDadosCID(dados_PE, c("A37", "A36", "B16", "G00", "A17", "B26", "B06", "B05", "A33", "A34", "A35", "A19"))
dados_PI_D_Imunizaveis = FiltraDadosCID(dados_PI, c("A37", "A36", "B16", "G00", "A17", "B26", "B06", "B05", "A33", "A34", "A35", "A19"))
dados_PR_D_Imunizaveis = FiltraDadosCID(dados_PR, c("A37", "A36", "B16", "G00", "A17", "B26", "B06", "B05", "A33", "A34", "A35", "A19"))
dados_RJ_D_Imunizaveis = FiltraDadosCID(dados_RJ, c("A37", "A36", "B16", "G00", "A17", "B26", "B06", "B05", "A33", "A34", "A35", "A19"))
dados_RN_D_Imunizaveis = FiltraDadosCID(dados_RN, c("A37", "A36", "B16", "G00", "A17", "B26", "B06", "B05", "A33", "A34", "A35", "A19"))
dados_RO_D_Imunizaveis = FiltraDadosCID(dados_RO, c("A37", "A36", "B16", "G00", "A17", "B26", "B06", "B05", "A33", "A34", "A35", "A19"))
dados_RR_D_Imunizaveis = FiltraDadosCID(dados_RR, c("A37", "A36", "B16", "G00", "A17", "B26", "B06", "B05", "A33", "A34", "A35", "A19"))
dados_RS_D_Imunizaveis = FiltraDadosCID(dados_RS, c("A37", "A36", "B16", "G00", "A17", "B26", "B06", "B05", "A33", "A34", "A35", "A19"))
dados_SC_D_Imunizaveis = FiltraDadosCID(dados_SC, c("A37", "A36", "B16", "G00", "A17", "B26", "B06", "B05", "A33", "A34", "A35", "A19"))
dados_SE_D_Imunizaveis = FiltraDadosCID(dados_SE, c("A37", "A36", "B16", "G00", "A17", "B26", "B06", "B05", "A33", "A34", "A35", "A19"))
dados_SP_D_Imunizaveis = FiltraDadosCID(dados_SP, c("A37", "A36", "B16", "G00", "A17", "B26", "B06", "B05", "A33", "A34", "A35", "A19"))
dados_TO_D_Imunizaveis = FiltraDadosCID(dados_TO, c("A37", "A36", "B16", "G00", "A17", "B26", "B06", "B05", "A33", "A34", "A35", "A19"))
estados = c("AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT","PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO")
for (uf in estados) {
  tryCatch({arrow::write_parquet(
    get(paste0("dados_", uf, "_D_Imunizaveis")) %>% as.data.frame(),
    paste0("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Doenças imunizáveis/dados_", uf, "_D_Imunizaveis.parquet"))},
    error = function(e) { arrow::write_parquet(
      get(paste0("dados_", uf, "_D_Imunizaveis")) %>% as.data.frame(),
      paste0("D:/NESCON/internacoes-sih-sus/Dados por CID/Doenças imunizáveis/dados_", uf, "_D_Imunizaveis.parquet")) })
}
dados_D_Imunizaveis = rbind(dados_AC_D_Imunizaveis,dados_AL_D_Imunizaveis,dados_AM_D_Imunizaveis,dados_AP_D_Imunizaveis,
                                 dados_BA_D_Imunizaveis,dados_CE_D_Imunizaveis,dados_DF_D_Imunizaveis,dados_ES_D_Imunizaveis,
                                 dados_GO_D_Imunizaveis,dados_MA_D_Imunizaveis,dados_MG_D_Imunizaveis,dados_MS_D_Imunizaveis,
                                 dados_MT_D_Imunizaveis,dados_PA_D_Imunizaveis,dados_PB_D_Imunizaveis,dados_PE_D_Imunizaveis,
                                 dados_PI_D_Imunizaveis,dados_PR_D_Imunizaveis,dados_RJ_D_Imunizaveis,dados_RN_D_Imunizaveis,
                                 dados_RO_D_Imunizaveis,dados_RR_D_Imunizaveis,dados_RS_D_Imunizaveis,dados_SC_D_Imunizaveis,
                                 dados_SE_D_Imunizaveis,dados_SP_D_Imunizaveis,dados_TO_D_Imunizaveis)
tryCatch({arrow::write_parquet(dados_D_Imunizaveis %>% as.data.frame(), "C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Doenças imunizáveis/dados_D_Imunizaveis.parquet")},
         error = function(e) { arrow::write_parquet(dados_D_Imunizaveis %>% as.data.frame(), 'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças imunizáveis/dados_D_Imunizaveis.parquet') })
tryCatch({write.xlsx(dados_D_Imunizaveis %>% as.data.frame(), "C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Doenças imunizáveis/dados_D_Imunizaveis.xlsx")},
         error = function(e) { write.xlsx(dados_D_Imunizaveis %>% as.data.frame(), "D:/NESCON/internacoes-sih-sus/Dados por CID/Doenças imunizáveis/dados_D_Imunizaveis.xlsx") })

####===========================================
#### Doenças relacionadas ao Pré-Natal e Parto
####===========================================
dados_AC_D_Rel_PreNat_Parto = FiltraDadosCID(dados_AC, c("O23", "A50", "P35"))
dados_AL_D_Rel_PreNat_Parto = FiltraDadosCID(dados_AL, c("O23", "A50", "P35"))
dados_AM_D_Rel_PreNat_Parto = FiltraDadosCID(dados_AM, c("O23", "A50", "P35"))
dados_AP_D_Rel_PreNat_Parto = FiltraDadosCID(dados_AP, c("O23", "A50", "P35"))
dados_BA_D_Rel_PreNat_Parto = FiltraDadosCID(dados_BA, c("O23", "A50", "P35"))
dados_CE_D_Rel_PreNat_Parto = FiltraDadosCID(dados_CE, c("O23", "A50", "P35"))
dados_DF_D_Rel_PreNat_Parto = FiltraDadosCID(dados_DF, c("O23", "A50", "P35"))
dados_ES_D_Rel_PreNat_Parto = FiltraDadosCID(dados_ES, c("O23", "A50", "P35"))
dados_GO_D_Rel_PreNat_Parto = FiltraDadosCID(dados_GO, c("O23", "A50", "P35"))
dados_MA_D_Rel_PreNat_Parto = FiltraDadosCID(dados_MA, c("O23", "A50", "P35"))
dados_MG_D_Rel_PreNat_Parto = FiltraDadosCID(dados_MG, c("O23", "A50", "P35"))
dados_MS_D_Rel_PreNat_Parto = FiltraDadosCID(dados_MS, c("O23", "A50", "P35"))
dados_MT_D_Rel_PreNat_Parto = FiltraDadosCID(dados_MT, c("O23", "A50", "P35"))
dados_PA_D_Rel_PreNat_Parto = FiltraDadosCID(dados_PA, c("O23", "A50", "P35"))
dados_PB_D_Rel_PreNat_Parto = FiltraDadosCID(dados_PB, c("O23", "A50", "P35"))
dados_PE_D_Rel_PreNat_Parto = FiltraDadosCID(dados_PE, c("O23", "A50", "P35"))
dados_PI_D_Rel_PreNat_Parto = FiltraDadosCID(dados_PI, c("O23", "A50", "P35"))
dados_PR_D_Rel_PreNat_Parto = FiltraDadosCID(dados_PR, c("O23", "A50", "P35"))
dados_RJ_D_Rel_PreNat_Parto = FiltraDadosCID(dados_RJ, c("O23", "A50", "P35"))
dados_RN_D_Rel_PreNat_Parto = FiltraDadosCID(dados_RN, c("O23", "A50", "P35"))
dados_RO_D_Rel_PreNat_Parto = FiltraDadosCID(dados_RO, c("O23", "A50", "P35"))
dados_RR_D_Rel_PreNat_Parto = FiltraDadosCID(dados_RR, c("O23", "A50", "P35"))
dados_RS_D_Rel_PreNat_Parto = FiltraDadosCID(dados_RS, c("O23", "A50", "P35"))
dados_SC_D_Rel_PreNat_Parto = FiltraDadosCID(dados_SC, c("O23", "A50", "P35"))
dados_SE_D_Rel_PreNat_Parto = FiltraDadosCID(dados_SE, c("O23", "A50", "P35"))
dados_SP_D_Rel_PreNat_Parto = FiltraDadosCID(dados_SP, c("O23", "A50", "P35"))
dados_TO_D_Rel_PreNat_Parto = FiltraDadosCID(dados_TO, c("O23", "A50", "P35"))
estados = c("AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT","PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO")
for (uf in estados) {
  tryCatch({arrow::write_parquet(
    get(paste0("dados_", uf, "_D_Rel_PreNat_Parto")) %>% as.data.frame(),
    paste0("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Doenças relacionadas ao Pré-Natal e Parto/dados_", uf, "_D_Rel_PreNat_Parto.parquet"))},
    error = function(e) { arrow::write_parquet(
      get(paste0("dados_", uf, "_D_Rel_PreNat_Parto")) %>% as.data.frame(),
      paste0("D:/NESCON/internacoes-sih-sus/Dados por CID/Doenças relacionadas ao Pré-Natal e Parto/dados_", uf, "_D_Rel_PreNat_Parto.parquet")) })
}
dados_D_Rel_PreNat_Parto = rbind(dados_AC_D_Rel_PreNat_Parto,dados_AL_D_Rel_PreNat_Parto,dados_AM_D_Rel_PreNat_Parto,dados_AP_D_Rel_PreNat_Parto,
                         dados_BA_D_Rel_PreNat_Parto,dados_CE_D_Rel_PreNat_Parto,dados_DF_D_Rel_PreNat_Parto,dados_ES_D_Rel_PreNat_Parto,
                         dados_GO_D_Rel_PreNat_Parto,dados_MA_D_Rel_PreNat_Parto,dados_MG_D_Rel_PreNat_Parto,dados_MS_D_Rel_PreNat_Parto,
                         dados_MT_D_Rel_PreNat_Parto,dados_PA_D_Rel_PreNat_Parto,dados_PB_D_Rel_PreNat_Parto,dados_PE_D_Rel_PreNat_Parto,
                         dados_PI_D_Rel_PreNat_Parto,dados_PR_D_Rel_PreNat_Parto,dados_RJ_D_Rel_PreNat_Parto,dados_RN_D_Rel_PreNat_Parto,
                         dados_RO_D_Rel_PreNat_Parto,dados_RR_D_Rel_PreNat_Parto,dados_RS_D_Rel_PreNat_Parto,dados_SC_D_Rel_PreNat_Parto,
                         dados_SE_D_Rel_PreNat_Parto,dados_SP_D_Rel_PreNat_Parto,dados_TO_D_Rel_PreNat_Parto)
tryCatch({arrow::write_parquet(dados_D_Rel_PreNat_Parto %>% as.data.frame(), "C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Doenças relacionadas ao Pré-Natal e Parto/dados_D_Rel_PreNat_Parto.parquet")},
         error = function(e) { arrow::write_parquet(dados_D_Rel_PreNat_Parto %>% as.data.frame(), 'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Doenças relacionadas ao Pré-Natal e Parto/dados_D_Rel_PreNat_Parto.parquet') })
tryCatch({write.xlsx(dados_D_Rel_PreNat_Parto %>% as.data.frame(), "C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Doenças relacionadas ao Pré-Natal e Parto/dados_D_Rel_PreNat_Parto.xlsx")},
         error = function(e) { write.xlsx(dados_D_Rel_PreNat_Parto %>% as.data.frame(), "D:/NESCON/internacoes-sih-sus/Dados por CID/Doenças relacionadas ao Pré-Natal e Parto/dados_D_Rel_PreNat_Parto.xlsx") })

####============
#### Epilepsias
####============
dados_AC_Epilepsias = FiltraDadosCID(dados_AC, c("G40","G41"))
dados_AL_Epilepsias = FiltraDadosCID(dados_AL, c("G40","G41"))
dados_AM_Epilepsias = FiltraDadosCID(dados_AM, c("G40","G41"))
dados_AP_Epilepsias = FiltraDadosCID(dados_AP, c("G40","G41"))
dados_BA_Epilepsias = FiltraDadosCID(dados_BA, c("G40","G41"))
dados_CE_Epilepsias = FiltraDadosCID(dados_CE, c("G40","G41"))
dados_DF_Epilepsias = FiltraDadosCID(dados_DF, c("G40","G41"))
dados_ES_Epilepsias = FiltraDadosCID(dados_ES, c("G40","G41"))
dados_GO_Epilepsias = FiltraDadosCID(dados_GO, c("G40","G41"))
dados_MA_Epilepsias = FiltraDadosCID(dados_MA, c("G40","G41"))
dados_MG_Epilepsias = FiltraDadosCID(dados_MG, c("G40","G41"))
dados_MS_Epilepsias = FiltraDadosCID(dados_MS, c("G40","G41"))
dados_MT_Epilepsias = FiltraDadosCID(dados_MT, c("G40","G41"))
dados_PA_Epilepsias = FiltraDadosCID(dados_PA, c("G40","G41"))
dados_PB_Epilepsias = FiltraDadosCID(dados_PB, c("G40","G41"))
dados_PE_Epilepsias = FiltraDadosCID(dados_PE, c("G40","G41"))
dados_PI_Epilepsias = FiltraDadosCID(dados_PI, c("G40","G41"))
dados_PR_Epilepsias = FiltraDadosCID(dados_PR, c("G40","G41"))
dados_RJ_Epilepsias = FiltraDadosCID(dados_RJ, c("G40","G41"))
dados_RN_Epilepsias = FiltraDadosCID(dados_RN, c("G40","G41"))
dados_RO_Epilepsias = FiltraDadosCID(dados_RO, c("G40","G41"))
dados_RR_Epilepsias = FiltraDadosCID(dados_RR, c("G40","G41"))
dados_RS_Epilepsias = FiltraDadosCID(dados_RS, c("G40","G41"))
dados_SC_Epilepsias = FiltraDadosCID(dados_SC, c("G40","G41"))
dados_SE_Epilepsias = FiltraDadosCID(dados_SE, c("G40","G41"))
dados_SP_Epilepsias = FiltraDadosCID(dados_SP, c("G40","G41"))
dados_TO_Epilepsias = FiltraDadosCID(dados_TO, c("G40","G41"))
estados = c("AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT","PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO")
for (uf in estados) {
  tryCatch({arrow::write_parquet(
    get(paste0("dados_", uf, "_Epilepsias")) %>% as.data.frame(),
    paste0("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Epilepsias/dados_", uf, "_Epilepsias.parquet"))},
    error = function(e) { arrow::write_parquet(
      get(paste0("dados_", uf, "_Epilepsias")) %>% as.data.frame(),
      paste0("D:/NESCON/internacoes-sih-sus/Dados por CID/Epilepsias/dados_", uf, "_Epilepsias.parquet")) })
}
dados_Epilepsias = rbind(dados_AC_Epilepsias,dados_AL_Epilepsias,dados_AM_Epilepsias,dados_AP_Epilepsias,
                         dados_BA_Epilepsias,dados_CE_Epilepsias,dados_DF_Epilepsias,dados_ES_Epilepsias,
                         dados_GO_Epilepsias,dados_MA_Epilepsias,dados_MG_Epilepsias,dados_MS_Epilepsias,
                         dados_MT_Epilepsias,dados_PA_Epilepsias,dados_PB_Epilepsias,dados_PE_Epilepsias,
                         dados_PI_Epilepsias,dados_PR_Epilepsias,dados_RJ_Epilepsias,dados_RN_Epilepsias,
                         dados_RO_Epilepsias,dados_RR_Epilepsias,dados_RS_Epilepsias,dados_SC_Epilepsias,
                         dados_SE_Epilepsias,dados_SP_Epilepsias,dados_TO_Epilepsias)
tryCatch({arrow::write_parquet(dados_Epilepsias %>% as.data.frame(), "C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Epilepsias/dados_Epilepsias.parquet")},
         error = function(e) { arrow::write_parquet(dados_Epilepsias %>% as.data.frame(), 'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Epilepsias/dados_Epilepsias.parquet') })
tryCatch({write.xlsx(dados_Epilepsias %>% as.data.frame(), "C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Epilepsias/dados_Epilepsias.xlsx")},
         error = function(e) { write.xlsx(dados_Epilepsias %>% as.data.frame(), "D:/NESCON/internacoes-sih-sus/Dados por CID/Epilepsias/dados_Epilepsias.xlsx") })

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
estados = c("AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT","PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO")
for (uf in estados) {
  tryCatch({arrow::write_parquet(
    get(paste0("dados_", uf, "_Gastro_Inf_Comp")) %>% as.data.frame(),
    paste0("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Gastroenterites Infecciosas e complicações/dados_", uf, "_Gastro_Inf_Comp.parquet"))},
    error = function(e) { arrow::write_parquet(
      get(paste0("dados_", uf, "_Gastro_Inf_Comp")) %>% as.data.frame(),
      paste0("D:/NESCON/internacoes-sih-sus/Dados por CID/Gastroenterites Infecciosas e complicações/dados_", uf, "_Gastro_Inf_Comp.parquet")) })
}
dados_Gastro_Inf_Comp = rbind(dados_AC_Gastro_Inf_Comp,dados_AL_Gastro_Inf_Comp,dados_AM_Gastro_Inf_Comp,dados_AP_Gastro_Inf_Comp,
                              dados_BA_Gastro_Inf_Comp,dados_CE_Gastro_Inf_Comp,dados_DF_Gastro_Inf_Comp,dados_ES_Gastro_Inf_Comp,
                              dados_GO_Gastro_Inf_Comp,dados_MA_Gastro_Inf_Comp,dados_MG_Gastro_Inf_Comp,dados_MS_Gastro_Inf_Comp,
                              dados_MT_Gastro_Inf_Comp,dados_PA_Gastro_Inf_Comp,dados_PB_Gastro_Inf_Comp,dados_PE_Gastro_Inf_Comp,
                              dados_PI_Gastro_Inf_Comp,dados_PR_Gastro_Inf_Comp,dados_RJ_Gastro_Inf_Comp,dados_RN_Gastro_Inf_Comp,
                              dados_RO_Gastro_Inf_Comp,dados_RR_Gastro_Inf_Comp,dados_RS_Gastro_Inf_Comp,dados_SC_Gastro_Inf_Comp,
                              dados_SE_Gastro_Inf_Comp,dados_SP_Gastro_Inf_Comp,dados_TO_Gastro_Inf_Comp)
tryCatch({arrow::write_parquet(dados_Gastro_Inf_Comp %>% as.data.frame(), "C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Gastroenterites Infecciosas e complicações/dados_Gastro_Inf_Comp.parquet")},
         error = function(e) { arrow::write_parquet(dados_Gastro_Inf_Comp %>% as.data.frame(), 'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Gastroenterites Infecciosas e complicações/dados_Gastro_Inf_Comp.parquet') })
tryCatch({write.xlsx(dados_Gastro_Inf_Comp %>% as.data.frame(), "C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Gastroenterites Infecciosas e complicações/dados_Gastro_Inf_Comp.xlsx")},
         error = function(e) { write.xlsx(dados_Gastro_Inf_Comp %>% as.data.frame(), "D:/NESCON/internacoes-sih-sus/Dados por CID/Gastroenterites Infecciosas e complicações/dados_Gastro_Inf_Comp.xlsx") })

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
estados = c("AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT","PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO")
for (uf in estados) {
  tryCatch({arrow::write_parquet(
    get(paste0("dados_", uf, "_Hipertensao")) %>% as.data.frame(),
    paste0("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Hipertensão/dados_", uf, "_Hipertensao.parquet"))},
    error = function(e) { arrow::write_parquet(
      get(paste0("dados_", uf, "_Hipertensao")) %>% as.data.frame(),
      paste0("D:/NESCON/internacoes-sih-sus/Dados por CID/Hipertensão/dados_", uf, "_Hipertensao.parquet")) })
}
dados_Hipertensao = rbind(dados_AC_Hipertensao,dados_AL_Hipertensao,dados_AM_Hipertensao,dados_AP_Hipertensao,
                          dados_BA_Hipertensao,dados_CE_Hipertensao,dados_DF_Hipertensao,dados_ES_Hipertensao,
                          dados_GO_Hipertensao,dados_MA_Hipertensao,dados_MG_Hipertensao,dados_MS_Hipertensao,
                          dados_MT_Hipertensao,dados_PA_Hipertensao,dados_PB_Hipertensao,dados_PE_Hipertensao,
                          dados_PI_Hipertensao,dados_PR_Hipertensao,dados_RJ_Hipertensao,dados_RN_Hipertensao,
                          dados_RO_Hipertensao,dados_RR_Hipertensao,dados_RS_Hipertensao,dados_SC_Hipertensao,
                          dados_SE_Hipertensao,dados_SP_Hipertensao,dados_TO_Hipertensao)
tryCatch({arrow::write_parquet(dados_Hipertensao %>% as.data.frame(), "C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Hipertensão/dados_Hipertensao.parquet")},
         error = function(e) { arrow::write_parquet(dados_Hipertensao %>% as.data.frame(), 'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Hipertensão/dados_Hipertensao.parquet') })
tryCatch({write.xlsx(dados_Hipertensao %>% as.data.frame(), "C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Hipertensão/dados_Hipertensao.xlsx")},
         error = function(e) { write.xlsx(dados_Hipertensao %>% as.data.frame(), "D:/NESCON/internacoes-sih-sus/Dados por CID/Hipertensão/dados_Hipertensao.xlsx") })

####======================================
#### Infecção da pele e tecido subcutâneo
####======================================
dados_AC_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_AC, c("L01","L02","L03","L04"))
dados_AL_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_AL, c("L01","L02","L03","L04"))
dados_AM_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_AM, c("L01","L02","L03","L04"))
dados_AP_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_AP, c("L01","L02","L03","L04"))
dados_BA_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_BA, c("L01","L02","L03","L04"))
dados_CE_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_CE, c("L01","L02","L03","L04"))
dados_DF_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_DF, c("L01","L02","L03","L04"))
dados_ES_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_ES, c("L01","L02","L03","L04"))
dados_GO_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_GO, c("L01","L02","L03","L04"))
dados_MA_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_MA, c("L01","L02","L03","L04"))
dados_MG_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_MG, c("L01","L02","L03","L04"))
dados_MS_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_MS, c("L01","L02","L03","L04"))
dados_MT_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_MT, c("L01","L02","L03","L04"))
dados_PA_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_PA, c("L01","L02","L03","L04"))
dados_PB_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_PB, c("L01","L02","L03","L04"))
dados_PE_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_PE, c("L01","L02","L03","L04"))
dados_PI_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_PI, c("L01","L02","L03","L04"))
dados_PR_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_PR, c("L01","L02","L03","L04"))
dados_RJ_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_RJ, c("L01","L02","L03","L04"))
dados_RN_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_RN, c("L01","L02","L03","L04"))
dados_RO_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_RO, c("L01","L02","L03","L04"))
dados_RR_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_RR, c("L01","L02","L03","L04"))
dados_RS_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_RS, c("L01","L02","L03","L04"))
dados_SC_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_SC, c("L01","L02","L03","L04"))
dados_SE_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_SE, c("L01","L02","L03","L04"))
dados_SP_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_SP, c("L01","L02","L03","L04"))
dados_TO_Inf_Pele_Tec_Sub = FiltraDadosCID(dados_TO, c("L01","L02","L03","L04"))
estados = c("AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT","PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO")
for (uf in estados) {
  tryCatch({arrow::write_parquet(
    get(paste0("dados_", uf, "_Inf_Pele_Tec_Sub")) %>% as.data.frame(),
    paste0("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Infecção da pele e tecido subcutâneo/dados_", uf, "_Inf_Pele_Tec_Sub.parquet"))},
    error = function(e) { arrow::write_parquet(
      get(paste0("dados_", uf, "_Inf_Pele_Tec_Sub")) %>% as.data.frame(),
      paste0("D:/NESCON/internacoes-sih-sus/Dados por CID/Infecção da pele e tecido subcutâneo/dados_", uf, "_Inf_Pele_Tec_Sub.parquet")) })
}
dados_Inf_Pele_Tec_Sub = rbind(dados_AC_Inf_Pele_Tec_Sub,dados_AL_Inf_Pele_Tec_Sub,dados_AM_Inf_Pele_Tec_Sub,dados_AP_Inf_Pele_Tec_Sub,
                               dados_BA_Inf_Pele_Tec_Sub,dados_CE_Inf_Pele_Tec_Sub,dados_DF_Inf_Pele_Tec_Sub,dados_ES_Inf_Pele_Tec_Sub,
                               dados_GO_Inf_Pele_Tec_Sub,dados_MA_Inf_Pele_Tec_Sub,dados_MG_Inf_Pele_Tec_Sub,dados_MS_Inf_Pele_Tec_Sub,
                               dados_MT_Inf_Pele_Tec_Sub,dados_PA_Inf_Pele_Tec_Sub,dados_PB_Inf_Pele_Tec_Sub,dados_PE_Inf_Pele_Tec_Sub,
                               dados_PI_Inf_Pele_Tec_Sub,dados_PR_Inf_Pele_Tec_Sub,dados_RJ_Inf_Pele_Tec_Sub,dados_RN_Inf_Pele_Tec_Sub,
                               dados_RO_Inf_Pele_Tec_Sub,dados_RR_Inf_Pele_Tec_Sub,dados_RS_Inf_Pele_Tec_Sub,dados_SC_Inf_Pele_Tec_Sub,
                               dados_SE_Inf_Pele_Tec_Sub,dados_SP_Inf_Pele_Tec_Sub,dados_TO_Inf_Pele_Tec_Sub)
tryCatch({arrow::write_parquet(dados_Inf_Pele_Tec_Sub %>% as.data.frame(), "C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Infecção da pele e tecido subcutâneo/dados_Inf_Pele_Tec_Sub.parquet")},
         error = function(e) { arrow::write_parquet(dados_Inf_Pele_Tec_Sub %>% as.data.frame(), 'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção da pele e tecido subcutâneo/dados_Inf_Pele_Tec_Sub.parquet') })
tryCatch({write.xlsx(dados_Inf_Pele_Tec_Sub %>% as.data.frame(), "C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Infecção da pele e tecido subcutâneo/dados_Inf_Pele_Tec_Sub.xlsx")},
         error = function(e) { write.xlsx(dados_Inf_Pele_Tec_Sub %>% as.data.frame(), "D:/NESCON/internacoes-sih-sus/Dados por CID/Infecção da pele e tecido subcutâneo/dados_Inf_Pele_Tec_Sub.xlsx") })

####=====================================
#### Infecção no Rim e no Trato Urinário
####=====================================
dados_AC_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_AC, c("N10","N11","N12","N39"))
dados_AL_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_AL, c("N10","N11","N12","N39"))
dados_AM_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_AM, c("N10","N11","N12","N39"))
dados_AP_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_AP, c("N10","N11","N12","N39"))
dados_BA_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_BA, c("N10","N11","N12","N39"))
dados_CE_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_CE, c("N10","N11","N12","N39"))
dados_DF_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_DF, c("N10","N11","N12","N39"))
dados_ES_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_ES, c("N10","N11","N12","N39"))
dados_GO_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_GO, c("N10","N11","N12","N39"))
dados_MA_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_MA, c("N10","N11","N12","N39"))
dados_MG_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_MG, c("N10","N11","N12","N39"))
dados_MS_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_MS, c("N10","N11","N12","N39"))
dados_MT_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_MT, c("N10","N11","N12","N39"))
dados_PA_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_PA, c("N10","N11","N12","N39"))
dados_PB_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_PB, c("N10","N11","N12","N39"))
dados_PE_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_PE, c("N10","N11","N12","N39"))
dados_PI_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_PI, c("N10","N11","N12","N39"))
dados_PR_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_PR, c("N10","N11","N12","N39"))
dados_RJ_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_RJ, c("N10","N11","N12","N39"))
dados_RN_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_RN, c("N10","N11","N12","N39"))
dados_RO_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_RO, c("N10","N11","N12","N39"))
dados_RR_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_RR, c("N10","N11","N12","N39"))
dados_RS_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_RS, c("N10","N11","N12","N39"))
dados_SC_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_SC, c("N10","N11","N12","N39"))
dados_SE_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_SE, c("N10","N11","N12","N39"))
dados_SP_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_SP, c("N10","N11","N12","N39"))
dados_TO_Inf_Rim_Tr_Urin = FiltraDadosCID(dados_TO, c("N10","N11","N12","N39"))
estados = c("AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT","PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO")
for (uf in estados) {
  tryCatch({arrow::write_parquet(
    get(paste0("dados_", uf, "_Inf_Rim_Tr_Urin")) %>% as.data.frame(),
    paste0("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Infecção no Rim e no Trato Urinário/dados_", uf, "_Inf_Rim_Tr_Urin.parquet"))},
    error = function(e) { arrow::write_parquet(
      get(paste0("dados_", uf, "_Inf_Rim_Tr_Urin")) %>% as.data.frame(),
      paste0("D:/NESCON/internacoes-sih-sus/Dados por CID/Infecção no Rim e no Trato Urinário/dados_", uf, "_Inf_Rim_Tr_Urin.parquet")) })
}
dados_Inf_Rim_Tr_Urin = rbind(dados_AC_Inf_Rim_Tr_Urin,dados_AL_Inf_Rim_Tr_Urin,dados_AM_Inf_Rim_Tr_Urin,dados_AP_Inf_Rim_Tr_Urin,
                              dados_BA_Inf_Rim_Tr_Urin,dados_CE_Inf_Rim_Tr_Urin,dados_DF_Inf_Rim_Tr_Urin,dados_ES_Inf_Rim_Tr_Urin,
                              dados_GO_Inf_Rim_Tr_Urin,dados_MA_Inf_Rim_Tr_Urin,dados_MG_Inf_Rim_Tr_Urin,dados_MS_Inf_Rim_Tr_Urin,
                              dados_MT_Inf_Rim_Tr_Urin,dados_PA_Inf_Rim_Tr_Urin,dados_PB_Inf_Rim_Tr_Urin,dados_PE_Inf_Rim_Tr_Urin,
                              dados_PI_Inf_Rim_Tr_Urin,dados_PR_Inf_Rim_Tr_Urin,dados_RJ_Inf_Rim_Tr_Urin,dados_RN_Inf_Rim_Tr_Urin,
                              dados_RO_Inf_Rim_Tr_Urin,dados_RR_Inf_Rim_Tr_Urin,dados_RS_Inf_Rim_Tr_Urin,dados_SC_Inf_Rim_Tr_Urin,
                              dados_SE_Inf_Rim_Tr_Urin,dados_SP_Inf_Rim_Tr_Urin,dados_TO_Inf_Rim_Tr_Urin)
tryCatch({arrow::write_parquet(dados_Inf_Rim_Tr_Urin %>% as.data.frame(), "C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Infecção no Rim e no Trato Urinário/dados_Inf_Rim_Tr_Urin.parquet")},
         error = function(e) { arrow::write_parquet(dados_Inf_Rim_Tr_Urin %>% as.data.frame(), 'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecção no Rim e no Trato Urinário/dados_Inf_Rim_Tr_Urin.parquet') })
tryCatch({write.xlsx(dados_Inf_Rim_Tr_Urin %>% as.data.frame(), "C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Infecção no Rim e no Trato Urinário/dados_Inf_Rim_Tr_Urin.xlsx")},
         error = function(e) { write.xlsx(dados_Inf_Rim_Tr_Urin %>% as.data.frame(), "D:/NESCON/internacoes-sih-sus/Dados por CID/Infecção no Rim e no Trato Urinário/dados_Inf_Rim_Tr_Urin.xlsx") })

####=======================================
#### Infecções de ouvido, nariz e garganta
####=======================================
dados_AC_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_AC, c("J03", "J02", "J06", "J00", "H66", "J31", "J01"))
dados_AL_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_AL, c("J03", "J02", "J06", "J00", "H66", "J31", "J01"))
dados_AM_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_AM, c("J03", "J02", "J06", "J00", "H66", "J31", "J01"))
dados_AP_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_AP, c("J03", "J02", "J06", "J00", "H66", "J31", "J01"))
dados_BA_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_BA, c("J03", "J02", "J06", "J00", "H66", "J31", "J01"))
dados_CE_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_CE, c("J03", "J02", "J06", "J00", "H66", "J31", "J01"))
dados_DF_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_DF, c("J03", "J02", "J06", "J00", "H66", "J31", "J01"))
dados_ES_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_ES, c("J03", "J02", "J06", "J00", "H66", "J31", "J01"))
dados_GO_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_GO, c("J03", "J02", "J06", "J00", "H66", "J31", "J01"))
dados_MA_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_MA, c("J03", "J02", "J06", "J00", "H66", "J31", "J01"))
dados_MG_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_MG, c("J03", "J02", "J06", "J00", "H66", "J31", "J01"))
dados_MS_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_MS, c("J03", "J02", "J06", "J00", "H66", "J31", "J01"))
dados_MT_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_MT, c("J03", "J02", "J06", "J00", "H66", "J31", "J01"))
dados_PA_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_PA, c("J03", "J02", "J06", "J00", "H66", "J31", "J01"))
dados_PB_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_PB, c("J03", "J02", "J06", "J00", "H66", "J31", "J01"))
dados_PE_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_PE, c("J03", "J02", "J06", "J00", "H66", "J31", "J01"))
dados_PI_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_PI, c("J03", "J02", "J06", "J00", "H66", "J31", "J01"))
dados_PR_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_PR, c("J03", "J02", "J06", "J00", "H66", "J31", "J01"))
dados_RJ_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_RJ, c("J03", "J02", "J06", "J00", "H66", "J31", "J01"))
dados_RN_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_RN, c("J03", "J02", "J06", "J00", "H66", "J31", "J01"))
dados_RO_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_RO, c("J03", "J02", "J06", "J00", "H66", "J31", "J01"))
dados_RR_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_RR, c("J03", "J02", "J06", "J00", "H66", "J31", "J01"))
dados_RS_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_RS, c("J03", "J02", "J06", "J00", "H66", "J31", "J01"))
dados_SC_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_SC, c("J03", "J02", "J06", "J00", "H66", "J31", "J01"))
dados_SE_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_SE, c("J03", "J02", "J06", "J00", "H66", "J31", "J01"))
dados_SP_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_SP, c("J03", "J02", "J06", "J00", "H66", "J31", "J01"))
dados_TO_Inf_Ouv_Nariz_Garg = FiltraDadosCID(dados_TO, c("J03", "J02", "J06", "J00", "H66", "J31", "J01"))
estados = c("AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT","PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO")
for (uf in estados) {
  tryCatch({arrow::write_parquet(
    get(paste0("dados_", uf, "_Inf_Ouv_Nariz_Garg")) %>% as.data.frame(),
    paste0("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Infecções de ouvido, nariz e garganta/dados_", uf, "_Inf_Ouv_Nariz_Garg.parquet"))},
    error = function(e) { arrow::write_parquet(
      get(paste0("dados_", uf, "_Inf_Ouv_Nariz_Garg")) %>% as.data.frame(),
      paste0("D:/NESCON/internacoes-sih-sus/Dados por CID/Infecções de ouvido, nariz e garganta/dados_", uf, "_Inf_Ouv_Nariz_Garg.parquet")) })
}
dados_Inf_Ouv_Nariz_Garg = rbind(dados_AC_Inf_Ouv_Nariz_Garg,dados_AL_Inf_Ouv_Nariz_Garg,dados_AM_Inf_Ouv_Nariz_Garg,dados_AP_Inf_Ouv_Nariz_Garg,
                                 dados_BA_Inf_Ouv_Nariz_Garg,dados_CE_Inf_Ouv_Nariz_Garg,dados_DF_Inf_Ouv_Nariz_Garg,dados_ES_Inf_Ouv_Nariz_Garg,
                                 dados_GO_Inf_Ouv_Nariz_Garg,dados_MA_Inf_Ouv_Nariz_Garg,dados_MG_Inf_Ouv_Nariz_Garg,dados_MS_Inf_Ouv_Nariz_Garg,
                                 dados_MT_Inf_Ouv_Nariz_Garg,dados_PA_Inf_Ouv_Nariz_Garg,dados_PB_Inf_Ouv_Nariz_Garg,dados_PE_Inf_Ouv_Nariz_Garg,
                                 dados_PI_Inf_Ouv_Nariz_Garg,dados_PR_Inf_Ouv_Nariz_Garg,dados_RJ_Inf_Ouv_Nariz_Garg,dados_RN_Inf_Ouv_Nariz_Garg,
                                 dados_RO_Inf_Ouv_Nariz_Garg,dados_RR_Inf_Ouv_Nariz_Garg,dados_RS_Inf_Ouv_Nariz_Garg,dados_SC_Inf_Ouv_Nariz_Garg,
                                 dados_SE_Inf_Ouv_Nariz_Garg,dados_SP_Inf_Ouv_Nariz_Garg,dados_TO_Inf_Ouv_Nariz_Garg)
tryCatch({arrow::write_parquet(dados_Inf_Ouv_Nariz_Garg %>% as.data.frame(), "C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Infecções de ouvido, nariz e garganta/dados_Inf_Ouv_Nariz_Garg.parquet")},
         error = function(e) { arrow::write_parquet(dados_Inf_Ouv_Nariz_Garg %>% as.data.frame(), 'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Infecções de ouvido, nariz e garganta/dados_Inf_Ouv_Nariz_Garg.parquet') })
tryCatch({write.xlsx(dados_Inf_Ouv_Nariz_Garg %>% as.data.frame(), "C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Infecções de ouvido, nariz e garganta/dados_Inf_Ouv_Nariz_Garg.xlsx")},
         error = function(e) { write.xlsx(dados_Inf_Ouv_Nariz_Garg %>% as.data.frame(), "D:/NESCON/internacoes-sih-sus/Dados por CID/Infecções de ouvido, nariz e garganta/dados_Inf_Ouv_Nariz_Garg.xlsx") })

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
estados = c("AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT","PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO")
for (uf in estados) {
  tryCatch({arrow::write_parquet(
    get(paste0("dados_", uf, "_Insuf_Card")) %>% as.data.frame(),
    paste0("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Insuficiência Cardíaca/dados_", uf, "_Insuf_Card.parquet"))},
    error = function(e) { arrow::write_parquet(
      get(paste0("dados_", uf, "_Insuf_Card")) %>% as.data.frame(),
      paste0("D:/NESCON/internacoes-sih-sus/Dados por CID/Insuficiência Cardíaca/dados_", uf, "_Insuf_Card.parquet")) })
}
dados_Insuf_Card = rbind(dados_AC_Insuf_Card,dados_AL_Insuf_Card,dados_AM_Insuf_Card,dados_AP_Insuf_Card,
                         dados_BA_Insuf_Card,dados_CE_Insuf_Card,dados_DF_Insuf_Card,dados_ES_Insuf_Card,
                         dados_GO_Insuf_Card,dados_MA_Insuf_Card,dados_MG_Insuf_Card,dados_MS_Insuf_Card,
                         dados_MT_Insuf_Card,dados_PA_Insuf_Card,dados_PB_Insuf_Card,dados_PE_Insuf_Card,
                         dados_PI_Insuf_Card,dados_PR_Insuf_Card,dados_RJ_Insuf_Card,dados_RN_Insuf_Card,
                         dados_RO_Insuf_Card,dados_RR_Insuf_Card,dados_RS_Insuf_Card,dados_SC_Insuf_Card,
                         dados_SE_Insuf_Card,dados_SP_Insuf_Card,dados_TO_Insuf_Card)
tryCatch({arrow::write_parquet(dados_Insuf_Card %>% as.data.frame(), "C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Insuficiência Cardíaca/dados_Insuf_Card.parquet")},
         error = function(e) { arrow::write_parquet(dados_Insuf_Card %>% as.data.frame(), 'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Insuficiência Cardíaca/dados_Insuf_Card.parquet') })
tryCatch({write.xlsx(dados_Insuf_Card %>% as.data.frame(), "C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Insuficiência Cardíaca/dados_Insuf_Card.xlsx")},
         error = function(e) { write.xlsx(dados_Insuf_Card %>% as.data.frame(), "D:/NESCON/internacoes-sih-sus/Dados por CID/Insuficiência Cardíaca/dados_Insuf_Card.xlsx") })

####============
#### Pneumonias
####============
dados_AC_Pneumonias = FiltraDadosCID(dados_AC, c("J15", "J18", "J14", "J13"))
dados_AL_Pneumonias = FiltraDadosCID(dados_AL, c("J15", "J18", "J14", "J13"))
dados_AM_Pneumonias = FiltraDadosCID(dados_AM, c("J15", "J18", "J14", "J13"))
dados_AP_Pneumonias = FiltraDadosCID(dados_AP, c("J15", "J18", "J14", "J13"))
dados_BA_Pneumonias = FiltraDadosCID(dados_BA, c("J15", "J18", "J14", "J13"))
dados_CE_Pneumonias = FiltraDadosCID(dados_CE, c("J15", "J18", "J14", "J13"))
dados_DF_Pneumonias = FiltraDadosCID(dados_DF, c("J15", "J18", "J14", "J13"))
dados_ES_Pneumonias = FiltraDadosCID(dados_ES, c("J15", "J18", "J14", "J13"))
dados_GO_Pneumonias = FiltraDadosCID(dados_GO, c("J15", "J18", "J14", "J13"))
dados_MA_Pneumonias = FiltraDadosCID(dados_MA, c("J15", "J18", "J14", "J13"))
dados_MG_Pneumonias = FiltraDadosCID(dados_MG, c("J15", "J18", "J14", "J13"))
dados_MS_Pneumonias = FiltraDadosCID(dados_MS, c("J15", "J18", "J14", "J13"))
dados_MT_Pneumonias = FiltraDadosCID(dados_MT, c("J15", "J18", "J14", "J13"))
dados_PA_Pneumonias = FiltraDadosCID(dados_PA, c("J15", "J18", "J14", "J13"))
dados_PB_Pneumonias = FiltraDadosCID(dados_PB, c("J15", "J18", "J14", "J13"))
dados_PE_Pneumonias = FiltraDadosCID(dados_PE, c("J15", "J18", "J14", "J13"))
dados_PI_Pneumonias = FiltraDadosCID(dados_PI, c("J15", "J18", "J14", "J13"))
dados_PR_Pneumonias = FiltraDadosCID(dados_PR, c("J15", "J18", "J14", "J13"))
dados_RJ_Pneumonias = FiltraDadosCID(dados_RJ, c("J15", "J18", "J14", "J13"))
dados_RN_Pneumonias = FiltraDadosCID(dados_RN, c("J15", "J18", "J14", "J13"))
dados_RO_Pneumonias = FiltraDadosCID(dados_RO, c("J15", "J18", "J14", "J13"))
dados_RR_Pneumonias = FiltraDadosCID(dados_RR, c("J15", "J18", "J14", "J13"))
dados_RS_Pneumonias = FiltraDadosCID(dados_RS, c("J15", "J18", "J14", "J13"))
dados_SC_Pneumonias = FiltraDadosCID(dados_SC, c("J15", "J18", "J14", "J13"))
dados_SE_Pneumonias = FiltraDadosCID(dados_SE, c("J15", "J18", "J14", "J13"))
dados_SP_Pneumonias = FiltraDadosCID(dados_SP, c("J15", "J18", "J14", "J13"))
dados_TO_Pneumonias = FiltraDadosCID(dados_TO, c("J15", "J18", "J14", "J13"))
estados = c("AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT","PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO")
for (uf in estados) {
  tryCatch({arrow::write_parquet(
    get(paste0("dados_", uf, "_Pneumonias")) %>% as.data.frame(),
    paste0("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Pneumonias/dados_", uf, "_Pneumonias.parquet"))},
    error = function(e) { arrow::write_parquet(
      get(paste0("dados_", uf, "_Pneumonias")) %>% as.data.frame(),
      paste0("D:/NESCON/internacoes-sih-sus/Dados por CID/Pneumonias/dados_", uf, "_Pneumonias.parquet")) })
}
dados_Pneumonias = rbind(dados_AC_Pneumonias,dados_AL_Pneumonias,dados_AM_Pneumonias,dados_AP_Pneumonias,
                         dados_BA_Pneumonias,dados_CE_Pneumonias,dados_DF_Pneumonias,dados_ES_Pneumonias,
                         dados_GO_Pneumonias,dados_MA_Pneumonias,dados_MG_Pneumonias,dados_MS_Pneumonias,
                         dados_MT_Pneumonias,dados_PA_Pneumonias,dados_PB_Pneumonias,dados_PE_Pneumonias,
                         dados_PI_Pneumonias,dados_PR_Pneumonias,dados_RJ_Pneumonias,dados_RN_Pneumonias,
                         dados_RO_Pneumonias,dados_RR_Pneumonias,dados_RS_Pneumonias,dados_SC_Pneumonias,
                         dados_SE_Pneumonias,dados_SP_Pneumonias,dados_TO_Pneumonias)
tryCatch({arrow::write_parquet(dados_Pneumonias %>% as.data.frame(), "C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Pneumonias/dados_Pneumonias.parquet")},
         error = function(e) { arrow::write_parquet(dados_Pneumonias %>% as.data.frame(), 'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Pneumonias/dados_Pneumonias.parquet') })
tryCatch({write.xlsx(dados_Pneumonias %>% as.data.frame(), "C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Pneumonias/dados_Pneumonias.xlsx")},
         error = function(e) { write.xlsx(dados_Pneumonias %>% as.data.frame(), "D:/NESCON/internacoes-sih-sus/Dados por CID/Pneumonias/dados_Pneumonias.xlsx") })

####========================================================
#### Úlcera gastrointestinal com hemorragia e/ou perfuração
####========================================================
dados_AC_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_AC, c("K25","K26","K27","K28","K92"))
dados_AL_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_AL, c("K25","K26","K27","K28","K92"))
dados_AM_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_AM, c("K25","K26","K27","K28","K92"))
dados_AP_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_AP, c("K25","K26","K27","K28","K92"))
dados_BA_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_BA, c("K25","K26","K27","K28","K92"))
dados_CE_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_CE, c("K25","K26","K27","K28","K92"))
dados_DF_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_DF, c("K25","K26","K27","K28","K92"))
dados_ES_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_ES, c("K25","K26","K27","K28","K92"))
dados_GO_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_GO, c("K25","K26","K27","K28","K92"))
dados_MA_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_MA, c("K25","K26","K27","K28","K92"))
dados_MG_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_MG, c("K25","K26","K27","K28","K92"))
dados_MS_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_MS, c("K25","K26","K27","K28","K92"))
dados_MT_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_MT, c("K25","K26","K27","K28","K92"))
dados_PA_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_PA, c("K25","K26","K27","K28","K92"))
dados_PB_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_PB, c("K25","K26","K27","K28","K92"))
dados_PE_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_PE, c("K25","K26","K27","K28","K92"))
dados_PI_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_PI, c("K25","K26","K27","K28","K92"))
dados_PR_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_PR, c("K25","K26","K27","K28","K92"))
dados_RJ_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_RJ, c("K25","K26","K27","K28","K92"))
dados_RN_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_RN, c("K25","K26","K27","K28","K92"))
dados_RO_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_RO, c("K25","K26","K27","K28","K92"))
dados_RR_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_RR, c("K25","K26","K27","K28","K92"))
dados_RS_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_RS, c("K25","K26","K27","K28","K92"))
dados_SC_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_SC, c("K25","K26","K27","K28","K92"))
dados_SE_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_SE, c("K25","K26","K27","K28","K92"))
dados_SP_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_SP, c("K25","K26","K27","K28","K92"))
dados_TO_Ulc_Gastro_Hem_Perf = FiltraDadosCID(dados_TO, c("K25","K26","K27","K28","K92"))
estados = c("AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT","PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO")
for (uf in estados) {
  tryCatch({arrow::write_parquet(
    get(paste0("dados_", uf, "_Ulc_Gastro_Hem_Perf")) %>% as.data.frame(),
    paste0("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Úlcera gastrointestinal com hemorragia eou perfuração/dados_", uf, "_Ulc_Gastro_Hem_Perf.parquet"))},
    error = function(e) { arrow::write_parquet(
      get(paste0("dados_", uf, "_Ulc_Gastro_Hem_Perf")) %>% as.data.frame(),
      paste0("D:/NESCON/internacoes-sih-sus/Dados por CID/Úlcera gastrointestinal com hemorragia eou perfuração/dados_", uf, "_Ulc_Gastro_Hem_Perf.parquet")) })
}
dados_Ulc_Gastro_Hem_Perf = rbind(dados_AC_Ulc_Gastro_Hem_Perf,dados_AL_Ulc_Gastro_Hem_Perf,dados_AM_Ulc_Gastro_Hem_Perf,dados_AP_Ulc_Gastro_Hem_Perf,
                                  dados_BA_Ulc_Gastro_Hem_Perf,dados_CE_Ulc_Gastro_Hem_Perf,dados_DF_Ulc_Gastro_Hem_Perf,dados_ES_Ulc_Gastro_Hem_Perf,
                                  dados_GO_Ulc_Gastro_Hem_Perf,dados_MA_Ulc_Gastro_Hem_Perf,dados_MG_Ulc_Gastro_Hem_Perf,dados_MS_Ulc_Gastro_Hem_Perf,
                                  dados_MT_Ulc_Gastro_Hem_Perf,dados_PA_Ulc_Gastro_Hem_Perf,dados_PB_Ulc_Gastro_Hem_Perf,dados_PE_Ulc_Gastro_Hem_Perf,
                                  dados_PI_Ulc_Gastro_Hem_Perf,dados_PR_Ulc_Gastro_Hem_Perf,dados_RJ_Ulc_Gastro_Hem_Perf,dados_RN_Ulc_Gastro_Hem_Perf,
                                  dados_RO_Ulc_Gastro_Hem_Perf,dados_RR_Ulc_Gastro_Hem_Perf,dados_RS_Ulc_Gastro_Hem_Perf,dados_SC_Ulc_Gastro_Hem_Perf,
                                  dados_SE_Ulc_Gastro_Hem_Perf,dados_SP_Ulc_Gastro_Hem_Perf,dados_TO_Ulc_Gastro_Hem_Perf)
tryCatch({arrow::write_parquet(dados_Ulc_Gastro_Hem_Perf %>% as.data.frame(), "C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Úlcera gastrointestinal com hemorragia eou perfuração/dados_Ulc_Gastro_Hem_Perf.parquet")},
         error = function(e) { arrow::write_parquet(dados_Ulc_Gastro_Hem_Perf %>% as.data.frame(), 'D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/Dados por CID/Úlcera gastrointestinal com hemorragia eou perfuração/dados_Ulc_Gastro_Hem_Perf.parquet') })
tryCatch({write.xlsx(dados_Ulc_Gastro_Hem_Perf %>% as.data.frame(), "C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Úlcera gastrointestinal com hemorragia eou perfuração/dados_Ulc_Gastro_Hem_Perf.xlsx")},
         error = function(e) { write.xlsx(dados_Ulc_Gastro_Hem_Perf %>% as.data.frame(), "D:/NESCON/internacoes-sih-sus/Dados por CID/Úlcera gastrointestinal com hemorragia eou perfuração/dados_Ulc_Gastro_Hem_Perf.xlsx") })

####===============================
#### Carregando os bancos de dados 
####===============================
dados_Anemia = tryCatch({read.xlsx("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Anemia/dados_Anemia.xlsx")},
                        error = function(e) {read.xlsx("D:/NESCON/internacoes-sih-sus/Dados por CID/Anemia/dados_Anemia.xlsx") })
dados_Angina = tryCatch({read.xlsx("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Angina/dados_Angina.xlsx")},
                        error = function(e) {read.xlsx("D:/NESCON/internacoes-sih-sus/Dados por CID/Angina/dados_Angina.xlsx") })
dados_Asma = tryCatch({read.xlsx("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Asma/dados_Asma.xlsx")},
                      error = function(e) {read.xlsx("D:/NESCON/internacoes-sih-sus/Dados por CID/Asma/dados_Asma.xlsx") })
dados_Condicoes_Evitaveis = tryCatch({read.xlsx("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Condições evitáveis/dados_Condicoes_Evitaveis.xlsx")},
                                     error = function(e) {read.xlsx("D:/NESCON/internacoes-sih-sus/Dados por CID/Condições evitáveis/dados_Condicoes_Evitaveis.xlsx") })
dados_Def_nut = tryCatch({read.xlsx("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Deficiências nutricionais/dados_Def_nut.xlsx")},
                         error = function(e) {read.xlsx("D:/NESCON/internacoes-sih-sus/Dados por CID/Deficiências nutricionais/dados_Def_nut.xlsx") })
dados_Diabetes = tryCatch({read.xlsx("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Diabetes/dados_Diabetes.xlsx")},
                          error = function(e) {read.xlsx("D:/NESCON/internacoes-sih-sus/Dados por CID/Diabetes/dados_Diabetes.xlsx") })
dados_D_Inf_Org_Pelv_Fem = tryCatch({read.xlsx("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Doença Inflamatória órgãos pélvicos femininos/dados_D_Inf_Org_Pelv_Fem.xlsx")},
                                    error = function(e) {read.xlsx("D:/NESCON/internacoes-sih-sus/Dados por CID/Doença Inflamatória órgãos pélvicos femininos/dados_D_Inf_Org_Pelv_Fem.xlsx") })
dados_D_Cerebrovasc = tryCatch({read.xlsx("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Doenças Cerebro-vasculares/dados_D_Cerebrovasc.xlsx")},
                               error = function(e) {read.xlsx("D:/NESCON/internacoes-sih-sus/Dados por CID/Doenças Cerebro-vasculares/dados_D_Cerebrovasc.xlsx") })
dados_D_Vias_Aereas_Inf = tryCatch({read.xlsx("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Doenças das vias aéreas inferiores/dados_D_Vias_Aereas_Inf.xlsx")},
                                error = function(e) {read.xlsx("D:/NESCON/internacoes-sih-sus/Dados por CID/Doenças das vias aéreas inferiores/dados_D_Vias_Aereas_Inf.xlsx") })
dados_D_Imunizaveis = tryCatch({read.xlsx("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Doenças imunizáveis/dados_D_Imunizaveis.xlsx")},
                               error = function(e) {read.xlsx("D:/NESCON/internacoes-sih-sus/Dados por CID/Doenças imunizáveis/dados_D_Imunizaveis.xlsx") })
dados_D_Rel_PreNat_Parto = tryCatch({read.xlsx("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Doenças relacionadas ao Pré-Natal e Parto/dados_D_Rel_PreNat_Parto.xlsx")},
                                    error = function(e) {read.xlsx("D:/NESCON/internacoes-sih-sus/Dados por CID/Doenças relacionadas ao Pré-Natal e Parto/dados_D_Rel_PreNat_Parto.xlsx") })
dados_Epilepsias = tryCatch({read.xlsx("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Epilepsias/dados_Epilepsias.xlsx")},
                            error = function(e) {read.xlsx("D:/NESCON/internacoes-sih-sus/Dados por CID/Epilepsias/dados_Epilepsias.xlsx") })
dados_Gastro_Inf_Comp = tryCatch({read.xlsx("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Gastroenterites Infecciosas e complicações/dados_Gastro_Inf_Comp.xlsx")},
                                 error = function(e) {read.xlsx("D:/NESCON/internacoes-sih-sus/Dados por CID/Gastroenterites Infecciosas e complicações/dados_Gastro_Inf_Comp.xlsx") })
dados_Hipertensao = tryCatch({read.xlsx("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Hipertensão/dados_Hipertensao.xlsx")},
                             error = function(e) {read.xlsx("D:/NESCON/internacoes-sih-sus/Dados por CID/Hipertensão/dados_Hipertensao.xlsx") })
dados_Inf_Pele_Tec_Sub = tryCatch({read.xlsx("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Infecção da pele e tecido subcutâneo/dados_Inf_Pele_Tec_Sub.xlsx")},
                                  error = function(e) {read.xlsx("D:/NESCON/internacoes-sih-sus/Dados por CID/Infecção da pele e tecido subcutâneo/dados_Inf_Pele_Tec_Sub.xlsx") })
dados_Inf_Rim_Tr_Urin = tryCatch({read.xlsx("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Infecção no Rim e no Trato Urinário/dados_Inf_Rim_Tr_Urin.xlsx")},
                                 error = function(e) {read.xlsx("D:/NESCON/internacoes-sih-sus/Dados por CID/Infecção no Rim e no Trato Urinário/dados_Inf_Rim_Tr_Urin.xlsx") })
dados_Inf_Ouv_Nariz_Garg = tryCatch({read.xlsx("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Infecções de ouvido, nariz e garganta/dados_Inf_Ouv_Nariz_Garg.xlsx")},
                                    error = function(e) {read.xlsx("D:/NESCON/internacoes-sih-sus/Dados por CID/Infecções de ouvido, nariz e garganta/dados_Inf_Ouv_Nariz_Garg.xlsx") })
dados_Insuf_Card = tryCatch({read.xlsx("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Insuficiência Cardíaca/dados_Insuf_Card.xlsx")},
                            error = function(e) {read.xlsx("D:/NESCON/internacoes-sih-sus/Dados por CID/Insuficiência Cardíaca/dados_Insuf_Card.xlsx") })
dados_Pneumonias = tryCatch({read.xlsx("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Pneumonias/dados_Pneumonias.xlsx")},
                            error = function(e) {read.xlsx("D:/NESCON/internacoes-sih-sus/Dados por CID/Pneumonias/dados_Pneumonias.xlsx") })
dados_Ulc_Gastro_Hem_Perf = tryCatch({read.xlsx("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes-sih-sus/Dados por CID/Úlcera gastrointestinal com hemorragia eou perfuração/dados_Ulc_Gastro_Hem_Perf.xlsx")},
                                     error = function(e) {read.xlsx("D:/NESCON/internacoes-sih-sus/Dados por CID/Úlcera gastrointestinal com hemorragia eou perfuração/dados_Ulc_Gastro_Hem_Perf.xlsx") })

####==================
#### Junção dos dados
####==================
dados_icsap = rbind(dados_Anemia %>% mutate(CID_ICSAP = 'Anemia'),
                    dados_Angina %>% mutate(CID_ICSAP = 'Angina'),
                    dados_Asma %>% mutate(CID_ICSAP = 'Asma'),
                    dados_Condicoes_Evitaveis %>% mutate(CID_ICSAP = 'Condições evitáveis'),
                    dados_Def_nut %>% mutate(CID_ICSAP = 'Deficiências nutricionais'),
                    dados_Diabetes %>% mutate(CID_ICSAP = 'Diabetes'),
                    dados_D_Inf_Org_Pelv_Fem %>% mutate(CID_ICSAP = 'Doença Inflamatória órgãos pélvicos femininos'),
                    dados_D_Cerebrovasc %>% mutate(CID_ICSAP = 'Doenças Cerebro-vasculares'),
                    dados_D_Vias_Aereas_Inf %>% mutate(CID_ICSAP = 'Doenças das vias aéreas inferiores'),
                    dados_D_Imunizaveis %>% mutate(CID_ICSAP = 'Doenças imunizáveis'),
                    dados_D_Rel_PreNat_Parto %>% mutate(CID_ICSAP = 'Doenças relacionadas ao Pré-Natal e Parto'),
                    dados_Epilepsias %>% mutate(CID_ICSAP = 'Epilepsias'),
                    dados_Gastro_Inf_Comp %>% mutate(CID_ICSAP = 'Gastroenterites Infecciosas e complicações'),
                    dados_Hipertensao %>% mutate(CID_ICSAP = 'Hipertensão'),
                    dados_Inf_Pele_Tec_Sub %>% mutate(CID_ICSAP = 'Infecção da pele e tecido subcutâneo'),
                    dados_Inf_Rim_Tr_Urin %>% mutate(CID_ICSAP = 'Infecção no Rim e no Trato Urinário'),
                    dados_Inf_Ouv_Nariz_Garg %>% mutate(CID_ICSAP = 'Infecções de ouvido, nariz e garganta'),
                    dados_Insuf_Card %>% mutate(CID_ICSAP = 'Insuficiência Cardíaca'),
                    dados_Pneumonias %>% mutate(CID_ICSAP = 'Pneumonias'),
                    dados_Ulc_Gastro_Hem_Perf %>% mutate(CID_ICSAP = 'Úlcera gastrointestinal com hemorragia eou perfuração'))
write.xlsx(dados_icsap %>% as.data.frame(),'dados_icsap.xlsx')
arrow::write_parquet(dados_icsap %>% as.data.frame(), "dados_icsap.parquet")
