####=================================================
#### Trabalho Cecília - Construção do banco de dados
####=================================================
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
if(!require(stringi)){ install.packages("stringi"); require(stringi)}
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

####=============================
#### Carregando o banco de dados 
####=============================
load_data = function(arquivos, caminho_pasta) {
  lista_dfs = list()
  for (arquivo in arquivos) {
    df = read.dbc(arquivo) %>% select(ANO_CMPT,MES_CMPT,MUNIC_RES,SEXO,DIAG_PRINC,COD_IDADE,IDADE)
    lista_dfs = append(lista_dfs, list(df))
    nome_arquivo = gsub(".dbc", ".parquet", basename(arquivo))
    caminho_arquivo = file.path(caminho_pasta, nome_arquivo)
    arrow::write_parquet(df %>% as.data.frame(), caminho_arquivo)
  }
  df_final = bind_rows(lista_dfs)
  return(df_final)
}

caminho_pasta_AC = "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/AC"
arquivos_AC = list.files(path = caminho_pasta_AC, pattern = "*.dbc", full.names = TRUE)
dados_empilhados_AC = load_data(arquivos = arquivos_AC, caminho_pasta = caminho_pasta_AC)
arrow::write_parquet(dados_empilhados_AC %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/AC/dados_empilhados_AC.parquet")

caminho_pasta_AL = "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/AL"
arquivos_AL = list.files(path = caminho_pasta_AL, pattern = "*.dbc", full.names = TRUE)
dados_empilhados_AL = load_data(arquivos = arquivos_AL, caminho_pasta = caminho_pasta_AL)
arrow::write_parquet(dados_empilhados_AL %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/AL/dados_empilhados_AL.parquet")

caminho_pasta_AM = "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/AM"
arquivos_AM = list.files(path = caminho_pasta_AM, pattern = "*.dbc", full.names = TRUE)
dados_empilhados_AM = load_data(arquivos = arquivos_AM, caminho_pasta = caminho_pasta_AM)
arrow::write_parquet(dados_empilhados_AM %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/AM/dados_empilhados_AM.parquet")

caminho_pasta_AP = "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/AP"
arquivos_AP = list.files(path = caminho_pasta_AP, pattern = "*.dbc", full.names = TRUE)
dados_empilhados_AP = load_data(arquivos = arquivos_AP, caminho_pasta = caminho_pasta_AP)
arrow::write_parquet(dados_empilhados_AP %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/AP/dados_empilhados_AP.parquet")

caminho_pasta_BA = "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/BA"
arquivos_BA = list.files(path = caminho_pasta_BA, pattern = "*.dbc", full.names = TRUE)
dados_empilhados_BA = load_data(arquivos = arquivos_BA, caminho_pasta = caminho_pasta_BA)
arrow::write_parquet(dados_empilhados_BA %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/BA/dados_empilhados_BA.parquet")

caminho_pasta_CE = "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/CE"
arquivos_CE = list.files(path = caminho_pasta_CE, pattern = "*.dbc", full.names = TRUE)
dados_empilhados_CE = load_data(arquivos = arquivos_CE, caminho_pasta = caminho_pasta_CE)
arrow::write_parquet(dados_empilhados_CE %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/CE/dados_empilhados_CE.parquet")

caminho_pasta_DF = "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/DF"
arquivos_DF = list.files(path = caminho_pasta_DF, pattern = "*.dbc", full.names = TRUE)
dados_empilhados_DF = load_data(arquivos = arquivos_DF, caminho_pasta = caminho_pasta_DF)
arrow::write_parquet(dados_empilhados_DF %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/DF/dados_empilhados_DF.parquet")

caminho_pasta_ES = "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/ES"
arquivos_ES = list.files(path = caminho_pasta_ES, pattern = "*.dbc", full.names = TRUE)
dados_empilhados_ES = load_data(arquivos = arquivos_ES, caminho_pasta = caminho_pasta_ES)
arrow::write_parquet(dados_empilhados_ES %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/ES/dados_empilhados_ES.parquet")

caminho_pasta_GO = "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/GO"
arquivos_GO = list.files(path = caminho_pasta_GO, pattern = "*.dbc", full.names = TRUE)
dados_empilhados_GO = load_data(arquivos = arquivos_GO, caminho_pasta = caminho_pasta_GO)
arrow::write_parquet(dados_empilhados_GO %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/GO/dados_empilhados_GO.parquet")

caminho_pasta_MA = "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/MA"
arquivos_MA = list.files(path = caminho_pasta_MA, pattern = "*.dbc", full.names = TRUE)
dados_empilhados_MA = load_data(arquivos = arquivos_MA, caminho_pasta = caminho_pasta_MA)
arrow::write_parquet(dados_empilhados_MA %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/MA/dados_empilhados_MA.parquet")

caminho_pasta_MG = "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/MG"
arquivos_MG = list.files(path = caminho_pasta_MG, pattern = "*.dbc", full.names = TRUE)
dados_empilhados_MG = load_data(arquivos = arquivos_MG, caminho_pasta = caminho_pasta_MG)
arrow::write_parquet(dados_empilhados_MG %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/MG/dados_empilhados_MG.parquet")

caminho_pasta_MS = "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/MS"
arquivos_MS = list.files(path = caminho_pasta_MS, pattern = "*.dbc", full.names = TRUE)
dados_empilhados_MS = load_data(arquivos = arquivos_MS, caminho_pasta = caminho_pasta_MS)
arrow::write_parquet(dados_empilhados_MS %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/MS/dados_empilhados_MS.parquet")

caminho_pasta_MT = "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/MT"
arquivos_MT = list.files(path = caminho_pasta_MT, pattern = "*.dbc", full.names = TRUE)
dados_empilhados_MT = load_data(arquivos = arquivos_MT, caminho_pasta = caminho_pasta_MT)
arrow::write_parquet(dados_empilhados_MT %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/MT/dados_empilhados_MT.parquet")

caminho_pasta_PA = "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/PA"
arquivos_PA = list.files(path = caminho_pasta_PA, pattern = "*.dbc", full.names = TRUE)
dados_empilhados_PA = load_data(arquivos = arquivos_PA, caminho_pasta = caminho_pasta_PA)
arrow::write_parquet(dados_empilhados_PA %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/PA/dados_empilhados_PA.parquet")

caminho_pasta_PB = "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/PB"
arquivos_PB = list.files(path = caminho_pasta_PB, pattern = "*.dbc", full.names = TRUE)
dados_empilhados_PB = load_data(arquivos = arquivos_PB, caminho_pasta = caminho_pasta_PB)
arrow::write_parquet(dados_empilhados_PB %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/PB/dados_empilhados_PB.parquet")

caminho_pasta_PE = "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/PE"
arquivos_PE = list.files(path = caminho_pasta_PE, pattern = "*.dbc", full.names = TRUE)
dados_empilhados_PE = load_data(arquivos = arquivos_PE, caminho_pasta = caminho_pasta_PE)
arrow::write_parquet(dados_empilhados_PE %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/PE/dados_empilhados_PE.parquet")

caminho_pasta_PI = "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/PI"
arquivos_PI = list.files(path = caminho_pasta_PI, pattern = "*.dbc", full.names = TRUE)
dados_empilhados_PI = load_data(arquivos = arquivos_PI, caminho_pasta = caminho_pasta_PI)
arrow::write_parquet(dados_empilhados_PI %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/PI/dados_empilhados_PI.parquet")

caminho_pasta_PR = "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/PR"
arquivos_PR = list.files(path = caminho_pasta_PR, pattern = "*.dbc", full.names = TRUE)
dados_empilhados_PR = load_data(arquivos = arquivos_PR, caminho_pasta = caminho_pasta_PR)
arrow::write_parquet(dados_empilhados_PR %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/PR/dados_empilhados_PR.parquet")

caminho_pasta_RJ = "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/RJ"
arquivos_RJ = list.files(path = caminho_pasta_RJ, pattern = "*.dbc", full.names = TRUE)
dados_empilhados_RJ = load_data(arquivos = arquivos_RJ, caminho_pasta = caminho_pasta_RJ)
arrow::write_parquet(dados_empilhados_RJ %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/RJ/dados_empilhados_RJ.parquet")

caminho_pasta_RN = "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/RN"
arquivos_RN = list.files(path = caminho_pasta_RN, pattern = "*.dbc", full.names = TRUE)
dados_empilhados_RN = load_data(arquivos = arquivos_RN, caminho_pasta = caminho_pasta_RN)
arrow::write_parquet(dados_empilhados_RN %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/RN/dados_empilhados_RN.parquet")

caminho_pasta_RO = "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/RO"
arquivos_RO = list.files(path = caminho_pasta_RO, pattern = "*.dbc", full.names = TRUE)
dados_empilhados_RO = load_data(arquivos = arquivos_RO, caminho_pasta = caminho_pasta_RO)
arrow::write_parquet(dados_empilhados_RO %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/RO/dados_empilhados_RO.parquet")

caminho_pasta_RR = "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/RR"
arquivos_RR = list.files(path = caminho_pasta_RR, pattern = "*.dbc", full.names = TRUE)
dados_empilhados_RR = load_data(arquivos = arquivos_RR, caminho_pasta = caminho_pasta_RR)
arrow::write_parquet(dados_empilhados_RR %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/RR/dados_empilhados_RR.parquet")

caminho_pasta_RS = "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/RS"
arquivos_RS = list.files(path = caminho_pasta_RS, pattern = "*.dbc", full.names = TRUE)
dados_empilhados_RS = load_data(arquivos = arquivos_RS, caminho_pasta = caminho_pasta_RS)
arrow::write_parquet(dados_empilhados_RS %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/RS/dados_empilhados_RS.parquet")

caminho_pasta_SC = "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/SC"
arquivos_SC = list.files(path = caminho_pasta_SC, pattern = "*.dbc", full.names = TRUE)
dados_empilhados_SC = load_data(arquivos = arquivos_SC, caminho_pasta = caminho_pasta_SC)
arrow::write_parquet(dados_empilhados_SC %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/SC/dados_empilhados_SC.parquet")

caminho_pasta_SE = "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/SE"
arquivos_SE = list.files(path = caminho_pasta_SE, pattern = "*.dbc", full.names = TRUE)
dados_empilhados_SE = load_data(arquivos = arquivos_SE, caminho_pasta = caminho_pasta_SE)
arrow::write_parquet(dados_empilhados_SE %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/SE/dados_empilhados_SE.parquet")

caminho_pasta_SP = "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/SP"
arquivos_SP = list.files(path = caminho_pasta_SP, pattern = "*.dbc", full.names = TRUE)
dados_empilhados_SP = load_data(arquivos = arquivos_SP, caminho_pasta = caminho_pasta_SP)
arrow::write_parquet(dados_empilhados_SP %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/SP/dados_empilhados_SP.parquet")

caminho_pasta_TO = "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/TO"
arquivos_TO = list.files(path = caminho_pasta_TO, pattern = "*.dbc", full.names = TRUE)
dados_empilhados_TO = load_data(arquivos = arquivos_TO, caminho_pasta = caminho_pasta_TO)
arrow::write_parquet(dados_empilhados_TO %>% as.data.frame(), "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/TO/dados_empilhados_TO.parquet")

#######################################3

caminho_pasta <- "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/AC/"
arquivos_dbc <- list.files(path = caminho_pasta, pattern = "*.dbc", full.names = TRUE)
carregar_empilhar_dbc <- function(arquivos) {
  df_final <- arquivos %>% lapply(read.dbc) %>% bind_rows()
  return(df_final)
}
dados_empilhados <- carregar_empilhar_dbc(arquivos_dbc)

dados_originais = read.dbc("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/internacoes/RDTO2405.dbc")
dados = dados_originais %>% select(ANO_CMPT,MES_CMPT,MUNIC_RES,SEXO,DIAG_PRINC,COD_IDADE,IDADE)
data %>% head
DescritivaCat(data$COD_IDADE)

dados %>% filter(COD_IDADE == 5)

data %>% select(ANO_CMPT,MES_CMPT,MUNIC_RES,SEXO,DIAG_PRINC,COD_IDADE,IDADE) %>% head

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


dados_pop = tryCatch({read.xlsx("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/hesitacao-vacinal/Dados Cecília Etapa 2.xlsx", sheet = 1)},
                     error = function(e) {read.xlsx("D:/NESCON/Trabalho - Cecília/hesitacao-vacinal/Dados Cecília Etapa 2.xlsx", sheet = 1)})

extrator = tryCatch({read.xlsx("C:/Users/cesar_macieira/Desktop/Usiminas/Nescon/hesitacao-vacinal/Dados extrator.xlsx", sheet = 1)},
                    error = function(e) {read.xlsx("D:/NESCON/Trabalho - Cecília/hesitacao-vacinal/Dados extrator.xlsx", sheet = 1)})
