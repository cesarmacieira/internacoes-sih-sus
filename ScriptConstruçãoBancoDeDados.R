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
load_data = function(arquivos, caminho_pasta_dbc, caminho_pasta_parquet) {
  lista_dfs = list()
  for (arquivo in arquivos) {
    df = read.dbc(arquivo) %>% select(ANO_CMPT,MES_CMPT,MUNIC_RES,SEXO,DIAG_PRINC,COD_IDADE,IDADE)
    lista_dfs = append(lista_dfs, list(df))
    nome_arquivo = gsub(".dbc", ".parquet", basename(arquivo))
    caminho_parquet = file.path(caminho_pasta_parquet, nome_arquivo)
    arrow::write_parquet(df %>% as.data.frame(), caminho_parquet)
  }
  df_final = bind_rows(lista_dfs)
  return(df_final)
}

caminho_pasta_dbc = "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/AC/Arquivos dbc"
caminho_pasta_parquet = "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/AC/Arquivos parquet"
arquivos_dbc = list.files(path = caminho_pasta_dbc, pattern = "*.dbc", full.names = TRUE)
dados_empilhados_AC = load_data(arquivos_dbc, 
                                caminho_pasta_dbc = "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/AC/Arquivos dbc",
                                caminho_pasta_parquet = "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/AC/Arquivos parquet")

caminho_pasta_dbc = "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/AC/Arquivos dbc"
caminho_pasta_parquet = "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/AC/Arquivos parquet"
arquivos_dbc = list.files(path = caminho_pasta_dbc, pattern = "*.dbc", full.names = TRUE)
dados_empilhados_AC = load_data(arquivos_dbc, 
                                caminho_pasta_dbc = "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/AC/Arquivos dbc",
                                caminho_pasta_parquet = "D:/NESCON/Bancos de Dados ICSAP SIH-SUS/internacoes-sih-sus/AC/Arquivos parquet")


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
