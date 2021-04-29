#Pacotes que serão utilizados ----
pacotes = c("tidyr","tidylog", "tidyverse", "janitor", "RODBC",
            "dbplyr")

for (x in pacotes) {
  if(!x %in% installed.packages()) {
    install.packages(x)
  }
}

lapply(pacotes, require, character.only = T)

rm(pacotes, x)


#Pasta com os arquivos access
access_folder <- "D:/Finbra"

#Listar arquivos access
access_files <- list.files(access_folder, pattern = "\\.mdb|\\.accdb")

#Lista de conexoes
lista_con <- as.list(paste0(access_folder, "/", access_files))

con <- lapply(lista_con, odbcConnectAccess2007)

#nomear com ano cada linha cada linha 
numerais <- readr::parse_number(access_files)
nomes <- paste0("ano_", numerais)
names(con) <- nomes

#Filtrar para os anos a partir de 2000
con_selec <- con[-14]

#Lista de tabelas
##funcao para requisitar tabelas
nome_tab <- function(x) {
  sqlTables(x, tableType = "TABLE")$TABLE_NAME
}

tabelas <- lapply(con_selec, nome_tab )


#1.Receitas ----
###obs:em 2002 e 2003 temos RecDesp e não Receita.3
receitas <- c()  #lista que vai receber as tabelas de receitas

for (i in 1:13) {
  ifelse(i %in% (3:4),
         receitas[[i]] <- sqlFetch(con_selec[[i]], "RecDesp"),
         receitas[[i]] <- sqlFetch(con_selec[[i]], "Receita")
         )
} #tabelas de receitas. obs:ideal seria mudar nomes das tabelas no access pra padronizar, mas não consegui

##OBS: Coluna 93 pra frente em 2002 e 2003 são despesas, excluir.
for (i in 3:4) {
  receitas[[i]] <- receitas[[i]][,1:92]
}

#transformar em formato long
long_func<- function(x){
pivot_longer(x,
                  cols = (3:length(x)),
                  names_to = "contas",
                  values_to = "valor")
}

receitas <- lapply(receitas, long_func)
#criar variavel ano
for (i in 1:13) {
         receitas[[i]] <- receitas[[i]] %>% 
           mutate(ano = 2000 + i - 1)

} 

#Juntar toda lista em um unico df
receitas <- bind_rows(receitas)

#Salvar
save(receitas, file = "receita_orcamentaria_municipal_2000-2012.Rda")

#2.Despesas Empenhadas ----
despesas <- c()  #lista que vai receber as tabelas de despesas

for (i in 1:13) {
  ifelse(i %in% (3:4),
         despesas[[i]] <- sqlFetch(con_selec[[i]], "RecDesp"),
         despesas[[i]] <- sqlFetch(con_selec[[i]], "Despesa")
  )
} #tabelas de despesas

##OBSERVACOES:
#Coluna 92 pra tras em 2002 e 2003 são receitas, excluir.----
for (i in 3:4) {
  despesas[[i]] <- despesas[[i]][,-(3:92)]
}

# Até 2003 as despesas empenhadas eram juntas com as despesas por funcao. Então, para separar:
## 2000 e 2001 coluna 31 pra frente é Despesa por funcao
## 2002 coluna 76 pra frente é Despesa por funcao
## 2003 coluna 75 pra frente é Despesa por funcao
for (i in 1:2) {
  despesas[[i]] <- despesas[[i]][,(1:30)]
}

despesas[[3]] <- despesas[[3]][,(1:75)]
despesas[[4]] <- despesas[[4]][,(1:74)]

#transformar em formato long ----

despesas <- lapply(despesas, long_func)

#criar variavel ano
for (i in 1:13) {
  despesas[[i]] <- despesas[[i]] %>% 
    mutate(ano = 2000 + i - 1)
  
} 

#Juntar toda lista em um unico df
despesas <- bind_rows(despesas)

#Salvar
save(despesas, file = "despesa_orcamentaria_empenhada_municipal_2000-2012.Rda")

#3.Despesas por funcao ----
despesas_funcao <- c()  #lista que vai receber as tabelas de despesas_funcao


for (i in 1:13) {
  if (i <3) {
    despesas_funcao[[i]] <- sqlFetch(con_selec[[i]], "Despesa")
} else {
  if(i %in% 3:4) {
    despesas_funcao[[i]] <- sqlFetch(con_selec[[i]], "RecDesp")
  } else{
    if(i == 5) {
      despesas_funcao[[i]] <- sqlFetch(con_selec[[i]], "DFuncao")
    } else{
      despesas_funcao[[i]] <- sqlFetch(con_selec[[i]], "DSubFuncao")
      }
    }
  }
}

##OBSERVACOES:
#Coluna 92 pra tras em 2002 e 2003 são receitas, excluir.----
for (i in 3:4) {
  despesas_funcao[[i]] <- despesas_funcao[[i]][,-(3:92)]
}

# Até 2003 as despesas empenhadas eram juntas com as despesas por funcao. Então, para separar:
## 2000 e 2001 coluna 31 pra frente é Despesa por funcao
## 2002 coluna 76 pra frente é Despesa por funcao
## 2003 coluna 75 pra frente é Despesa por funcao
for (i in 1:2) {
  despesas_funcao[[i]] <- despesas_funcao[[i]][,-(3:30)]
}

despesas_funcao[[3]] <- despesas_funcao[[3]][,-(3:75)]
despesas_funcao[[4]] <- despesas_funcao[[4]][,-(3:74)]

#OBS: ----
# A partir de 2004, CD_UF e CD_MUN viram UF e Cod Mun
for (i in 5:length(despesas_funcao)) {
  despesas_funcao[[i]] <- despesas_funcao[[i]] %>% 
    rename(CD_UF = UF,
           CD_MUN = `Cod Mun`)
  
}
#transformar em formato long ----

despesas_funcao <- lapply(despesas_funcao, long_func)

#criar variavel ano
for (i in 1:13) {
  despesas_funcao[[i]] <- despesas_funcao[[i]] %>% 
    mutate(ano = 2000 + i - 1)
  
} 

#Juntar toda lista em um unico df
despesas_funcao <- bind_rows(despesas_funcao)

#Salvar
save(despesas_funcao, file = "despesa_funcao_municipal_2000-2012.Rda")

#OBS: Para 2010, a despesa por funcao trouxe valores para as colunas. Corrigir.





