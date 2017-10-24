

#' @export
search_nodes <- function(string, props, caseSensitive = FALSE, graph){
  #props <- c("title", "name")
  if(caseSensitive){
    propsClause <- "n.{props} =~ '.*{string}.*'"
  }else{
    propsClause <- "n.{props} =~ '(?i).*{string}.*'"
  }
  q <- "MATCH (n) where {clause} return n;"
  clause <- paste0(map_chr(props, ~ str_tpl_format(propsClause, list(props = .))),collapse = " OR ")
  q <- str_tpl_format(q, list(clause = clause, string = string))
  nodes <- cypherToList(graph, q)
  if(length(nodes)==0) return()
  f <- function(n){
    n <- n$n
    n$.neo4j_url <- attr(n,"self")
    n$.id <- basename(n$.neo4j_url)
    map(n,paste,collapse="||")
  }
  nodes2 <- map(nodes,f)
  d <- bind_rows(nodes2)
  d %>% select(.id,everything())
}

