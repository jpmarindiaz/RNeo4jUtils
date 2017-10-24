
#' @export
neo4j_nodeId <- function(node){
  if(!"node" %in% class(node))
    stop("node must be of class node")
  basename(attributes(node)$self)
}


#' @export
get_total_node_count <- function(graph){
  q <- "MATCH (n)
  RETURN COUNT(n)"
  flatten_int(cypher(graph, q))
}

#' @export
get_node_by_id <- function(id, graph, asNode = FALSE){
  q <- 'MATCH (n)
WHERE ID(n) = {id}
RETURN n'
  q <- str_tpl_format(q,list(id = id))
  n <- getSingleNode(graph, q)
  if(is.null(n)){
    warning("id not in db ",id)
    return(NULL)
  }
  if(asNode) return(n)
  n
}

#' @export
get_node_by_uid <- function(uid, prop, label = NULL, graph = NULL, asNode = FALSE){
  if(is.character(uid)){
    uid <- paste0("'",uid,"'")
  }
  if(is.null(label)){
      q <- "MATCH (n)
    WHERE n.{prop} = {uid}
    RETURN n"
      q <- str_tpl_format(q,list(uid = uid, prop = prop))
  }else{
    q <- "MATCH (n:{label})
    WHERE n.{prop} = {uid}
    RETURN n"
    q <- str_tpl_format(q,list(uid = uid, prop = prop, label = label))
  }
  n <- getSingleNode(graph, q)
  if(is.null(n)){
    warning("uid not in db ", uid)
    return(NULL)
  }
  if(asNode) return(n)
  n
}

#' @export
get_keys <- function(label, graph = NULL, asTable = TRUE){
  q <- "MATCH (p:{label}) RETURN id(p),keys(p);"
  q <- str_tpl_format(q,list(label = label))
  keys <- cypherToList(graph, q)
  f <- function(x){
    l <- x[[2]]
    l$id <- x[[1]]
    l
  }
  keys <- map(keys,f)
  if(asTable) return(bind_rows(keys))
}

#' @export
get_nodes_label_table <- function(label, graph){
  labels <- getLabel(graph)
  if(!label %in% labels) stop("label not in Labels")
  nodes <- getLabeledNodes(graph, label)
  f <- function(n){
    n$.neo4j_url <- attr(n,"self")
    n$.id <- basename(n$.neo4j_url)
    map(n,paste,collapse="||")
  }
  nodes2 <- map(nodes,f)
  d <- bind_rows(nodes2)
  d %>% select(.id,everything())
}

#' @export
get_nodes_table <- function(graph){
  labels <- getLabel(graph)
  nodesTables <- map(labels, get_nodes_label_table, graph) %>% set_names(labels)
  bind_rows(nodesTables, .id = "label")
}

#' @export
get_edges_table <- function(graph){
  q <- "MATCH ()-[r]->() RETURN r"
  r <- cypherToList(graph, q)
  start <- map_chr(r, ~ basename(attr(.$r,"start")))
  end <-  map_chr(r, ~ basename(attr(.$r,"end")))
  type <-  map_chr(r, ~ basename(attr(.$r,"type")))
  relId <-  map_chr(r, ~ basename(attr(.$r,"self")))
  edgesTable <- data_frame(.relId = relId, type = type, from = start, to = end)
  labels <- getLabel(graph)
  nodesTables <- get_nodes_table(graph)
  nodesTablesIds <- nodesTables[c(".id","label")]
  edgesTable$fromLabel <- match_replace(edgesTable$from,nodesTablesIds)
  edgesTable$toLabel <- match_replace(edgesTable$to,nodesTablesIds)
  edgesTable
}

#' @export
load_nodes_data_frame <- function(d, label = NULL, graph = NULL){
  #d <- read_csv(file)
  nodes <- transpose(d)
  map(nodes,~createNode(graph,label,.))
  createNode(graph,label = label)
}

#' @export
load_nodes_csv <- function(file, label = NULL, graph = NULL){
  d <- read_csv(file)
  load_nodes_data_frame(d,label = label, graph)
}


#' @export
get_node_count <- function(label,graph){
  q <- "MATCH (n:{label})
  RETURN COUNT(n)"
  q <- str_tpl_format(q,list(label = label))
  flatten_int(cypher(graph, q))
}

#' @export
get_edge_count <- function(relType, graph){
  q <- "MATCH (n)-[r:`{relType}`]-(m)
  RETURN COUNT(r)"
  q <- str_tpl_format(q,list(relType = relType))
  flatten_int(cypher(graph, q))
}

#' @export
get_total_edge_count <- function(graph){
  q <- "MATCH (n)-[r]-(m)
    RETURN COUNT(r)"
  flatten_int(cypher(graph, q))
}


#' @export
delete_node <- function(id, graph = NULL, withRels = FALSE){
  if(withRels){
    q = "
    MATCH (n)-[r]-()
    WHERE id(n)={id}
    DELETE n, r"
  }else{
    q <- "
    MATCH (n)
    WHERE id(n)={id}
    DELETE n"
  }
  q <- str_tpl_format(q,list(id = id))
  cypher(graph,q)
  TRUE
}

#' @export
delete_node_by_uid <- function(uid, prop, label, graph = NULL, withRels = FALSE){
  if(is.character(uid)){
    uid <- paste0("'",uid,"'")
  }
  if(withRels){
    q = "
    MATCH (n:{label})-[r]-()
    WHERE n.{prop}={uid}
    DELETE n, r"
  }else{
    q <- "
    MATCH (n:{label})
    WHERE n.{prop}={uid}
    DELETE n"
  }
  q <- str_tpl_format(q,list(uid=uid,label = label, prop = prop))
  cypher(graph,q)
  TRUE
}

#' @export
delete_nodes <- function(label, graph = NULL, withRels = FALSE){
  if(withRels){
    q = "
    MATCH (n:{label})-[r]-()
    DELETE n, r"
  }else{
    q = "MATCH (n:{label})
    DELETE n"
  }
  q <- str_tpl_format(q,list(label = label))
  cypher(graph,q)
  TRUE
}


#' @export
load_edges_data_frame <- function(edges,
                                  sourceCol = NULL, targetCol = NULL,
                                  relType = NULL, relTypeCol = NULL,
                                  sourceProp = NULL, targetProp = NULL,
                                  sourceLabel = NULL, targetLabel = NULL,
                                  graph = NULL,createNodes = FALSE){
  ed <- transpose(edges)
  f <- function(e){
    #e <- ed[[1]]
    props <- e
    props[sourceCol] <- NULL
    props[targetCol] <- NULL
    src <- get_node_by_uid(e[[sourceCol]],prop = sourceProp, label = sourceLabel,
                           graph = graph)
    tgt <- get_node_by_uid(e[[targetCol]],prop = targetProp, label = targetLabel,
                           graph = graph)
    if(is.null(src) || is.null(tgt))
      return(paste0("ERROR in src:",e[[sourceCol]], ", tgt: ",e[[targetCol]]))
    if(!is.null(relTypeCol)){
      if(!relTypeCol %in% names(e)) stop("RelTypeCol not in node")
      relType <- e[[relTypeCol]]
    }
    props <- props[!is.na(props)]
    if(is.na(relType))
      return(paste0("ERROR: REL_TYPE is NA in src:",e[[sourceCol]], ", tgt: ",e[[targetCol]]))

    if(length(props) == 0)
      props <- NULL

      createRel(src, relType, tgt, props)
    "OK"
  }
  map(ed,f)
}

#' @export
load_edges_csv <- function(file, sourceCol = NULL, targetCol = NULL,
                           relType = NULL,
                           sourceProp = NULL, targetProp = NULL,
                           sourceLabel = NULL, targetLabel = NULL,
                           graph = NULL,createNodes = FALSE){
  edges <- read_csv(file)
  load_edges_data_frame(edges,sourceCol = sourceCol, targetCol = targetCol,
                        relType = relType,
                        sourceProp = sourceProp, targetProp = targetProp,
                        sourceLabel = sourceLabel, targetLabel = targetLabel,
                        graph = graph,createNodes = FALSE)
}



