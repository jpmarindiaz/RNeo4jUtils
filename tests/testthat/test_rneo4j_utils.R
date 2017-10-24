context("test")

test_that("test",{

  # MAKE SURE NEO4J IS UP: neo4j start
  dburl <- "http://localhost:7474/db/data/"
  username <- "neo4j"
  password <- "neo4jpwd"

  graph <- startGraph(dburl, username = username, password = password)
  clear(graph = graph, input = FALSE)

  file <- system.file("data/movies.csv",package = "RNeo4jUtils")
  label <- c("Movie","Pelicula")

  load_nodes_csv(file,label,graph)
  expect_equal(get_total_node_count(graph),5)
  expect_equal(get_total_edge_count(graph),0)
  expect_equal(get_node_count("Movie",graph),4)
  expect_equal(get_node_count("Pelicula",graph),4)

  movies <- get_nodes_label_table("Movie", graph = graph)
  peliculas <- get_nodes_label_table("Pelicula", graph = graph)
  nodes <- read_csv(file, col_types = cols(.default = "c"))
  attr(nodes,"spec") <- NULL
  movies2 <- movies[names(nodes)]
  expect_equal(nodes,movies2)

  delete_node(sample(movies$.id,1),graph)
  expect_equal(get_node_count("Movie",graph),3)

  createNode(graph,"Movie",id = "New movie", country = "COL", vals = c("val1", "val2"))
  movies <- get_nodes_label_table("Movie", graph = graph)
  expect_true(!is.null(movies$vals))
  expect_true(length(strsplit(movies$vals[4], split="||", fixed = TRUE)[[1]]) == 2)
  delete_nodes("Movie",graph)
  expect_equal(get_node_count("Movie",graph),0)

  load_nodes_csv(file,"Movie",graph)
  file <- system.file("data/persons.csv",package = "RNeo4jUtils")
  load_nodes_csv(file,"Person",graph)
  get_node_count("Person",graph)

  people <- get_nodes_label_table("Person",graph)
  movies <- get_nodes_label_table("Movie",graph)

  allNodes <- get_nodes_table(graph)

  n <- get_node_by_id(sample(people$.id,1), graph = graph)
  expect_true("node" %in% class(n))
  n <- get_node_by_uid(uid = 1, prop = "id", label = "Person", graph = graph)
  expect_true("node" %in% class(n))
  n <- get_node_by_uid(uid = "la-estrategia", prop = "id", label = "Movie", graph = graph)
  expect_true("node" %in% class(n))
  n <- get_node_by_uid(uid = "1", prop = "id", label = "Movie", graph = graph)
  expect_true("node" %in% class(n))
  # Get node by uid with no label
  n <- get_node_by_uid(uid = "1", prop = "id", graph = graph)
  expect_true("node" %in% class(n))

  edges <- read_csv(system.file("data/roles.csv",package = "RNeo4jUtils"))

  sourceCol <- "personId"
  targetCol <- "movieId"
  sourceProp <- "id"
  targetProp <- "id"
  sourceLabel <- "Person"
  targetLabel <- "Movie"
  relType <- "ROLE"
  relTypeCol <- "role"

  load_edges_data_frame(edges,
                        sourceCol = sourceCol, targetCol = targetCol,
                        relType = relType, relTypeCol = NULL,
                        sourceProp = sourceProp, targetProp = targetProp,
                        sourceLabel = sourceLabel, targetLabel = targetLabel,
                        graph,createNodes = FALSE)
  expect_equal(get_total_edge_count(graph),14)

  load_edges_data_frame(edges,
                        sourceCol = sourceCol, targetCol = targetCol,
                        relTypeCol = relTypeCol,
                        sourceProp = sourceProp, targetProp = targetProp,
                        sourceLabel = sourceLabel, targetLabel = targetLabel,
                        graph = graph, createNodes = FALSE)

  edgesTable <- get_edges_table(graph)

  edgesNew <- edges
  edgesNew$personId[1] <- 99
  expect_warning(load_edges_data_frame(edgesNew,
                        sourceCol = sourceCol, targetCol = targetCol,
                        relType = relType, relTypeCol = NULL,
                        sourceProp = sourceProp, targetProp = targetProp,
                        sourceLabel = sourceLabel, targetLabel = targetLabel,
                        graph,createNodes = FALSE))

  edgesNew$personId[1] <- 1
  edgesNew$role[1] <- NA
  load_edges_data_frame(edgesNew,
                        sourceCol = sourceCol, targetCol = targetCol,
                        relType = NULL, relTypeCol = "role",
                        sourceProp = sourceProp, targetProp = targetProp,
                        sourceLabel = sourceLabel, targetLabel = targetLabel,
                        graph,createNodes = FALSE)

  nMovies <- get_node_count("Movie",graph)
  expect_error(delete_node_by_uid("1","id","Movie", graph = graph))
  delete_node_by_uid("1","id","Movie", graph = graph, withRels = TRUE)
  expect_equal(nMovies -1, get_node_count("Movie", graph = graph))
  expect_equal(get_edge_count("ROLE",graph),16)
  expect_equal(get_edge_count("ROLE",graph),16)
  expect_equal(get_total_node_count(graph),12)

  allNodes <- get_nodes_table(graph)
  string <- "merixxxxxxx"
  props <- c("id","name","country")
  expect_true(is.null(search_nodes(string, props, caseSensitive = FALSE, graph)))
  string <- "een"
  props <- c("id","name","country")
  expect_true(nrow(search_nodes(string, props, caseSensitive = FALSE, graph)) == 2)

})
