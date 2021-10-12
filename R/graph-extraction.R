#' Spark NLP GraphExtraction
#'
#' Spark ML transformer that Extracts a dependency graph between entities.
#' The GraphExtraction class takes e.g. extracted entities from a NerDLModel and creates a dependency 
#' tree which describes how the entities relate to each other. For that a triple store format is used. 
#' Nodes represent the entities and the edges represent the relations between those entities. The 
#' graph can then be used to find relevant relationships between words.
#' 
#' Both the DependencyParserModel and TypedDependencyParserModel need to be present in the pipeline. There are two ways to set them:
#' 
#'     Both Annotators are present in the pipeline already. The dependencies are taken implicitly from these two Annotators.
#'     Setting setMergeEntities to true will download the default pretrained models for those two Annotators automatically.
#'      The specific models can also be set with setDependencyParserModel and setTypedDependencyParserModel:
#' 
#' See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#graphextraction}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param delimiter Delimiter symbol used for path output (Default: ",")
#' @param dependency_parser_model Coordinates (name, lang, remoteLoc) to a pretrained Dependency Parser model (Default: Array())
#' @param entity_types Find paths between a pair of entities (Default: Array())
#' @param explode_entities When set to true find paths between entities (Default: false)
#' @param include_edges Whether to include edges when building paths (Default: true)
#' @param max_sentence_size Maximum sentence size that the annotator will process (Default: 1000). 
#' @param merge_entities Merge same neighboring entities as a single token (Default: false)
#' @param merge_entities_iob_format IOB format to apply when merging entities
#' @param min_sentence_size Minimum sentence size that the annotator will process (Default: 2).
#' @param pos_model Coordinates (name, lang, remoteLoc) to a pretrained POS model (Default: Array())
#' @param relationship_types Find paths between a pair of token and entity (Default: Array())
#' @param root_tokens Tokens to be consider as root to start traversing the paths (Default: Array()).
#' @param typed_dependency_parser_model Coordinates (name, lang, remoteLoc) to a pretrained Typed Dependency Parser model (Default: Array())
#' 
#' @export
nlp_graph_extraction <- function(x, input_cols, output_col,
                 delimiter = NULL, dependency_parser_model = NULL, entity_types = NULL, explode_entities = NULL, include_edges = NULL, max_sentence_size = NULL, merge_entities = NULL, merge_entities_iob_format = NULL, min_sentence_size = NULL, pos_model = NULL, relationship_types = NULL, root_tokens = NULL, typed_dependency_parser_model = NULL,
                 uid = random_string("graph_extraction_")) {
  UseMethod("nlp_graph_extraction")
}

#' @export
nlp_graph_extraction.spark_connection <- function(x, input_cols, output_col,
                 delimiter = NULL, dependency_parser_model = NULL, entity_types = NULL, explode_entities = NULL, include_edges = NULL, max_sentence_size = NULL, merge_entities = NULL, merge_entities_iob_format = NULL, min_sentence_size = NULL, pos_model = NULL, relationship_types = NULL, root_tokens = NULL, typed_dependency_parser_model = NULL,
                 uid = random_string("graph_extraction_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    delimiter = delimiter,
    dependency_parser_model = dependency_parser_model,
    entity_types = entity_types,
    explode_entities = explode_entities,
    include_edges = include_edges,
    max_sentence_size = max_sentence_size,
    merge_entities = merge_entities,
    merge_entities_iob_format = merge_entities_iob_format,
    min_sentence_size = min_sentence_size,
    pos_model = pos_model,
    relationship_types = relationship_types,
    root_tokens = root_tokens,
    typed_dependency_parser_model = typed_dependency_parser_model,
    uid = uid
  ) %>%
  validator_nlp_graph_extraction()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.GraphExtraction",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setDelimiter", args[["delimiter"]])  %>%
    sparklyr::jobj_set_param("setDependencyParserModel", args[["dependency_parser_model"]])  %>%
    sparklyr::jobj_set_param("setEntityTypes", args[["entity_types"]])  %>%
    sparklyr::jobj_set_param("setExplodeEntities", args[["explode_entities"]])  %>%
    sparklyr::jobj_set_param("setIncludeEdges", args[["include_edges"]])  %>%
    sparklyr::jobj_set_param("setMaxSentenceSize", args[["max_sentence_size"]])  %>%
    sparklyr::jobj_set_param("setMergeEntities", args[["merge_entities"]])  %>%
    sparklyr::jobj_set_param("setMergeEntitiesIOBFormat", args[["merge_entities_iob_format"]])  %>%
    sparklyr::jobj_set_param("setMinSentenceSize", args[["min_sentence_size"]])  %>%
    sparklyr::jobj_set_param("setPosModel", args[["pos_model"]])  %>%
    sparklyr::jobj_set_param("setRelationshipTypes", args[["relationship_types"]])  %>%
    sparklyr::jobj_set_param("setRootTokens", args[["root_tokens"]])  %>%
    sparklyr::jobj_set_param("setTypedDependencyParserModel", args[["typed_dependency_parser_model"]]) 

  new_nlp_graph_extraction(jobj)
}

#' @export
nlp_graph_extraction.ml_pipeline <- function(x, input_cols, output_col,
                 delimiter = NULL, dependency_parser_model = NULL, entity_types = NULL, explode_entities = NULL, include_edges = NULL, max_sentence_size = NULL, merge_entities = NULL, merge_entities_iob_format = NULL, min_sentence_size = NULL, pos_model = NULL, relationship_types = NULL, root_tokens = NULL, typed_dependency_parser_model = NULL,
                 uid = random_string("graph_extraction_")) {

  stage <- nlp_graph_extraction.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    delimiter = delimiter,
    dependency_parser_model = dependency_parser_model,
    entity_types = entity_types,
    explode_entities = explode_entities,
    include_edges = include_edges,
    max_sentence_size = max_sentence_size,
    merge_entities = merge_entities,
    merge_entities_iob_format = merge_entities_iob_format,
    min_sentence_size = min_sentence_size,
    pos_model = pos_model,
    relationship_types = relationship_types,
    root_tokens = root_tokens,
    typed_dependency_parser_model = typed_dependency_parser_model,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_graph_extraction.tbl_spark <- function(x, input_cols, output_col,
                 delimiter = NULL, dependency_parser_model = NULL, entity_types = NULL, explode_entities = NULL, include_edges = NULL, max_sentence_size = NULL, merge_entities = NULL, merge_entities_iob_format = NULL, min_sentence_size = NULL, pos_model = NULL, relationship_types = NULL, root_tokens = NULL, typed_dependency_parser_model = NULL,
                 uid = random_string("graph_extraction_")) {
  stage <- nlp_graph_extraction.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    delimiter = delimiter,
    dependency_parser_model = dependency_parser_model,
    entity_types = entity_types,
    explode_entities = explode_entities,
    include_edges = include_edges,
    max_sentence_size = max_sentence_size,
    merge_entities = merge_entities,
    merge_entities_iob_format = merge_entities_iob_format,
    min_sentence_size = min_sentence_size,
    pos_model = pos_model,
    relationship_types = relationship_types,
    root_tokens = root_tokens,
    typed_dependency_parser_model = typed_dependency_parser_model,
    uid = uid
  )

  stage %>% sparklyr::ml_transform(x)
}
#' @import forge
validator_nlp_graph_extraction <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["delimiter"]] <- cast_nullable_string(args[["delimiter"]])
  args[["dependency_parser_model"]] <- cast_nullable_string_list(args[["dependency_parser_model"]])
  args[["entity_types"]] <- cast_nullable_string_list(args[["entity_types"]])
  args[["explode_entities"]] <- cast_nullable_logical(args[["explode_entities"]])
  args[["include_edges"]] <- cast_nullable_logical(args[["include_edges"]])
  args[["max_sentence_size"]] <- cast_nullable_integer(args[["max_sentence_size"]])
  args[["merge_entities"]] <- cast_nullable_logical(args[["merge_entities"]])
  args[["merge_entities_iob_format"]] <- cast_nullable_string(args[["merge_entities_iob_format"]])
  args[["min_sentence_size"]] <- cast_nullable_integer(args[["min_sentence_size"]])
  args[["pos_model"]] <- cast_nullable_string_list(args[["pos_model"]])
  args[["relationship_types"]] <- cast_nullable_string_list(args[["relationship_types"]])
  args[["root_tokens"]] <- cast_nullable_string_list(args[["root_tokens"]])
  args[["typed_dependency_parser_model"]] <- cast_nullable_string_list(args[["typed_dependency_parser_model"]])
  args
}

nlp_float_params.nlp_graph_extraction <- function(x) {
  return(c())
}
new_nlp_graph_extraction <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_graph_extraction")
}
