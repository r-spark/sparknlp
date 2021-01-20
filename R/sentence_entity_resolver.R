#' Spark NLP SentenceEntityResolverApproach
#'
#' Spark ML estimator that assigns a standard code (ICD10 CM, PCS, ICDO; CPT) to
#' sentence embeddings pooled over chunks from TextMatchers or the NER Models.
#' This annotator is particularly handy when working with BertSentenceEmbeddings
#' from the upstream chunks.
#' 
#' See \url{https://nlp.johnsnowlabs.com/docs/en/licensed_annotators#sentenceentityresolver}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param label_column column name for the value we are trying to resolve
#' @param normalized_col column name for the original, normalized description
#' @param neighbors number of neighbors to consider in the KNN query to calculate WMD
#' @param threshold threshold value for the aggregated distance
#' @param miss_as_empty whether or not to return an empty annotation on unmatched chunks
#' @param case_sensitive whether the entity should be considered using case sensitivity
#' @param confidence_function what function to use to calculate confidence: INVERSE or SOFTMAX
#' @param distance_function what distance function to use for KNN: 'EUCLIDEAN' or 'COSINE'
#' 
#' @export
nlp_sentence_entity_resolver <- function(x, input_cols, output_col,
                 label_column = NULL, normalized_col = NULL, neighbors = NULL, threshold = NULL, miss_as_empty = NULL, case_sensitive = NULL, confidence_function = NULL, distance_function = NULL,
                 uid = random_string("sentence_entity_resolver_")) {
  UseMethod("nlp_sentence_entity_resolver")
}

#' @export
nlp_sentence_entity_resolver.spark_connection <- function(x, input_cols, output_col,
                 label_column = NULL, normalized_col = NULL, neighbors = NULL, threshold = NULL, miss_as_empty = NULL, case_sensitive = NULL, confidence_function = NULL, distance_function = NULL,
                 uid = random_string("sentence_entity_resolver_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    label_column = label_column,
    normalized_col = normalized_col,
    neighbors = neighbors,
    threshold = threshold,
    miss_as_empty = miss_as_empty,
    case_sensitive = case_sensitive,
    confidence_function = confidence_function,
    distance_function = distance_function,
    uid = uid
  ) %>%
  validator_nlp_sentence_entity_resolver()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.resolution.SentenceEntityResolverApproach",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setLabelCol", args[["label_column"]])  %>%
    sparklyr::jobj_set_param("setNormalizedCol", args[["normalized_col"]])  %>%
    sparklyr::jobj_set_param("setNeighbours", args[["neighbors"]])  %>%
    sparklyr::jobj_set_param("setThreshold", args[["threshold"]])  %>%
    sparklyr::jobj_set_param("setMissAsEmpty", args[["miss_as_empty"]])  %>%
    sparklyr::jobj_set_param("setCaseSensitive", args[["case_sensitive"]])  %>%
    sparklyr::jobj_set_param("setConfidenceFunction", args[["confidence_function"]])  %>%
    sparklyr::jobj_set_param("setDistanceFunction", args[["distance_function"]]) 

  new_nlp_sentence_entity_resolver(jobj)
}

#' @export
nlp_sentence_entity_resolver.ml_pipeline <- function(x, input_cols, output_col,
                 label_column = NULL, normalized_col = NULL, neighbors = NULL, threshold = NULL, miss_as_empty = NULL, case_sensitive = NULL, confidence_function = NULL, distance_function = NULL,
                 uid = random_string("sentence_entity_resolver_")) {

  stage <- nlp_sentence_entity_resolver.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    label_column = label_column,
    normalized_col = normalized_col,
    neighbors = neighbors,
    threshold = threshold,
    miss_as_empty = miss_as_empty,
    case_sensitive = case_sensitive,
    confidence_function = confidence_function,
    distance_function = distance_function,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_sentence_entity_resolver.tbl_spark <- function(x, input_cols, output_col,
                 label_column = NULL, normalized_col = NULL, neighbors = NULL, threshold = NULL, miss_as_empty = NULL, case_sensitive = NULL, confidence_function = NULL, distance_function = NULL,
                 uid = random_string("sentence_entity_resolver_")) {
  stage <- nlp_sentence_entity_resolver.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    label_column = label_column,
    normalized_col = normalized_col,
    neighbors = neighbors,
    threshold = threshold,
    miss_as_empty = miss_as_empty,
    case_sensitive = case_sensitive,
    confidence_function = confidence_function,
    distance_function = distance_function,
    uid = uid
  )

  stage %>% sparklyr::ml_fit_and_transform(x)
}
#' @import forge
validator_nlp_sentence_entity_resolver <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["label_column"]] <- cast_nullable_string(args[["label_column"]])
  args[["normalized_col"]] <- cast_nullable_string(args[["normalized_col"]])
  args[["neighbors"]] <- cast_nullable_integer(args[["neighbors"]])
  args[["threshold"]] <- cast_nullable_double(args[["threshold"]])
  args[["miss_as_empty"]] <- cast_nullable_logical(args[["miss_as_empty"]])
  args[["case_sensitive"]] <- cast_nullable_logical(args[["case_sensitive"]])
  args[["confidence_function"]] <- cast_nullable_string(args[["confidence_function"]])
  args[["distance_function"]] <- cast_nullable_string(args[["distance_function"]])
  args
}

#' Load a pretrained Spark NLP T5 Transformer model
#' 
#' Create a pretrained Spark NLP \code{T5TransformerModel} model
#' 
#' @template roxlate-pretrained-params
#' @template roxlate-inputs-output-params
#' @param case_sensitive whether to treat the entities as case sensitive
#' @param confidence_function what function to use to calculate confidence: INVERSE or SOFTMAX
#' @param distance_function what distance function to use for KNN: 'EUCLIDEAN' or 'COSINE'
#' @param miss_as_empty whether or not to return an empty annotation on unmatched chunks
#' @param neighbors number of neighbours to consider in the KNN query to calculate WMD
#' @param threshold threshold value for the aggregated distance#' 
#' 
#' @export
nlp_sentence_entity_resolver_pretrained <- function(sc, input_cols, output_col, 
                                                 case_sensitive = NULL, confidence_function = NULL, distance_function = NULL, extra_mass_penalty = NULL, miss_as_empty = NULL, neighbors = NULL, threshold = NULL,
                                                 name = NULL, lang = NULL, remote_loc = NULL) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    case_sensitive = case_sensitive,
    confidence_function = confidence_function,
    distance_function = distance_function,
    extra_mass_penalty = extra_mass_penalty,
    miss_as_empty = miss_as_empty,
    neighbors = neighbors,
    threshold = threshold
  ) %>%
    validator_nlp_sentence_entity_resolver()
  
  model_class <- "com.johnsnowlabs.nlp.annotators.resolution.SentenceEntityResolverModel"
  model <- pretrained_model(sc, model_class, name, lang, remote_loc)
  spark_jobj(model) %>%
    sparklyr::jobj_set_param("setInputCols", args[["input_cols"]]) %>% 
    sparklyr::jobj_set_param("setOutputCol", args[["output_col"]]) %>% 
    sparklyr::jobj_set_param("setCaseSensitive", args[["case_sensitive"]])  %>%
    sparklyr::jobj_set_param("setConfidenceFunction", args[["confidence_function"]])  %>%
    sparklyr::jobj_set_param("setDistanceFunction", args[["distance_function"]])  %>%
    sparklyr::jobj_set_param("setExtramassPenalty", args[["extra_mass_penalty"]])  %>%
    sparklyr::jobj_set_param("setMissAsEmpty", args[["miss_as_empty"]])  %>%
    sparklyr::jobj_set_param("setNeighbours", args[["neighbors"]])  %>%
    sparklyr::jobj_set_param("setThreshold", args[["threshold"]]) 
  
  new_nlp_sentence_entity_resolver_model(model)
}

nlp_float_params.nlp_sentence_entity_resolver <- function(x) {
  return(c())
}
new_nlp_sentence_entity_resolver <- function(jobj) {
  sparklyr::new_ml_estimator(jobj, class = "nlp_sentence_entity_resolver")
}
new_nlp_sentence_entity_resolver_model <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_sentence_entity_resolver_model")
}
nlp_float_params.nlp_sentence_entity_resolver_model <- function(x) {
  return(c())
}
