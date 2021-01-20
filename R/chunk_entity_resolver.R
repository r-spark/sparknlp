#' Spark NLP ChunkEntityResolverApproach
#'
#' Spark ML estimator that 
#' See \url{https://nlp.johnsnowlabs.com/docs/en/licensed_annotators#chunkentityresolver}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param all_distances_metadata whether or not to return an all distance values in the metadata.
#' @param alternatives number of results to return in the metadata after sorting by last distance calculated
#' @param case_sensitive whether to treat the entities as case sensitive
#' @param confidence_function what function to use to calculate confidence: INVERSE or SOFTMAX
#' @param distance_function what distance function to use for KNN: 'EUCLIDEAN' or 'COSINE'
#' @param distance_weights distance weights to apply before pooling: (WMD, TFIDF, Jaccard, SorensenDice, JaroWinkler, Levenshtein) 
#' @param enable_jaccard whether or not to use Jaccard token distance.
#' @param enable_jaro_winkler whether or not to use Jaro-Winkler character distance.
#' @param enable_levenshtein whether or not to use Levenshtein character distance.
#' @param enable_sorensen_dice whether or not to use Sorensen-Dice token distance.
#' @param enable_tfidf whether or not to use TFIDF token distance.
#' @param enable_wmd whether or not to use WMD token distance.
#' @param extra_mass_penalty penalty for extra words in the knowledge base match during WMD calculation
#' @param label_column column name for the value we are trying to resolve
#' @param miss_as_empty whether or not to return an empty annotation on unmatched chunks
#' @param neighbors number of neighbours to consider in the KNN query to calculate WMD
#' @param normalized_col column name for the original, normalized description
#' @param pooling_strategy pooling strategy to aggregate distances: AVERAGE or SUM
#' @param threshold threshold value for the aggregated distance
#' 
#' @export
nlp_chunk_entity_resolver <- function(x, input_cols, output_col,
                 all_distances_metadata = NULL, alternatives = NULL, case_sensitive = NULL, confidence_function = NULL, distance_function = NULL, distance_weights = NULL, enable_jaccard = NULL, enable_jaro_winkler = NULL, enable_levenshtein = NULL, enable_sorensen_dice = NULL, enable_tfidf = NULL, enable_wmd = NULL, extra_mass_penalty = NULL, label_column = NULL, miss_as_empty = NULL, neighbors = NULL, normalized_col = NULL, pooling_strategy = NULL, threshold = NULL,
                 uid = random_string("chunk_entity_resolver_")) {
  UseMethod("nlp_chunk_entity_resolver")
}

#' @export
nlp_chunk_entity_resolver.spark_connection <- function(x, input_cols, output_col,
                 all_distances_metadata = NULL, alternatives = NULL, case_sensitive = NULL, confidence_function = NULL, distance_function = NULL, distance_weights = NULL, enable_jaccard = NULL, enable_jaro_winkler = NULL, enable_levenshtein = NULL, enable_sorensen_dice = NULL, enable_tfidf = NULL, enable_wmd = NULL, extra_mass_penalty = NULL, label_column = NULL, miss_as_empty = NULL, neighbors = NULL, normalized_col = NULL, pooling_strategy = NULL, threshold = NULL,
                 uid = random_string("chunk_entity_resolver_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    all_distances_metadata = all_distances_metadata,
    alternatives = alternatives,
    case_sensitive = case_sensitive,
    confidence_function = confidence_function,
    distance_function = distance_function,
    distance_weights = distance_weights,
    enable_jaccard = enable_jaccard,
    enable_jaro_winkler = enable_jaro_winkler,
    enable_levenshtein = enable_levenshtein,
    enable_sorensen_dice = enable_sorensen_dice,
    enable_tfidf = enable_tfidf,
    enable_wmd = enable_wmd,
    extra_mass_penalty = extra_mass_penalty,
    label_column = label_column,
    miss_as_empty = miss_as_empty,
    neighbors = neighbors,
    normalized_col = normalized_col,
    pooling_strategy = pooling_strategy,
    threshold = threshold,
    uid = uid
  ) %>%
  validator_nlp_chunk_entity_resolver()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.resolution.ChunkEntityResolverApproach",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setAllDistancesMetadata", args[["all_distances_metadata"]])  %>%
    sparklyr::jobj_set_param("setAlternatives", args[["alternatives"]])  %>%
    sparklyr::jobj_set_param("setCaseSensitive", args[["case_sensitive"]])  %>%
    sparklyr::jobj_set_param("setConfidenceFunction", args[["confidence_function"]])  %>%
    sparklyr::jobj_set_param("setDistanceFunction", args[["distance_function"]])  %>%
    sparklyr::jobj_set_param("setDistanceWeights", args[["distance_weights"]])  %>%
    sparklyr::jobj_set_param("setEnableJaccard", args[["enable_jaccard"]])  %>%
    sparklyr::jobj_set_param("setEnableJaroWinkler", args[["enable_jaro_winkler"]])  %>%
    sparklyr::jobj_set_param("setEnableLevenshtein", args[["enable_levenshtein"]])  %>%
    sparklyr::jobj_set_param("setEnableSorensenDice", args[["enable_sorensen_dice"]])  %>%
    sparklyr::jobj_set_param("setEnableTfidf", args[["enable_tfidf"]])  %>%
    sparklyr::jobj_set_param("setEnableWmd", args[["enable_wmd"]])  %>%
    sparklyr::jobj_set_param("setExtramassPenalty", args[["extra_mass_penalty"]])  %>%
    sparklyr::jobj_set_param("setLabelCol", args[["label_column"]])  %>%
    sparklyr::jobj_set_param("setMissAsEmpty", args[["miss_as_empty"]])  %>%
    sparklyr::jobj_set_param("setNeighbours", args[["neighbors"]])  %>%
    sparklyr::jobj_set_param("setNormalizedCol", args[["normalized_col"]])  %>%
    sparklyr::jobj_set_param("setPoolingStrategy", args[["pooling_strategy"]])  %>%
    sparklyr::jobj_set_param("setThreshold", args[["threshold"]]) 

  new_nlp_chunk_entity_resolver(jobj)
}

#' @export
nlp_chunk_entity_resolver.ml_pipeline <- function(x, input_cols, output_col,
                 all_distances_metadata = NULL, alternatives = NULL, case_sensitive = NULL, confidence_function = NULL, distance_function = NULL, distance_weights = NULL, enable_jaccard = NULL, enable_jaro_winkler = NULL, enable_levenshtein = NULL, enable_sorensen_dice = NULL, enable_tfidf = NULL, enable_wmd = NULL, extra_mass_penalty = NULL, label_column = NULL, miss_as_empty = NULL, neighbors = NULL, normalized_col = NULL, pooling_strategy = NULL, threshold = NULL,
                 uid = random_string("chunk_entity_resolver_")) {

  stage <- nlp_chunk_entity_resolver.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    all_distances_metadata = all_distances_metadata,
    alternatives = alternatives,
    case_sensitive = case_sensitive,
    confidence_function = confidence_function,
    distance_function = distance_function,
    distance_weights = distance_weights,
    enable_jaccard = enable_jaccard,
    enable_jaro_winkler = enable_jaro_winkler,
    enable_levenshtein = enable_levenshtein,
    enable_sorensen_dice = enable_sorensen_dice,
    enable_tfidf = enable_tfidf,
    enable_wmd = enable_wmd,
    extra_mass_penalty = extra_mass_penalty,
    label_column = label_column,
    miss_as_empty = miss_as_empty,
    neighbors = neighbors,
    normalized_col = normalized_col,
    pooling_strategy = pooling_strategy,
    threshold = threshold,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_chunk_entity_resolver.tbl_spark <- function(x, input_cols, output_col,
                 all_distances_metadata = NULL, alternatives = NULL, case_sensitive = NULL, confidence_function = NULL, distance_function = NULL, distance_weights = NULL, enable_jaccard = NULL, enable_jaro_winkler = NULL, enable_levenshtein = NULL, enable_sorensen_dice = NULL, enable_tfidf = NULL, enable_wmd = NULL, extra_mass_penalty = NULL, label_column = NULL, miss_as_empty = NULL, neighbors = NULL, normalized_col = NULL, pooling_strategy = NULL, threshold = NULL,
                 uid = random_string("chunk_entity_resolver_")) {
  stage <- nlp_chunk_entity_resolver.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    all_distances_metadata = all_distances_metadata,
    alternatives = alternatives,
    case_sensitive = case_sensitive,
    confidence_function = confidence_function,
    distance_function = distance_function,
    distance_weights = distance_weights,
    enable_jaccard = enable_jaccard,
    enable_jaro_winkler = enable_jaro_winkler,
    enable_levenshtein = enable_levenshtein,
    enable_sorensen_dice = enable_sorensen_dice,
    enable_tfidf = enable_tfidf,
    enable_wmd = enable_wmd,
    extra_mass_penalty = extra_mass_penalty,
    label_column = label_column,
    miss_as_empty = miss_as_empty,
    neighbors = neighbors,
    normalized_col = normalized_col,
    pooling_strategy = pooling_strategy,
    threshold = threshold,
    uid = uid
  )

  stage %>% sparklyr::ml_fit_and_transform(x)
}
#' @import forge
validator_nlp_chunk_entity_resolver <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["all_distances_metadata"]] <- cast_nullable_logical(args[["all_distances_metadata"]])
  args[["alternatives"]] <- cast_nullable_integer(args[["alternatives"]])
  args[["case_sensitive"]] <- cast_nullable_logical(args[["case_sensitive"]])
  args[["confidence_function"]] <- cast_nullable_string(args[["confidence_function"]])
  args[["distance_function"]] <- cast_nullable_string(args[["distance_function"]])
  args[["distance_weights"]] <- cast_nullable_double_list(args[["distance_weights"]])
  args[["enable_jaccard"]] <- cast_nullable_logical(args[["enable_jaccard"]])
  args[["enable_jaro_winkler"]] <- cast_nullable_logical(args[["enable_jaro_winkler"]])
  args[["enable_levenshtein"]] <- cast_nullable_logical(args[["enable_levenshtein"]])
  args[["enable_sorensen_dice"]] <- cast_nullable_logical(args[["enable_sorensen_dice"]])
  args[["enable_tfidf"]] <- cast_nullable_logical(args[["enable_tfidf"]])
  args[["enable_wmd"]] <- cast_nullable_logical(args[["enable_wmd"]])
  args[["extra_mass_penalty"]] <- cast_nullable_double(args[["extra_mass_penalty"]])
  args[["label_column"]] <- cast_nullable_string(args[["label_column"]])
  args[["miss_as_empty"]] <- cast_nullable_logical(args[["miss_as_empty"]])
  args[["neighbors"]] <- cast_nullable_integer(args[["neighbors"]])
  args[["normalized_col"]] <- cast_nullable_string(args[["normalized_col"]])
  args[["pooling_strategy"]] <- cast_nullable_string(args[["pooling_strategy"]])
  args[["threshold"]] <- cast_nullable_double(args[["threshold"]])
  args
}

#' Load a pretrained Spark NLP T5 Transformer model
#' 
#' Create a pretrained Spark NLP \code{T5TransformerModel} model
#' 
#' @template roxlate-pretrained-params
#' @template roxlate-inputs-output-params
#' @param all_distances_metadata whether or not to return an all distance values in the metadata.
#' @param alternatives number of results to return in the metadata after sorting by last distance calculated
#' @param case_sensitive whether to treat the entities as case sensitive
#' @param confidence_function what function to use to calculate confidence: INVERSE or SOFTMAX
#' @param distance_function what distance function to use for KNN: 'EUCLIDEAN' or 'COSINE'
#' @param distance_weights distance weights to apply before pooling: (WMD, TFIDF, Jaccard, SorensenDice, JaroWinkler, Levenshtein) 
#' @param enable_jaccard whether or not to use Jaccard token distance.
#' @param enable_jaro_winkler whether or not to use Jaro-Winkler character distance.
#' @param enable_levenshtein whether or not to use Levenshtein character distance.
#' @param enable_sorensen_dice whether or not to use Sorensen-Dice token distance.
#' @param enable_tfidf whether or not to use TFIDF token distance.
#' @param enable_wmd whether or not to use WMD token distance.
#' @param extra_mass_penalty penalty for extra words in the knowledge base match during WMD calculation
#' @param miss_as_empty whether or not to return an empty annotation on unmatched chunks
#' @param neighbors number of neighbours to consider in the KNN query to calculate WMD
#' @param pooling_strategy pooling strategy to aggregate distances: AVERAGE or SUM
#' @param threshold threshold value for the aggregated distance#' 
#' 
#' @export
nlp_chunk_entity_resolver_pretrained <- function(sc, input_cols, output_col, 
                                                 all_distances_metadata = NULL, alternatives = NULL, case_sensitive = NULL, confidence_function = NULL, distance_function = NULL, distance_weights = NULL, enable_jaccard = NULL, enable_jaro_winkler = NULL, enable_levenshtein = NULL, enable_sorensen_dice = NULL, enable_tfidf = NULL, enable_wmd = NULL, extra_mass_penalty = NULL, miss_as_empty = NULL, neighbors = NULL, pooling_strategy = NULL, threshold = NULL,
                                          name, lang = NULL, remote_loc = NULL) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    all_distances_metadata = all_distances_metadata,
    alternatives = alternatives,
    case_sensitive = case_sensitive,
    confidence_function = confidence_function,
    distance_function = distance_function,
    distance_weights = distance_weights,
    enable_jaccard = enable_jaccard,
    enable_jaro_winkler = enable_jaro_winkler,
    enable_levenshtein = enable_levenshtein,
    enable_sorensen_dice = enable_sorensen_dice,
    enable_tfidf = enable_tfidf,
    enable_wmd = enable_wmd,
    extra_mass_penalty = extra_mass_penalty,
    miss_as_empty = miss_as_empty,
    neighbors = neighbors,
    pooling_strategy = pooling_strategy,
    threshold = threshold
  ) %>%
    validator_nlp_chunk_entity_resolver()
  
  model_class <- "com.johnsnowlabs.nlp.annotators.resolution.ChunkEntityResolverModel"
  model <- pretrained_model(sc, model_class, name, lang, remote_loc)
  spark_jobj(model) %>%
    sparklyr::jobj_set_param("setInputCols", args[["input_cols"]]) %>% 
    sparklyr::jobj_set_param("setOutputCol", args[["output_col"]]) %>% 
    sparklyr::jobj_set_param("setAllDistancesMetadata", args[["all_distances_metadata"]])  %>%
    sparklyr::jobj_set_param("setAlternatives", args[["alternatives"]])  %>%
    sparklyr::jobj_set_param("setCaseSensitive", args[["case_sensitive"]])  %>%
    sparklyr::jobj_set_param("setConfidenceFunction", args[["confidence_function"]])  %>%
    sparklyr::jobj_set_param("setDistanceFunction", args[["distance_function"]])  %>%
    sparklyr::jobj_set_param("setDistanceWeights", args[["distance_weights"]])  %>%
    sparklyr::jobj_set_param("setEnableJaccard", args[["enable_jaccard"]])  %>%
    sparklyr::jobj_set_param("setEnableJaroWinkler", args[["enable_jaro_winkler"]])  %>%
    sparklyr::jobj_set_param("setEnableLevenshtein", args[["enable_levenshtein"]])  %>%
    sparklyr::jobj_set_param("setEnableSorensenDice", args[["enable_sorensen_dice"]])  %>%
    sparklyr::jobj_set_param("setEnableTfidf", args[["enable_tfidf"]])  %>%
    sparklyr::jobj_set_param("setEnableWmd", args[["enable_wmd"]])  %>%
    sparklyr::jobj_set_param("setExtramassPenalty", args[["extra_mass_penalty"]])  %>%
    sparklyr::jobj_set_param("setMissAsEmpty", args[["miss_as_empty"]])  %>%
    sparklyr::jobj_set_param("setNeighbours", args[["neighbors"]])  %>%
    sparklyr::jobj_set_param("setPoolingStrategy", args[["pooling_strategy"]])  %>%
    sparklyr::jobj_set_param("setThreshold", args[["threshold"]]) 
  
  new_nlp_chunk_entity_resolver_model(model)
}

nlp_float_params.nlp_chunk_entity_resolver <- function(x) {
  return(c())
}
new_nlp_chunk_entity_resolver <- function(jobj) {
  sparklyr::new_ml_estimator(jobj, class = "nlp_chunk_entity_resolver")
}
new_nlp_chunk_entity_resolver_model <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_chunk_entity_resolver_model")
}
nlp_float_params.nlp_chunk_entity_resolver_model <- function(x) {
  return(c())
}
