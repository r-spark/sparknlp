#' Spark NLP ChunkFilterer
#'
#' Spark ML transformer that will filter out named entities by some conditions
#' or predefined look-up lists, so that you can feed these entities to other
#' annotators like Assertion Status or Entity Resolvers. It can be used with 
#' two criteria: isin and regex.
#' See \url{https://nlp.johnsnowlabs.com/docs/en/licensed_release_notes#2-chunkfilterer}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param criteria isin or regex
#' @param whitelist If defined, list of entities to process.
#' @param regex If defined, list of entities to process.
#' 
#' @export
nlp_chunk_filterer <- function(x, input_cols, output_col,
                 criteria = NULL, whitelist = NULL, regex = NULL,
                 uid = random_string("chunk_filterer_")) {
  UseMethod("nlp_chunk_filterer")
}

#' @export
nlp_chunk_filterer.spark_connection <- function(x, input_cols, output_col,
                 criteria = NULL, whitelist = NULL, regex = NULL,
                 uid = random_string("chunk_filterer_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    criteria = criteria,
    whitelist = whitelist,
    regex = regex,
    uid = uid
  ) %>%
  validator_nlp_chunk_filterer()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.chunker.ChunkFilterer",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("seCriteria", args[["criteria"]])  %>%
    sparklyr::jobj_set_param("setWhiteList", args[["whitelist"]])  %>%
    sparklyr::jobj_set_param("setRegex", args[["regex"]]) 

  new_nlp_chunk_filterer(jobj)
}

#' @export
nlp_chunk_filterer.ml_pipeline <- function(x, input_cols, output_col,
                 criteria = NULL, whitelist = NULL, regex = NULL,
                 uid = random_string("chunk_filterer_")) {

  stage <- nlp_chunk_filterer.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    criteria = criteria,
    whitelist = whitelist,
    regex = regex,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_chunk_filterer.tbl_spark <- function(x, input_cols, output_col,
                 criteria = NULL, whitelist = NULL, regex = NULL,
                 uid = random_string("chunk_filterer_")) {
  stage <- nlp_chunk_filterer.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    criteria = criteria,
    whitelist = whitelist,
    regex = regex,
    uid = uid
  )

  stage %>% sparklyr::ml_transform(x)
}
#' @import forge
validator_nlp_chunk_filterer <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["criteria"]] <- cast_nullable_string(args[["criteria"]])
  args[["whitelist"]] <- cast_nullable_string_list(args[["whitelist"]])
  args[["regex"]] <- cast_nullable_string_list(args[["regex"]])
  args
}

nlp_float_params.nlp_chunk_filterer <- function(x) {
  return(c())
}
new_nlp_chunk_filterer <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_chunk_filterer")
}
