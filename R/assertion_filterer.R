#' Spark NLP AssertionFilterer
#'
#' Spark ML transformer that will allow you to filter out the named entities by
#' the list of acceptable assertion statuses. This annotator would be quite handy
#' if you want to set a white list for the acceptable assertion statuses like 
#' present or conditional; and do not want absent conditions get out of your pipeline.
#' See \url{https://nlp.johnsnowlabs.com/docs/en/licensed_release_notes#3-assertionfilterer}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param criteria isin or regex
#' @param whitelist If defined, list of entities to process.
#' @param regex If defined, list of entities to process.
#' 
#' @export
nlp_assertion_filterer <- function(x, input_cols, output_col,
                 criteria = NULL, whitelist = NULL, regex = NULL,
                 uid = random_string("assertion_filterer_")) {
  UseMethod("nlp_assertion_filterer")
}

#' @export
nlp_assertion_filterer.spark_connection <- function(x, input_cols, output_col,
                 criteria = NULL, whitelist = NULL, regex = NULL,
                 uid = random_string("assertion_filterer_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    criteria = criteria,
    whitelist = whitelist,
    regex = regex,
    uid = uid
  ) %>%
  validator_nlp_assertion_filterer()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.chunker.AssertionFilterer",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("seCriteria", args[["criteria"]])  %>%
    sparklyr::jobj_set_param("setWhiteList", args[["whitelist"]])  %>%
    sparklyr::jobj_set_param("setRegex", args[["regex"]]) 

  new_nlp_assertion_filterer(jobj)
}

#' @export
nlp_assertion_filterer.ml_pipeline <- function(x, input_cols, output_col,
                 criteria = NULL, whitelist = NULL, regex = NULL,
                 uid = random_string("assertion_filterer_")) {

  stage <- nlp_assertion_filterer.spark_connection(
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
nlp_assertion_filterer.tbl_spark <- function(x, input_cols, output_col,
                 criteria = NULL, whitelist = NULL, regex = NULL,
                 uid = random_string("assertion_filterer_")) {
  stage <- nlp_assertion_filterer.spark_connection(
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
validator_nlp_assertion_filterer <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["criteria"]] <- cast_nullable_string(args[["criteria"]])
  args[["whitelist"]] <- cast_nullable_string_list(args[["whitelist"]])
  args[["regex"]] <- cast_nullable_string_list(args[["regex"]])
  args
}

nlp_float_params.nlp_assertion_filterer <- function(x) {
  return(c())
}
new_nlp_assertion_filterer <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_assertion_filterer")
}
