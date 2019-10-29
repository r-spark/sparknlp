#' Spark NLP DateMatcher
#'
#' Spark ML transformer that reads from different forms of date and time expressions and converts them to a provided 
#' date format. Extracts only ONE date per sentence. Use with sentence detector for more matches.
#' See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#datematcher}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param format SimpleDateFormat standard date output formatting. Defaults to yyyy/MM/dd
#' 
#' @export
nlp_date_matcher <- function(x, input_cols, output_col,
                 format = NULL,
                 uid = random_string("date_matcher_")) {
  UseMethod("nlp_date_matcher")
}

#' @export
nlp_date_matcher.spark_connection <- function(x, input_cols, output_col,
                 format = NULL,
                 uid = random_string("date_matcher_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    format = format,
    uid = uid
  ) %>%
  validator_nlp_date_matcher()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.DateMatcher",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setFormat", args[["format"]])

  new_nlp_date_matcher(jobj)
}

#' @export
nlp_date_matcher.ml_pipeline <- function(x, input_cols, output_col,
                 format = NULL,
                 uid = random_string("date_matcher_")) {

  stage <- nlp_date_matcher.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    format = format,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_date_matcher.tbl_spark <- function(x, input_cols, output_col,
                 format = NULL,
                 uid = random_string("date_matcher_")) {
  stage <- nlp_date_matcher.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    format = format,
    uid = uid
  )

  stage %>% sparklyr::ml_transform(x)
}
#' @import forge
validator_nlp_date_matcher <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["format"]] <- cast_nullable_string(args[["format"]])
  args
}

new_nlp_date_matcher <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_date_matcher")
}
