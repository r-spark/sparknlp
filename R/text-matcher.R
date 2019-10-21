#' Spark NLP TextMatcher phrase matching
#'
#' Spark ML transformer to match entire phrases (by token) provided in a file against a Document
#' See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#textmatcher}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param path a path to a file that contains the entities in the specified format.
#' @param read_as the format of the file, can be one of {ReadAs.LINE_BY_LINE, ReadAs.SPARK_DATASET}. Defaults to LINE_BY_LINE.
#' @param options a map of additional parameters. Defaults to {“format”: “text”}. NOTE THIS IS CURRENTLY NOT USED. (see
#' \url{https://github.com/rstudio/sparklyr/issues/1058})
#' 
#' @return When \code{x} is a \code{spark_connection} the function returns a TextMatcher transformer.
#' When \code{x} is a \code{ml_pipeline} the pipeline with the TextMatcher added. When \code{x}
#' is a \code{tbl_spark} a transformed \code{tbl_spark}  (note that the Dataframe passed in must have the input_cols specified).
#' 
#' @export
nlp_text_matcher <- function(x, input_cols, output_col,
                 path, read_as = "LINE_BY_LINE", options = NULL,
                 uid = random_string("text_matcher_")) {
  UseMethod("nlp_text_matcher")
}

#' @export
nlp_text_matcher.spark_connection <- function(x, input_cols, output_col,
                 path, read_as = "LINE_BY_LINE", options = NULL,
                 uid = random_string("text_matcher_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    path = path,
    read_as = read_as,
    options = options,
    uid = uid
  ) %>%
  validator_nlp_text_matcher()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.TextMatcher",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::invoke("setEntities", args[["path"]], read_as(args[["read_as"]]), NULL)

  new_nlp_text_matcher(jobj)
}

#' @export
nlp_text_matcher.ml_pipeline <- function(x, input_cols, output_col,
                 path, read_as = "LINE_BY_LINE", options = NULL,
                 uid = random_string("text_matcher_")) {

  stage <- nlp_text_matcher.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    path = path,
    read_as = read_as,
    options = options,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_text_matcher.tbl_spark <- function(x, input_cols, output_col,
                 path, read_as = "LINE_BY_LINE", options = NULL,
                 uid = random_string("text_matcher_")) {
  stage <- nlp_text_matcher.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    path = path,
    read_as = read_as,
    options = options,
    uid = uid
  )

  stage %>% sparklyr::ml_fit_and_transform(x)
}
#' @import forge
validator_nlp_text_matcher <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["path"]] <- cast_string(args[["path"]])
  args[["read_as"]] <- cast_choice(args[["read_as"]], choices = c("LINE_BY_LINE", "SPARK_DATASET"), allow_null = TRUE)
  args[["options"]] <- cast_nullable_string_list(args[["options"]])
  args
}

new_nlp_text_matcher <- function(jobj) {
  sparklyr::new_ml_estimator(jobj, class = "nlp_text_matcher")
}
