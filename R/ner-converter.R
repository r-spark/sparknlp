#' Spark NLP NerConverter
#'
#' Spark ML transformer that converts IOB or IOB2 representation of NER to user-friendly. 
#' 
#' @template roxlate-nlp-transformer
#' @template roxlate-inputs-output-params
#' @param white_list If defined, list of entities to process. The rest will be ignored. Do not include IOB prefix on labels"
#' 
#' @return When \code{x} is a \code{spark_connection} the function returns a NerConverter transformer.
#' When \code{x} is a \code{ml_pipeline} the pipeline with the NerConverter added. When \code{x}
#' is a \code{tbl_spark} a transformed \code{tbl_spark}  (note that the Dataframe passed in must have the input_cols specified).
#' 
#' @export
nlp_ner_converter <- function(x, input_cols, output_col,
                 white_list = NULL,
                 uid = random_string("ner_converter_")) {
  UseMethod("nlp_ner_converter")
}

#' @export
nlp_ner_converter.spark_connection <- function(x, input_cols, output_col,
                 white_list = NULL,
                 uid = random_string("ner_converter_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    white_list = white_list,
    uid = uid
  ) %>%
  validator_nlp_ner_converter()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.ner.NerConverter",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setWhiteList", args[["white_list"]]) 

  new_nlp_ner_converter(jobj)
}

#' @export
nlp_ner_converter.ml_pipeline <- function(x, input_cols, output_col,
                 white_list = NULL,
                 uid = random_string("ner_converter_")) {

  stage <- nlp_ner_converter.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    white_list = white_list,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_ner_converter.tbl_spark <- function(x, input_cols, output_col,
                 white_list = NULL,
                 uid = random_string("ner_converter_")) {
  stage <- nlp_ner_converter.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    white_list = white_list,
    uid = uid
  )

  stage %>% sparklyr::ml_transform(x)
}
#' @import forge
validator_nlp_ner_converter <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["white_list"]] <- cast_nullable_string_list(args[["white_list"]])
  args
}

new_nlp_ner_converter <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_ner_converter")
}
