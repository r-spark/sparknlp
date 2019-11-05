#' Spark NLP TokenAssembler
#'
#' Spark ML transformer that 
#' See \url{https://nlp.johnsnowlabs.com/docs/en/transformers#tokenassembler-getting-data-reshaped}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' 
#' @export
nlp_token_assembler <- function(x, input_cols, output_col,
                 uid = random_string("token_assembler_")) {
  UseMethod("nlp_token_assembler")
}

#' 
#' @export
nlp_token_assembler <- function(x, input_cols, output_col,
                 uid = random_string("token_assembler_")) {
  UseMethod("nlp_token_assembler")
}

#' @export
nlp_token_assembler.spark_connection <- function(x, input_cols, output_col,
                 uid = random_string("token_assembler_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    uid = uid
  ) %>%
  validator_nlp_token_assembler()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.TokenAssembler",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  )

  new_nlp_token_assembler(jobj)
}

#' @export
nlp_token_assembler.ml_pipeline <- function(x, input_cols, output_col,
                 uid = random_string("token_assembler_")) {

  stage <- nlp_token_assembler.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_token_assembler.tbl_spark <- function(x, input_cols, output_col,
                 uid = random_string("token_assembler_")) {
  stage <- nlp_token_assembler.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    uid = uid
  )

  stage %>% sparklyr::ml_transform(x)
}
#' @import forge
validator_nlp_token_assembler <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args
}

new_nlp_token_assembler <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_token_assembler")
}
