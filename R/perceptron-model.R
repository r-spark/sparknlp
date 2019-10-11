#' Spark NLP PerceptronModel
#'
#' Spark ML transformer that 
#' See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#postagger}
#' 
#' @param x 
#' @param input_cols 
#' @param output_col 
#' @param n_iterations 
#' @param pos_column 
#' @param uid 
#' 
#' @return When \code{x} is a \code{spark_connection} the function returns a PerceptronModel transformer.
#' When \code{x} is a \code{ml_pipeline} the pipeline with the PerceptronModel added. When \code{x}
#' is a \code{tbl_spark} a transformed \code{tbl_spark}  (note that the Dataframe passed in must have the input_cols specified).
#' 
#' @export
nlp_perceptron_model <- function(x, input_cols, output_col,
                 n_iterations = NULL, pos_column = NULL,
                 uid = random_string("perceptron_model_")) {
  UseMethod("nlp_perceptron_model")
}

#' @export
nlp_perceptron_model.spark_connection <- function(x, input_cols, output_col,
                 n_iterations = NULL, pos_column = NULL,
                 uid = random_string("perceptron_model_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    n_iterations = n_iterations,
    pos_column = pos_column,
    uid = uid
  ) %>%
  validator_nlp_perceptron_model()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.pos.perceptron.PerceptronModel",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setNIterations", args[["n_iterations"]])  %>%
    sparklyr::jobj_set_param("setPosColumn", args[["pos_column"]]) 

  new_nlp_perceptron_model(jobj)
}

#' @export
nlp_perceptron_model.ml_pipeline <- function(x, input_cols, output_col,
                 n_iterations = NULL, pos_column = NULL,
                 uid = random_string("perceptron_model_")) {

  stage <- nlp_perceptron_model.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    n_iterations = n_iterations,
    pos_column = pos_column,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_perceptron_model.tbl_spark <- function(x, input_cols, output_col,
                 n_iterations = NULL, pos_column = NULL,
                 uid = random_string("perceptron_model_")) {
  stage <- nlp_perceptron_model.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    n_iterations = n_iterations,
    pos_column = pos_column,
    uid = uid
  )

  stage %>% sparklyr::ml_transform(x)
}
#' @import forge
validator_nlp_perceptron_model <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["n_iterations"]] <- cast_nullable_integer(args[["n_iterations"]])
  args[["pos_column"]] <- cast_nullable_string(args[["pos_column"]])
  args
}

new_nlp_perceptron_model <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_perceptron_model")
}
