#' Spark NLP Perceptron
#'
#' Spark ML transformer that sets a POS tag to each word within a sentence. Its train data (train_pos) is a spark 
#' dataset of POS format values with Annotation columns.
#' See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#postagger}
#' 
#' @template roxlate-nlp-ml-algo
#' @template roxlate-inputs-output-params
#' @param n_iterations Number of iterations for training. May improve accuracy but takes longer. Default 5.
#' @param pos_column Column containing an array of POS Tags matching every token on the line.
#' 
#' @export
nlp_perceptron <- function(x, input_cols, output_col,
                 n_iterations = NULL, pos_column = NULL,
                 uid = random_string("perceptron_")) {
  UseMethod("nlp_perceptron")
}

#' @export
nlp_perceptron.spark_connection <- function(x, input_cols, output_col,
                 n_iterations = NULL, pos_column = NULL,
                 uid = random_string("perceptron_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    n_iterations = n_iterations,
    pos_column = pos_column,
    uid = uid
  ) %>%
  validator_nlp_perceptron()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.pos.perceptron.PerceptronApproach",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setInputCols", args[["input_cols"]]) %>% 
    sparklyr::jobj_set_param("setOutputCol", args[["output_col"]]) %>% 
    sparklyr::jobj_set_param("setNIterations", args[["n_iterations"]]) %>% 
    sparklyr::jobj_set_param("setPosColumn", args[["pos_column"]])
  
  new_nlp_perceptron(jobj)
}

#' @export
nlp_perceptron.ml_pipeline <- function(x, input_cols, output_col,
                 n_iterations = NULL, pos_column = NULL,
                 uid = random_string("perceptron_")) {

  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    n_iterations = n_iterations,
    pos_column = pos_column,
    uid = uid
  ) %>%
    validator_nlp_perceptron()
  
  stage <- nlp_perceptron_pretrained(
    sc = sparklyr::spark_connection(x), 
    input_cols = input_cols,
    output_col = output_col)

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_perceptron.tbl_spark <- function(x, input_cols, output_col,
                 n_iterations = NULL, pos_column = NULL,
                 uid = random_string("perceptron_")) {
  stage <- nlp_perceptron.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    n_iterations = n_iterations,
    pos_column = pos_column,
    uid = uid
  )

  stage %>% sparklyr::ml_fit(x)
}

#' Load a pretrained Spark NLP model
#' 
#' Create a pretrained Spark NLP \code{Perceptron} model
#' 
#' @template roxlate-pretrained-params
#' @template roxlate-inputs-output-params
#' @export
nlp_perceptron_pretrained <- function(sc, input_cols, output_col,
                                      name = NULL, lang = NULL, remote_loc = NULL) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col
  ) %>%
    validator_nlp_perceptron()
  
  model_class <- "com.johnsnowlabs.nlp.annotators.pos.perceptron.PerceptronModel"
  model <- pretrained_model(sc, model_class, name, lang, remote_loc)
  spark_jobj(model) %>%
    sparklyr::jobj_set_param("setInputCols", args[["input_cols"]]) %>% 
    sparklyr::jobj_set_param("setOutputCol", args[["output_col"]])
}

#' @import forge
validator_nlp_perceptron <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["n_iterations"]] <- cast_nullable_integer(args[["n_iterations"]])
  args[["pos_column"]] <- cast_nullable_string(args[["pos_column"]])
  args
}

new_nlp_perceptron <- function(jobj) {
  sparklyr::new_ml_estimator(jobj, class = "nlp_perceptron")
}

#' Read a part of speech tagging training file into a dataset
#' 
#' In order to train a Part of Speech Tagger annotator, we need to get corpus data as a spark dataframe. 
#' This function does this: it reads a plain text file and transforms it to a spark dataset that is ready
#' for training a POS tagger.
#' See the Scala API docs for the default parameter values (
#' \url{https://nlp.johnsnowlabs.com/api/index.html#com.johnsnowlabs.nlp.training.POS)}
#' 
#' @param sc Spark connection
#' @param file_path path to the text file with the training data
#' 
#' @return Spark dataframe containing the data
#' 
#' @export
nlp_pos <- function(sc, file_path, delimiter = NULL, output_pos_col = NULL, output_document_col = NULL, output_text_col = NULL) {
  module <- invoke_static(sc, "com.johnsnowlabs.nlp.training.POS$", "MODULE$")
  delimiter_default <- invoke(invoke(module, "apply"), "readDataset$default$3")
  output_pos_col_default <- invoke(invoke(module, "apply"), "readDataset$default$4")
  output_document_col_default <- invoke(invoke(module, "apply"), "readDataset$default$5")
  output_text_col_default <- invoke(invoke(module, "apply"), "readDataset$default$6")
  
  if (is.null(delimiter)) delimiter <- delimiter_default
  if (is.null(output_pos_col)) output_pos_col <- output_pos_col_default
  if (is.null(output_document_col)) output_document_col <- output_document_col_default
  if (is.null(output_text_col)) output_text_col <- output_text_col_default
  
  pos <- invoke_new(sc, "com.johnsnowlabs.nlp.training.POS")
  invoke(pos, "readDataset", spark_session(sc), file_path, delimiter, output_pos_col, output_document_col, output_text_col) %>%
    sdf_register()
}