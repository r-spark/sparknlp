#' Spark NLP T5Transformer
#'
#' Spark ML transformer that 
#' See \url{https://nlp.johnsnowlabs.com/api/#com.johnsnowlabs.nlp.annotators.seq2seq.T5Transformer}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param task name to give the task being performed
#' @param max_output_length maximum output length
#' 
#' @export
nlp_t5_transformer <- function(x, input_cols, output_col,
                 task = NULL, max_output_length = NULL,
                 uid = random_string("t5_transformer_")) {
  UseMethod("nlp_t5_transformer")
}

#' @export
nlp_t5_transformer.spark_connection <- function(x, input_cols, output_col,
                 task = NULL, max_output_length = NULL,
                 uid = random_string("t5_transformer_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    task = task,
    max_output_length = max_output_length,
    uid = uid
  ) %>%
  validator_nlp_t5_transformer()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.seq2seq.T5Transformer",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setTask", args[["task"]])  %>%
    sparklyr::jobj_set_param("setMaxOutputLength", args[["max_output_length"]]) 

  new_nlp_t5_transformer(jobj)
}

#' @export
nlp_t5_transformer.ml_pipeline <- function(x, input_cols, output_col,
                 task = NULL, max_output_length = NULL,
                 uid = random_string("t5_transformer_")) {

  stage <- nlp_t5_transformer.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    task = task,
    max_output_length = max_output_length,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_t5_transformer.tbl_spark <- function(x, input_cols, output_col,
                 task = NULL, max_output_length = NULL,
                 uid = random_string("t5_transformer_")) {
  stage <- nlp_t5_transformer.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    task = task,
    max_output_length = max_output_length,
    uid = uid
  )

  stage %>% sparklyr::ml_transform(x)
}
#' @import forge
validator_nlp_t5_transformer <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["task"]] <- cast_nullable_string(args[["task"]])
  args[["max_output_length"]] <- cast_nullable_integer(args[["max_output_length"]])
  args
}

#' Load a pretrained Spark NLP T5 Transformer model
#' 
#' Create a pretrained Spark NLP \code{T5TransformerModel} model
#' 
#' @template roxlate-pretrained-params
#' @template roxlate-inputs-output-params
#' @param task name to give the task being performed
#' @param max_output_length the maximum output length
#' 
#' @export
nlp_t5_transformer_pretrained <- function(sc, input_cols, output_col, task = NULL, max_output_length = NULL,
                                              name = NULL, lang = NULL, remote_loc = NULL) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    task = task,
    max_output_length = max_output_length
  )
  
  args[["input_cols"]] <- forge::cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- forge::cast_string(args[["output_col"]])
  args[["task"]] <- forge::cast_nullable_string(args[["task"]])
  args[["max_output_length"]] <- forge::cast_nullable_integer(args[["max_output_length"]])
  
  model_class <- "com.johnsnowlabs.nlp.annotators.seq2seq.T5Transformer"
  model <- pretrained_model(sc, model_class, name, lang, remote_loc)
  spark_jobj(model) %>%
    sparklyr::jobj_set_param("setInputCols", args[["input_cols"]]) %>% 
    sparklyr::jobj_set_param("setOutputCol", args[["output_col"]]) %>% 
    sparklyr::jobj_set_param("setTask", args[["task"]]) %>%
    sparklyr::jobj_set_param("setMaxOutputLength", args[["max_output_length"]])
    
  new_nlp_t5_transformer(model)
}

nlp_float_params.nlp_t5_transformer <- function(x) {
  return(c())
}
new_nlp_t5_transformer <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_t5_transformer")
}
