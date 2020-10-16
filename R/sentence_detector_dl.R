#' Spark NLP SentenceDetectorDLApproach
#'
#' Spark ML estimator that 
#' See \url{https://nlp.johnsnowlabs.com/docs/en/annotators}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param epochs_number maximum number of epochs to train
#' @param impossible_penultimates impossible penultimates
#' @param model model architecture
#' @param output_logs_path path to folder to output logs
#' @param validation_split choose the proportion of training dataset to be validated agaisnt the model on each epoch
#' 
#' @export
nlp_sentence_detector_dl <- function(x, input_cols, output_col,
                 epochs_number = NULL, impossible_penultimates = NULL, model = NULL,
                 output_logs_path = NULL, validation_split = NULL,
                 uid = random_string("sentence_detector_dl_")) {
  UseMethod("nlp_sentence_detector_dl")
}

#' @export
nlp_sentence_detector_dl.spark_connection <- function(x, input_cols, output_col,
                 epochs_number = NULL, impossible_penultimates = NULL, model = NULL,
                 output_logs_path = NULL, validation_split = NULL,
                 uid = random_string("sentence_detector_dl_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    epochs_number = epochs_number,
    impossible_penultimates = impossible_penultimates,
    model = model,
    output_logs_path = output_logs_path,
    validation_split = validation_split,
    uid = uid
  ) %>%
  validator_nlp_sentence_detector_dl()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.sentence_detector_dl.SentenceDetectorDLApproach",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setEpochsNumber", args[["epochs_number"]])  %>%
    sparklyr::jobj_set_param("setImpossiblePenultimates", args[["impossible_penultimates"]])  %>%
    sparklyr::jobj_set_param("setModel", args[["model"]])  %>%
    sparklyr::jobj_set_param("setOutputLogsPath", args[["output_logs_path"]])

    model <- new_nlp_sentence_detector_dl(jobj)
    
    if (!is.null(args[["validation_split"]])) {
      model <- nlp_set_param(model, "validation_split", args[["validation_split"]])
    }
    
    return(model)
}

#' @export
nlp_sentence_detector_dl.ml_pipeline <- function(x, input_cols, output_col,
                 epochs_number = NULL, impossible_penultimates = NULL, model = NULL,
                 output_logs_path = NULL, validation_split = NULL,
                 uid = random_string("sentence_detector_dl_")) {

  stage <- nlp_sentence_detector_dl.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    epochs_number = epochs_number,
    impossible_penultimates = impossible_penultimates,
    model = model,
    output_logs_path = output_logs_path,
    validation_split = validation_split,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_sentence_detector_dl.tbl_spark <- function(x, input_cols, output_col,
                 epochs_number = NULL, impossible_penultimates = NULL, model = NULL,
                 output_logs_path = NULL, validation_split = NULL,
                 uid = random_string("sentence_detector_dl_")) {
  stage <- nlp_sentence_detector_dl.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    epochs_number = epochs_number,
    impossible_penultimates = impossible_penultimates,
    model = model,
    output_logs_path = output_logs_path,
    validation_split = validation_split,
    uid = uid
  )

  stage %>% sparklyr::ml_fit_and_transform(x)
}
#' @import forge
validator_nlp_sentence_detector_dl <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["epochs_number"]] <- cast_nullable_integer(args[["epochs_number"]])
  args[["impossible_penultimates"]] <- cast_nullable_string_list(args[["impossible_penultimates"]])
  args[["model"]] <- cast_nullable_string(args[["model"]])
  args[["output_logs_path"]] <- cast_nullable_string(args[["output_logs_path"]])
  args[["validation_split"]] <- cast_nullable_double(args[["validation_split"]])
  args
}

#' Load a pretrained Spark NLP Sentence Detector DL model
#' 
#' Create a pretrained Spark NLP \code{SentenceDetectorDLModel} model
#' 
#' @template roxlate-pretrained-params
#' @template roxlate-inputs-output-params
#' @param impossible_penultimates impossible penultimates
#' @param model model architecture
#' 
#' @export
nlp_sentence_detector_dl_pretrained <- function(sc, input_cols, output_col, impossible_penultimates = NULL,
                                  model = NULL,
                                  name = NULL, lang = NULL, remote_loc = NULL) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col
  )
  
  args[["input_cols"]] <- forge::cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- forge::cast_string(args[["output_col"]])
  args[["impossible_penultimates"]] <- forge::cast_nullable_string_list(args[["impossible_penultimates"]])
  args[["model"]] <- forge::cast_nullable_string(args[["model"]])
  
  model_class <- "com.johnsnowlabs.nlp.annotators.sentence_detector_dl.SentenceDetectorDLModel"
  model <- pretrained_model(sc, model_class, name, lang, remote_loc)
  spark_jobj(model) %>%
    sparklyr::jobj_set_param("setInputCols", args[["input_cols"]]) %>% 
    sparklyr::jobj_set_param("setOutputCol", args[["output_col"]]) %>%
    sparklyr::jobj_set_param("setImpossiblePenultimates", args[["impossible_penultimates"]]) %>% 
    sparklyr::jobj_set_param("setModel", args[["model"]])
  
  new_nlp_sentence_detector_dl_model(model)
}

nlp_float_params.nlp_sentence_detector_dl <- function(x) {
  return(c("validation_split"))
}

new_nlp_sentence_detector_dl <- function(jobj) {
  sparklyr::new_ml_estimator(jobj, class = "nlp_sentence_detector_dl")
}

new_nlp_sentence_detector_dl_model <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_sentence_detector_dl_model")
}

nlp_float_params.nlp_sentence_detector_dl_model <- function(x) {
  return(c())
}
