#' Spark NLP MultiClassifierDLApproach
#'
#' Spark ML estimator that 
#' See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#multiclassifierdl-multi-label-text-classification}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param batch_size Batch size
#' @param enable_output_logs whether to output to annotators log folder
#' @param label_col column with label per each document
#' @param lr learning rate
#' @param max_epochs maximum number of epoch to train
#' @param output_logs_path output logs path
#' @param shuffle_per_epoch shuffle per epoch
#' @param threshold the minimum threshold for each label to be accepted
#' @param validation_split choose the proportion of training dataset to be validated against the model on each epoch
#' @param verbose level of verbosity during training (integer)
#' 
#' @export
nlp_multi_classifier_dl <- function(x, input_cols, output_col,
                 batch_size = NULL, enable_output_logs = NULL, label_col = NULL, 
                 lr = NULL, max_epochs = NULL, output_logs_path = NULL,
                 shuffle_per_epoch = NULL, threshold = NULL, validation_split = NULL,
                 verbose = NULL,
                 uid = random_string("multi_classifier_dl_")) {
  UseMethod("nlp_multi_classifier_dl")
}

#' @export
nlp_multi_classifier_dl.spark_connection <- function(x, input_cols, output_col,
                 batch_size = NULL, enable_output_logs = NULL, label_col = NULL,
                 lr = NULL, max_epochs = NULL, output_logs_path = NULL, 
                 shuffle_per_epoch = NULL, threshold = NULL, validation_split = NULL, 
                 verbose = NULL,
                 uid = random_string("multi_classifier_dl_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    batch_size = batch_size,
    enable_output_logs = enable_output_logs,
    label_col = label_col,
    lr = lr,
    max_epochs = max_epochs,
    output_logs_path = output_logs_path,
    shuffle_per_epoch = shuffle_per_epoch,
    threshold = threshold,
    validation_split = validation_split,
    verbose = verbose,
    uid = uid
  ) %>%
  validator_nlp_multi_classifier_dl()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.classifier.dl.MultiClassifierDLApproach",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setBatchSize", args[["batch_size"]])  %>%
    sparklyr::jobj_set_param("setEnableOutputLogs", args[["enable_output_logs"]])  %>%
    sparklyr::jobj_set_param("setLabelColumn", args[["label_col"]])  %>%
    #sparklyr::jobj_set_param("setLr", args[["lr"]])  %>%
    sparklyr::jobj_set_param("setMaxEpochs", args[["max_epochs"]])  %>%
    sparklyr::jobj_set_param("setOutputLogsPath", args[["output_logs_path"]])  %>%
    sparklyr::jobj_set_param("setShufflePerEpoch", args[["shuffle_per_epoch"]])  %>%
    #sparklyr::jobj_set_param("setThreshold", args[["threshold"]])  %>%
    #sparklyr::jobj_set_param("setValidationSplit", args[["validation_split"]])  %>%
    sparklyr::jobj_set_param("setVerbose", args[["verbose"]])

  annotator <- new_nlp_multi_classifier_dl(jobj)
  
  if (!is.null(args[["lr"]])) {
    annotator <- nlp_set_param(annotator, "lr", args[["lr"]])
  }
  
  if (!is.null(args[["threshold"]])) {
    annotator <- nlp_set_param(annotator, "threshold", args[["threshold"]])
  }
  
  if (!is.null(args[["validation_split"]])) {
    annotator <- nlp_set_param(annotator, "validation_split", args[["validation_split"]])
  }
  
  return(annotator)
}

#' @export
nlp_multi_classifier_dl.ml_pipeline <- function(x, input_cols, output_col,
                 batch_size = NULL, enable_output_logs = NULL, label_col = NULL,
                 lr = NULL, max_epochs = NULL, output_logs_path = NULL,
                 shuffle_per_epoch = NULL, threshold = NULL, validation_split = NULL,
                 verbose = NULL,
                 uid = random_string("multi_classifier_dl_")) {

  stage <- nlp_multi_classifier_dl.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    batch_size = batch_size,
    enable_output_logs = enable_output_logs,
    label_col = label_col,
    lr = lr,
    max_epochs = max_epochs,
    output_logs_path = output_logs_path,
    shuffle_per_epoch = shuffle_per_epoch,
    threshold = threshold,
    validation_split = validation_split,
    verbose = verbose,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_multi_classifier_dl.tbl_spark <- function(x, input_cols, output_col,
                 batch_size = NULL, enable_output_logs = NULL, label_col = NULL, lr = NULL, max_epochs = NULL, output_logs_path = NULL, shuffle_per_epoch = NULL, threshold = NULL, validation_split = NULL, verbose = NULL,
                 uid = random_string("multi_classifier_dl_")) {
  stage <- nlp_multi_classifier_dl.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    batch_size = batch_size,
    enable_output_logs = enable_output_logs,
    label_col = label_col,
    lr = lr,
    max_epochs = max_epochs,
    output_logs_path = output_logs_path,
    shuffle_per_epoch = shuffle_per_epoch,
    threshold = threshold,
    validation_split = validation_split,
    verbose = verbose,
    uid = uid
  )

  stage %>% sparklyr::ml_fit_and_transform(x)
}
#' @import forge
validator_nlp_multi_classifier_dl <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["batch_size"]] <- cast_nullable_integer(args[["batch_size"]])
  args[["enable_output_logs"]] <- cast_nullable_logical(args[["enable_output_logs"]])
  args[["label_col"]] <- cast_nullable_string(args[["label_col"]])
  args[["lr"]] <- cast_nullable_double(args[["lr"]])
  args[["max_epochs"]] <- cast_nullable_integer(args[["max_epochs"]])
  args[["output_logs_path"]] <- cast_nullable_string(args[["output_logs_path"]])
  args[["shuffle_per_epoch"]] <- cast_nullable_logical(args[["shuffle_per_epoch"]])
  args[["threshold"]] <- cast_nullable_double(args[["threshold"]])
  args[["validation_split"]] <- cast_nullable_double(args[["validation_split"]])
  args[["verbose"]] <- cast_nullable_integer(args[["verbose"]])
  args
}

#' Load a pretrained Spark NLP Multilabel Classifier DL model
#' 
#' Create a pretrained Spark NLP \code{MultiClassifierDLModel} model
#' 
#' @template roxlate-pretrained-params
#' @template roxlate-inputs-output-params
#' @param threshold the minimum threshold for each label to be accepted
#' @export
nlp_multi_classifier_dl_pretrained <- function(sc, input_cols, output_col, threshold = NULL,
                                  name = NULL, lang = NULL, remote_loc = NULL) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col
  )
  
  args[["input_cols"]] <- forge::cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- forge::cast_string(args[["output_col"]])
  args[["threshold"]] <- forge::cast_nullable_double(args[["threshold"]])
  
  model_class <- "com.johnsnowlabs.nlp.annotators.classifier.dl.MultiClassifierDLModel"
  model <- pretrained_model(sc, model_class, name, lang, remote_loc)
  
  model <- spark_jobj(model) %>%
    sparklyr::jobj_set_param("setInputCols", args[["input_cols"]]) %>% 
    sparklyr::jobj_set_param("setOutputCol", args[["output_col"]]) %>% 
    new_nlp_multi_classifier_dl_model()
    
  if (!is.null(threshold)) {
    model <- nlp_set_param(model, "threshold", threshold)
  }

  return(model)
}

nlp_float_params.nlp_multi_classifier_dl <- function(x) {
  return(c("lr", "threshold", "validation_split"))
}

nlp_float_params.nlp_multi_classifier_dl_model <- function(x) {
  return(c("threshold"))
}

new_nlp_multi_classifier_dl <- function(jobj) {
  sparklyr::new_ml_estimator(jobj, class = "nlp_multi_classifier_dl")
}
new_nlp_multi_classifier_dl_model <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_multi_classifier_dl_model")
}
nlp_float_params.nlp_multi_classifier_dl_model <- function(x) {
  return(c())
}
