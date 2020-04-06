#' Spark NLP ClassifierDLApproach
#'
#' Spark ML annotator that 
#' See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#classifierdl}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param label_col name of the column containing the category labels
#' @param max_epochs Maximum number of epochs to train
#' @param lr Initial learning rate
#' @param batch_size Batch size for training
#' @param dropout Dropout coefficient
#' @param verbose Verbosity level 
#' @param validation_split proportion of data to split off for validation
#' @param enable_output_logs boolean to enable/disable output logs
#' @param lazy_annotator boolean
#' 
#' @export
nlp_classifier_dl <- function(x, input_cols, output_col,
                 label_col, batch_size = NULL, max_epochs = NULL, lr = NULL, dropout = NULL, 
                 validation_split = NULL, verbose = NULL, enable_output_logs = NULL, lazy_annotator = NULL,
                 uid = random_string("classifier_dl_")) {
  UseMethod("nlp_classifier_dl")
}

#' @export
nlp_classifier_dl.spark_connection <- function(x, input_cols, output_col,
                 label_col, batch_size = NULL, max_epochs = NULL, lr = NULL, dropout = NULL, validation_split = NULL, verbose = NULL, enable_output_logs = NULL, lazy_annotator = NULL,
                 uid = random_string("classifier_dl_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    label_col = label_col,
    batch_size = batch_size,
    max_epochs = max_epochs,
    lr = lr,
    dropout = dropout,
    validation_split = validation_split,
    verbose = verbose,
    enable_output_logs = enable_output_logs,
    lazy_annotator = lazy_annotator,
    uid = uid
  ) %>%
  validator_nlp_classifier_dl()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.classifier.dl.ClassifierDLApproach",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setLabelColumn", args[["label_col"]])  %>%
    sparklyr::jobj_set_param("setBatchSize", args[["batch_size"]])  %>%
    sparklyr::jobj_set_param("setMaxEpochs", args[["max_epochs"]])  %>%
    sparklyr::jobj_set_param("setVerbose", args[["verbose"]])  %>%
    sparklyr::jobj_set_param("setEnableOutputLogs", args[["enable_output_logs"]])  %>%
    sparklyr::jobj_set_param("setLazyAnnotator", args[["lazy_annotator"]]) 
  
  if (!is.null(args[["lr"]])) {
    jobj <- sparklyr::invoke_static(x, "sparknlp.Utils", "setCDLLrParam", jobj, args[["lr"]])
  }

  if (!is.null(args[["dropout"]])) {
    jobj <- sparklyr::invoke_static(x, "sparknlp.Utils", "setCDLDropoutParam", jobj, args[["dropout"]])
  }
  
  if (!is.null(args[["validation_split"]])) {
    jobj <- sparklyr::invoke_static(x, "sparknlp.Utils", "setCDLValidationSplitParam", jobj, args[["validation_split"]])
  }

  new_nlp_classifier_dl(jobj)
}

#' @export
nlp_classifier_dl.ml_pipeline <- function(x, input_cols, output_col,
                 label_col, batch_size = NULL, max_epochs = NULL, lr = NULL, dropout = NULL, validation_split = NULL, verbose = NULL, enable_output_logs = NULL, lazy_annotator = NULL,
                 uid = random_string("classifier_dl_")) {

  stage <- nlp_classifier_dl.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    label_col = label_col,
    batch_size = batch_size,
    max_epochs = max_epochs,
    lr = lr,
    dropout = dropout,
    validation_split = validation_split,
    verbose = verbose,
    enable_output_logs = enable_output_logs,
    lazy_annotator = lazy_annotator,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_classifier_dl.tbl_spark <- function(x, input_cols, output_col,
                 label_col, batch_size = NULL, max_epochs = NULL, lr = NULL, dropout = NULL, validation_split = NULL, verbose = NULL, enable_output_logs = NULL, lazy_annotator = NULL,
                 uid = random_string("classifier_dl_")) {
  stage <- nlp_classifier_dl.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    label_col = label_col,
    batch_size = batch_size,
    max_epochs = max_epochs,
    lr = lr,
    dropout = dropout,
    validation_split = validation_split,
    verbose = verbose,
    enable_output_logs = enable_output_logs,
    lazy_annotator = lazy_annotator,
    uid = uid
  )

  stage %>% sparklyr::ml_fit(x)
}
#' @import forge
validator_nlp_classifier_dl <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["label_col"]] <- cast_string(args[["label_col"]])
  args[["batch_size"]] <- cast_nullable_integer(args[["batch_size"]])
  args[["max_epochs"]] <- cast_nullable_integer(args[["max_epochs"]])
  args[["lr"]] <- cast_nullable_double(args[["lr"]])
  args[["dropout"]] <- cast_nullable_double(args[["dropout"]])
  args[["validation_split"]] <- cast_nullable_double(args[["validation_split"]])
  args[["verbose"]] <- cast_nullable_integer(args[["verbose"]])
  args[["enable_output_logs"]] <- cast_nullable_logical(args[["enable_output_logs"]])
  args[["lazy_annotator"]] <- cast_nullable_logical(args[["lazy_annotator"]])
  args
}

new_nlp_classifier_dl <- function(jobj) {
  sparklyr::new_ml_estimator(jobj, class = "nlp_classifier_dl")
}

#' Load a pretrained Spark NLP Classifier DL model
#' 
#' Create a pretrained Spark NLP \code{ClassifierDLModel} model
#' 
#' @template roxlate-pretrained-params
#' @template roxlate-inputs-output-params
#' @export
nlp_classifier_dl_pretrained <- function(sc, input_cols, output_col, include_confidence = NULL,
                                  name = NULL, lang = NULL, remote_loc = NULL) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col
  )
  
  args[["input_cols"]] <- forge::cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- forge::cast_string(args[["output_col"]])

  model_class <- "com.johnsnowlabs.nlp.annotators.classifier.dl.ClassifierDLModel"
  model <- pretrained_model(sc, model_class, name, lang, remote_loc)
  spark_jobj(model) %>%
    sparklyr::jobj_set_param("setInputCols", args[["input_cols"]]) %>% 
    sparklyr::jobj_set_param("setOutputCol", args[["output_col"]])

  new_ml_transformer(model)
}
