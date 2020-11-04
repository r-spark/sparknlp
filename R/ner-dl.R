#' Spark NLP NerDLApproach Named Entity Recognition Deep Learning annotator
#'
#' This Named Entity recognition annotator allows to train generic NER model based on Neural Networks. 
#' Its train data (train_ner) is either a labeled or an external CoNLL 2003 IOB based spark dataset with Annotations 
#' columns. Also the user has to provide word embeddings annotation column.
#' 
#' Neural Network architecture is Char CNNs - BiLSTM - CRF that achieves state-of-the-art in most datasets.
#' See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#ner-dl}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param label_col If DatasetPath is not provided, this Seq[Annotation] type of column should have labeled data per token (string)
#' @param max_epochs Maximum number of epochs to train (integer)
#' @param lr Initial learning rate (float)
#' @param po Learning rate decay coefficient. Real Learning Rate: lr / (1 + po * epoch) (float)
#' @param batch_size Batch size for training (integer)
#' @param dropout Dropout coefficient (float)
#' @param verbose Verbosity level (integer)
#' @param include_confidence whether to include confidence values (boolean)
#' @param random_seed Random seed (integer)
#' @param validation_split proportion of the data to use for validation (float)
#' @param eval_log_extended ? (boolean)
#' @param enable_output_logs whether to enable the TensorFlow output logs (boolean)
#' @param output_logs_path path for the output logs
#' @param graph_folder folder path that contain external graph files
#' @param enable_memory_optimizer allow training NerDLApproach on a dataset larger than the memory
#' 
#' @return When \code{x} is a \code{spark_connection} the function returns a NerDLApproach estimator.
#' When \code{x} is a \code{ml_pipeline} the pipeline with the NerDLApproach added. When \code{x}
#' is a \code{tbl_spark} a transformed \code{tbl_spark}  (note that the Dataframe passed in must have the input_cols specified).
#' 
#' @export
nlp_ner_dl <- function(x, input_cols, output_col,
                 label_col = NULL, max_epochs = NULL, lr = NULL, po = NULL, batch_size = NULL, dropout = NULL, 
                 verbose = NULL, include_confidence = NULL, random_seed = NULL, graph_folder = NULL,
                 validation_split = NULL, eval_log_extended = NULL, enable_output_logs = NULL, output_logs_path = NULL,
                 enable_memory_optimizer = NULL,
                 uid = random_string("ner_dl_")) {
  UseMethod("nlp_ner_dl")
}

#' @export
nlp_ner_dl.spark_connection <- function(x, input_cols, output_col,
                 label_col = NULL, max_epochs = NULL, lr = NULL, po = NULL, batch_size = NULL, dropout = NULL, 
                 verbose = NULL, include_confidence = NULL, random_seed = NULL, graph_folder = NULL,
                 validation_split = NULL, eval_log_extended = NULL, enable_output_logs = NULL, output_logs_path = NULL,
                 enable_memory_optimizer = NULL,
                 uid = random_string("ner_dl_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    label_col = label_col,
    max_epochs = max_epochs,
    lr = lr,
    po = po,
    batch_size = batch_size,
    dropout = dropout,
    verbose = verbose,
    include_confidence = include_confidence,
    random_seed = random_seed,
    graph_folder = graph_folder,
    validation_split = validation_split,
    eval_log_extended = eval_log_extended,
    enable_output_logs = enable_output_logs,
    output_logs_path = output_logs_path,
    enable_memory_optimizer = enable_memory_optimizer,
    uid = uid
  ) %>%
  validator_nlp_ner_dl()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.ner.dl.NerDLApproach",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setLabelColumn", args[["label_col"]])  %>%
    sparklyr::jobj_set_param("setMaxEpochs", args[["max_epochs"]])  %>%
    sparklyr::jobj_set_param("setBatchSize", args[["batch_size"]])  %>%
    sparklyr::jobj_set_param("setVerbose", args[["verbose"]])  %>%
    sparklyr::jobj_set_param("setIncludeConfidence", args[["include_confidence"]]) %>%
    sparklyr::jobj_set_param("setRandomSeed", args[["random_seed"]]) %>% 
    sparklyr::jobj_set_param("setGraphFolder", args[["graph_folder"]]) %>% 
    sparklyr::jobj_set_param("setEvaluationLogExtended", args[["eval_log_extended"]]) %>% 
    sparklyr::jobj_set_param("setEnableOutputLogs", args[["enable_output_logs"]]) %>% 
    sparklyr::jobj_set_param("setOutputLogsPath", args[["output_logs_path"]]) %>% 
    sparklyr::jobj_set_param("setEnableMemoryOptimizer", args[["enable_memory_optimizer"]])
  
  if (!is.null(args[["lr"]])) {
    jobj <- sparklyr::invoke_static(x, "sparknlp.Utils", "setNerLrParam", jobj, args[["lr"]])
  }

  if (!is.null(args[["po"]])) {
    jobj <- sparklyr::invoke_static(x, "sparknlp.Utils", "setNerPoParam", jobj, args[["po"]])
  }

  if (!is.null(args[["dropout"]])) {
    jobj <- sparklyr::invoke_static(x, "sparknlp.Utils", "setNerDropoutParam", jobj, args[["dropout"]])
  }

  if (!is.null(args[["validation_split"]])) {
    jobj <- sparklyr::invoke_static(x, "sparknlp.Utils", "setNerValidationSplitParam", jobj, args[["validation_split"]])
  }

  new_nlp_ner_dl(jobj)
}

nlp_float_params.nlp_ner_dl <- function(x) {
  return(c("lr", "po", "dropout", "validation_split"))
}


nlp_float_params.nlp_ner_dl_model <- function(x) {
  return(c("min_probability"))
}

#' @export
nlp_ner_dl.ml_pipeline <- function(x, input_cols, output_col,
                 label_col = NULL, max_epochs = NULL, lr = NULL, po = NULL, batch_size = NULL, dropout = NULL, 
                 verbose = NULL, include_confidence = NULL, random_seed = NULL, graph_folder = NULL,
                 validation_split = NULL, eval_log_extended = NULL, enable_output_logs = NULL, output_logs_path = NULL,
                 enable_memory_optimizer = NULL,
                 uid = random_string("ner_dl_")) {

  stage <- nlp_ner_dl.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    label_col = label_col,
    max_epochs = max_epochs,
    lr = lr,
    po = po,
    batch_size = batch_size,
    dropout = dropout,
    verbose = verbose,
    include_confidence = include_confidence,
    random_seed = random_seed,
    graph_folder = graph_folder,
    validation_split = validation_split,
    eval_log_extended = eval_log_extended,
    enable_output_logs = enable_output_logs,
    output_logs_path = output_logs_path,
    enable_memory_optimizer = enable_memory_optimizer,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_ner_dl.tbl_spark <- function(x, input_cols, output_col,
                 label_col = NULL, max_epochs = NULL, lr = NULL, po = NULL, batch_size = NULL, dropout = NULL, 
                 verbose = NULL, include_confidence = NULL, random_seed = NULL, graph_folder = NULL,
                 validation_split = NULL, eval_log_extended = NULL, enable_output_logs = NULL, output_logs_path = NULL,
                 enable_memory_optimizer = NULL,
                 uid = random_string("ner_dl_")) {
  stage <- nlp_ner_dl.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    label_col = label_col,
    max_epochs = max_epochs,
    lr = lr,
    po = po,
    batch_size = batch_size,
    dropout = dropout,
    verbose = verbose,
    include_confidence = include_confidence,
    random_seed = random_seed,
    graph_folder = graph_folder,
    validation_split = validation_split,
    eval_log_extended = eval_log_extended,
    enable_output_logs = enable_output_logs,
    output_logs_path = output_logs_path,
    enable_memory_optimizer = enable_memory_optimizer,
    uid = uid
  )

  stage %>% sparklyr::ml_fit(x)
}
#' @import forge
validator_nlp_ner_dl <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["label_col"]] <- cast_nullable_string(args[["label_col"]])
  args[["max_epochs"]] <- cast_nullable_integer(args[["max_epochs"]])
  args[["lr"]] <- cast_nullable_double(args[["lr"]])
  args[["po"]] <- cast_nullable_double(args[["po"]])
  args[["batch_size"]] <- cast_nullable_integer(args[["batch_size"]])
  args[["dropout"]] <- cast_nullable_double(args[["dropout"]])
  args[["verbose"]] <- cast_nullable_integer(args[["verbose"]])
  args[["include_confidence"]] <- cast_nullable_logical(args[["include_confidence"]])
  args[["random_seed"]] <- cast_nullable_integer(args[["random_seed"]])
  args[["graph_folder"]] <- cast_nullable_string(args[["graph_folder"]])
  args[["validation_split"]] <- cast_nullable_double(args[["validation_split"]])
  args[["eval_log_extended"]] <- cast_nullable_logical(args[["eval_log_extended"]])
  args[["enable_output_logs"]] <- cast_nullable_logical(args[["enable_output_logs"]])
  args[["output_logs_path"]] <- cast_nullable_string(args[["output_logs_path"]])
  args[["enable_memory_optimizer"]] <- cast_nullable_logical(args[["enable_memory_optimizer"]])
  args
}

new_nlp_ner_dl <- function(jobj) {
  sparklyr::new_ml_estimator(jobj, class = "nlp_ner_dl")
}

new_nlp_ner_dl_model <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_ner_dl_model")
}

#' Load a pretrained Spark NLP NER DL model
#' 
#' Create a pretrained Spark NLP \code{NerDLModel} model
#' 
#' @template roxlate-pretrained-params
#' @template roxlate-inputs-output-params
#' @param include_confidence whether to include confidence values
#' @export
nlp_ner_dl_pretrained <- function(sc, input_cols, output_col, include_confidence = NULL,
                                   name = NULL, lang = NULL, remote_loc = NULL) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col
  )
  
  args[["input_cols"]] <- forge::cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- forge::cast_string(args[["output_col"]])
  args[["include_confidence"]] <- forge::cast_nullable_logical(args[["include_confidence"]])
  
  model_class <- "com.johnsnowlabs.nlp.annotators.ner.dl.NerDLModel"
  model <- pretrained_model(sc, model_class, name, lang, remote_loc)
  spark_jobj(model) %>%
    sparklyr::jobj_set_param("setInputCols", args[["input_cols"]]) %>% 
    sparklyr::jobj_set_param("setOutputCol", args[["output_col"]]) %>%
    sparklyr::jobj_set_param("setIncludeConfidence", args[["include_confidence"]])
  
  new_nlp_ner_dl_model(model)
}

