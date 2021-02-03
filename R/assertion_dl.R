#' Spark NLP AssertionDLApproach
#'
#' Spark ML estimator that classifies each clinically relevant named entity into its assertion 
#' type: “present”, “absent”, “hypothetical”, “conditional”, “associated_with_other_person”, etc.
#' See \url{https://nlp.johnsnowlabs.com/docs/en/licensed_annotators#assertiondl}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param graph_folder forlder containing the TF graph files
#' @param config_proto_bytes array of integers
#' @param label_column column name to use as the labels for training
#' @param batch_size gradient descent batch size
#' @param epochs number of training epochs
#' @param learning_rate learning rate for the algorithm
#' @param dropout dropout for the algorithm
#' @param max_sent_len regulates the length of the longest sentence
#' @param start_col the name of the column with the value for the start index of the target
#' @param end_col the name of the column with the value for the ending index of the target
#' @param chunk_col the name of the column containing the chunks
#' @param classes number of classes of assertion
#' @param enable_output_logs Whether to output to annotators log folder
#' @param output_logs_path path for the output logs to go
#' @param target_col the name of the column containing the target text
#' @param validation_split Choose the proportion of training dataset to be validated against the model on each Epoch.
#' @param verbose level of verbosity. One of All, PerStep, Epochs, TrainingStat, Silent
#' 
#' @export
nlp_assertion_dl <- function(x, input_cols, output_col,
                 graph_folder = NULL, config_proto_bytes = NULL, label_column = NULL, batch_size = NULL, epochs = NULL, learning_rate = NULL, dropout = NULL, max_sent_len = NULL, start_col = NULL, end_col = NULL, chunk_col = NULL, classes = NULL, enable_output_logs = NULL, output_logs_path = NULL, target_col = NULL, validation_split = NULL, verbose = NULL,
                 uid = random_string("assertion_dl_")) {
  UseMethod("nlp_assertion_dl")
}

#' @export
nlp_assertion_dl.spark_connection <- function(x, input_cols, output_col,
                 graph_folder = NULL, config_proto_bytes = NULL, label_column = NULL, batch_size = NULL, epochs = NULL, learning_rate = NULL, dropout = NULL, max_sent_len = NULL, start_col = NULL, end_col = NULL, chunk_col = NULL, classes = NULL, enable_output_logs = NULL, output_logs_path = NULL, target_col = NULL, validation_split = NULL, verbose = NULL,
                 uid = random_string("assertion_dl_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    graph_folder = graph_folder,
    config_proto_bytes = config_proto_bytes,
    label_column = label_column,
    batch_size = batch_size,
    epochs = epochs,
    learning_rate = learning_rate,
    dropout = dropout,
    max_sent_len = max_sent_len,
    start_col = start_col,
    end_col = end_col,
    chunk_col = chunk_col,
    classes = classes,
    enable_output_logs = enable_output_logs,
    output_logs_path = output_logs_path,
    target_col = target_col,
    validation_split = validation_split,
    verbose = verbose,
    uid = uid
  ) %>%
  validator_nlp_assertion_dl()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.assertion.dl.AssertionDLApproach",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setGraphFolder", args[["graph_folder"]])  %>%
    sparklyr::jobj_set_param("setConfigProtoBytes", args[["config_proto_bytes"]])  %>%
    sparklyr::jobj_set_param("setBatchSize", args[["batch_size"]])  %>%
    sparklyr::jobj_set_param("setEpochs", args[["epochs"]])  %>%
    sparklyr::jobj_set_param("setMaxSentLen", args[["max_sent_len"]])  %>%
    sparklyr::jobj_set_param("setStartCol", args[["start_col"]])  %>%
    sparklyr::jobj_set_param("setEndCol", args[["end_col"]])  %>%
    sparklyr::jobj_set_param("setChunkCol", args[["chunk_col"]])  %>%
    sparklyr::jobj_set_param("setClasses", args[["classes"]])  %>%
    sparklyr::jobj_set_param("setEnableOutputLogs", args[["enable_output_logs"]])  %>%
    sparklyr::jobj_set_param("setOutputLogsPath", args[["output_logs_path"]])

  annotator <- new_nlp_assertion_dl(jobj)
  
  if (!is.null(args[["label_col"]])) {
    annotator <- nlp_set_param(annotator, "label_column", args[["label_column"]])
  }
  
  if (!is.null(args[["target_col"]])) {
    #spark_jobj(x) %>% invoke(setter, value) %>% ml_call_constructor()
    #annotator <- sparklyr:::ml_set_param(annotator, "target_col", args[["target_col"]])
    annotator <- sparklyr::invoke(sparklyr::spark_jobj(annotator), "setTargetCol", args[["target_col"]]) %>% 
      sparklyr::ml_call_constructor()
  }

  if (!is.null(args[["verbose"]])) {
    verbose_level <- invoke_static(sc, "com.johnsnowlabs.nlp.annotators.ner.Verbose", "withName", args[["verbose"]])
    annotator <- nlp_set_param(annotator, "verbose", verbose_level)
  }
  
  if (!is.null(args[["learning_rate"]])) {  
    annotator <- nlp_set_param(annotator, "learning_rate", args[["learning_rate"]])
  }
  
  if (!is.null(args[["validation_split"]])) { 
    annotator <- nlp_set_param(annotator, "validation_split", args[["validation_split"]])
  }
  
  if (!is.null(args[["dropout"]])) { 
    annotator <- nlp_set_param(annotator, "dropout", args[["dropout"]])
  }
  
  return(annotator)
}

#' @export
nlp_assertion_dl.ml_pipeline <- function(x, input_cols, output_col,
                 graph_folder = NULL, config_proto_bytes = NULL, label_column = NULL, batch_size = NULL, epochs = NULL, learning_rate = NULL, dropout = NULL, max_sent_len = NULL, start_col = NULL, end_col = NULL, chunk_col = NULL, classes = NULL, enable_output_logs = NULL, output_logs_path = NULL, target_col = NULL, validation_split = NULL, verbose = NULL,
                 uid = random_string("assertion_dl_")) {

  stage <- nlp_assertion_dl.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    graph_folder = graph_folder,
    config_proto_bytes = config_proto_bytes,
    label_column = label_column,
    batch_size = batch_size,
    epochs = epochs,
    learning_rate = learning_rate,
    dropout = dropout,
    max_sent_len = max_sent_len,
    start_col = start_col,
    end_col = end_col,
    chunk_col = chunk_col,
    classes = classes,
    enable_output_logs = enable_output_logs,
    output_logs_path = output_logs_path,
    target_col = target_col,
    validation_split = validation_split,
    verbose = verbose,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_assertion_dl.tbl_spark <- function(x, input_cols, output_col,
                 graph_folder = NULL, config_proto_bytes = NULL, label_column = NULL, batch_size = NULL, epochs = NULL, learning_rate = NULL, dropout = NULL, max_sent_len = NULL, start_col = NULL, end_col = NULL, chunk_col = NULL, classes = NULL, enable_output_logs = NULL, output_logs_path = NULL, target_col = NULL, validation_split = NULL, verbose = NULL,
                 uid = random_string("assertion_dl_")) {
  stage <- nlp_assertion_dl.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    graph_folder = graph_folder,
    config_proto_bytes = config_proto_bytes,
    label_column = label_column,
    batch_size = batch_size,
    epochs = epochs,
    learning_rate = learning_rate,
    dropout = dropout,
    max_sent_len = max_sent_len,
    start_col = start_col,
    end_col = end_col,
    chunk_col = chunk_col,
    classes = classes,
    enable_output_logs = enable_output_logs,
    output_logs_path = output_logs_path,
    target_col = target_col,
    validation_split = validation_split,
    verbose = verbose,
    uid = uid
  )

  stage %>% sparklyr::ml_fit_and_transform(x)
}
#' @import forge
validator_nlp_assertion_dl <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["graph_folder"]] <- cast_nullable_string(args[["graph_folder"]])
  args[["config_proto_bytes"]] <- cast_nullable_integer_list(args[["config_proto_bytes"]])
  args[["label_column"]] <- cast_nullable_string(args[["label_column"]])
  args[["batch_size"]] <- cast_nullable_integer(args[["batch_size"]])
  args[["epochs"]] <- cast_nullable_integer(args[["epochs"]])
  args[["learning_rate"]] <- cast_nullable_double(args[["learning_rate"]])
  args[["dropout"]] <- cast_nullable_double(args[["dropout"]])
  args[["max_sent_len"]] <- cast_nullable_integer(args[["max_sent_len"]])
  args[["start_col"]] <- cast_nullable_string(args[["start_col"]])
  args[["end_col"]] <- cast_nullable_string(args[["end_col"]])
  args[["chunk_col"]] <- cast_nullable_string(args[["chunk_col"]])
  args[["classes"]] <- cast_nullable_integer(args[["classes"]])
  args[["enable_output_logs"]] <- cast_nullable_logical(args[["enable_output_logs"]])
  args[["output_logs_path"]] <- cast_nullable_string(args[["output_logs_path"]])
  args[["target_col"]] <- cast_nullable_string(args[["target_col"]])
  args[["validation_split"]] <- cast_nullable_double(args[["validation_split"]])
  args[["verbose"]] <- cast_nullable_string(args[["verbose"]])
  args
}

nlp_float_params.nlp_assertion_dl <- function(x) {
  return(c("learning_rate", "dropout", "validation_split"))
}
new_nlp_assertion_dl <- function(jobj) {
  sparklyr::new_ml_estimator(jobj, class = "nlp_assertion_dl")
}
new_nlp_assertion_dl_model <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_assertion_dl_model")
}

#' Load a pretrained Spark NLP Assertion DL model
#' 
#' Create a pretrained Spark NLP \code{AssertionDLModel} model
#' 
#' @template roxlate-pretrained-params
#' @template roxlate-inputs-output-params
#' @param batch_size Parameter, which regulates the size of the batch
#' @param max_sent_len Parameter, which regulates the length of the longest sentence
#' @param storage_ref storage reference for embeddings
#' @export
nlp_assertion_dl_pretrained <- function(sc, input_cols, output_col, batch_size = NULL,
                                  max_sent_len = NULL, storage_ref = NULL,
                                  name = NULL, lang = NULL, remote_loc = NULL) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col
  )
  
  args[["input_cols"]] <- forge::cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- forge::cast_string(args[["output_col"]])
  args[["batch_size"]] <- forge::cast_nullable_integer(args[["batch_size"]])
  args[["max_sent_len"]] <- forge::cast_nullable_integer(args[["max_sent_len"]])
  args[["storage_ref"]] <- forge::cast_nullable_string(args[["storage_ref"]])
  
  model_class <- "com.johnsnowlabs.nlp.annotators.assertion.dl.AssertionDLModel"
  model <- pretrained_model(sc, model_class, name, lang, remote_loc)
  spark_jobj(model) %>%
    sparklyr::jobj_set_param("setInputCols", args[["input_cols"]]) %>% 
    sparklyr::jobj_set_param("setOutputCol", args[["output_col"]]) %>%
    sparklyr::jobj_set_param("setBatchSize", args[["batch_size"]]) %>% 
    sparklyr::jobj_set_param("setMaxSentLen", args[["max_sent_len"]]) %>% 
    sparklyr::jobj_set_param("setStorageRef", args[["storage_ref"]])
  
  new_nlp_assertion_dl_model(model)
}

nlp_float_params.nlp_assertion_dl_model <- function(x) {
  return(c())
}
