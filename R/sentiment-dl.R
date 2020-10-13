#' Spark NLP SentimentDLApproach Sentiment detection Deep Learning annotator
#' 
#' Multi-class sentiment analysis annotator. 
#' SentimentDL is an annotator for multi-class sentiment analysis. This annotator comes with 2 available pre-trained models trained on IMDB and Twitter datasets
#' NOTE: This annotator accepts a label column of a single item in either type of String, Int, Float, or Double.
#' NOTE: UniversalSentenceEncoder and SentenceEmbeddings can be used for the inputCol
#' 
#' See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#sentimentdl}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param label_col If DatasetPath is not provided, this Seq[Annotation] type of column should have labeled data per token (string)
#' @param max_epochs Maximum number of epochs to train (integer)
#' @param lr Initial learning rate (float)
#' @param batch_size Batch size for training (integer)
#' @param dropout Dropout coefficient (float)
#' @param verbose Verbosity level during training (integer)
#' @param validation_split Choose the proportion of training dataset to be validated against the model on each Epoch. The value should be between 0.0 and 1.0 and by default it is 0.0 and off (float)
#' @param enable_output_logs whether to enable the TensorFlow output logs (boolean)
#' @param output_logs_path path for the output logs

#' @param threshold The minimum threshold for the final result otheriwse it will be either neutral or the value set in thresholdLabel
#' @param threshold_label In case the score is less than threshold, what should be the label. Default is neutral.
#' 
#' @return When \code{x} is a \code{spark_connection} the function returns a SentimentDLApproach estimator.
#' When \code{x} is a \code{ml_pipeline} the pipeline with the SentimentDLApproach added. When \code{x}
#' is a \code{tbl_spark} a transformed \code{tbl_spark}  (note that the Dataframe passed in must have the input_cols specified).
#' 
#' @export
nlp_sentiment_dl <- function(x, input_cols, output_col,
                 label_col = NULL, max_epochs = NULL, lr = NULL, batch_size = NULL, dropout = NULL, 
                 verbose = NULL, threshold = NULL, threshold_label = NULL,
                 validation_split = NULL, enable_output_logs = NULL, output_logs_path = NULL,
                 uid = random_string("sentiment_dl_")) {
  UseMethod("nlp_sentiment_dl")
}

#' @export
nlp_sentiment_dl.spark_connection <- function(x, input_cols, output_col,
                                        label_col = NULL, max_epochs = NULL, lr = NULL, batch_size = NULL, dropout = NULL, 
                                        verbose = NULL, threshold = NULL, threshold_label = NULL, 
                                        validation_split = NULL, enable_output_logs = NULL, output_logs_path = NULL,
                 uid = random_string("sentiment_dl_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    label_col = label_col,
    max_epochs = max_epochs,
    lr = lr,
    batch_size = batch_size,
    dropout = dropout,
    verbose = verbose,
    validation_split = validation_split,
    enable_output_logs = enable_output_logs,
    output_logs_path = output_logs_path,
    threshold = threshold,
    threshold_label = threshold_label,
    uid = uid
  ) %>%
  validator_nlp_sentiment_dl()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.classifier.dl.SentimentDLApproach",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setLabelColumn", args[["label_col"]])  %>%
    sparklyr::jobj_set_param("setMaxEpochs", args[["max_epochs"]])  %>%
    sparklyr::jobj_set_param("setBatchSize", args[["batch_size"]])  %>%
    sparklyr::jobj_set_param("setVerbose", args[["verbose"]])  %>%
    sparklyr::jobj_set_param("setRandomSeed", args[["random_seed"]]) %>% 
    sparklyr::jobj_set_param("setEnableOutputLogs", args[["enable_output_logs"]]) %>% 
    sparklyr::jobj_set_param("setOutputLogsPath", args[["output_logs_path"]]) %>% 
    sparklyr::jobj_set_param("setThresholdLabel", args[["threshold_label"]])
  
  if (!is.null(args[["lr"]])) {
    jobj <- sparklyr::invoke_static(x, "sparknlp.Utils", "setSentimentLrParam", jobj, args[["lr"]])
  }

  if (!is.null(args[["dropout"]])) {
    jobj <- sparklyr::invoke_static(x, "sparknlp.Utils", "setSentimentDropoutParam", jobj, args[["dropout"]])
  }

  if (!is.null(args[["validation_split"]])) {
    jobj <- sparklyr::invoke_static(x, "sparknlp.Utils", "setSentimentValidationSplitParam", jobj, args[["validation_split"]])
  }
  
  if (!is.null(args[["threshold"]])) {
    jobj <- sparklyr::invoke_static(x, "sparknlp.Utils", "setSentimentThreshold", jobj, args[["threshold"]])
  }

  new_nlp_sentiment_dl(jobj)
}

nlp_float_params.nlp_sentiment_dl <- function(x) {
  return(c("lr", "dropout", "validation_split", "threshold"))
}

#' @export
nlp_sentiment_dl.ml_pipeline <- function(x, input_cols, output_col,
                                   label_col = NULL, max_epochs = NULL, lr = NULL, batch_size = NULL, dropout = NULL, 
                                   verbose = NULL, threshold = NULL, threshold_label = NULL, 
                                   validation_split = NULL, enable_output_logs = NULL, output_logs_path = NULL,
                 uid = random_string("sentiment_dl_")) {

  stage <- nlp_sentiment_dl.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    label_col = label_col,
    max_epochs = max_epochs,
    lr = lr,
    batch_size = batch_size,
    dropout = dropout,
    verbose = verbose,
    validation_split = validation_split,
    enable_output_logs = enable_output_logs,
    output_logs_path = output_logs_path,
    threshold = threshold,
    threshold_label = threshold_label,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_sentiment_dl.tbl_spark <- function(x, input_cols, output_col,
                                 label_col = NULL, max_epochs = NULL, lr = NULL, batch_size = NULL, dropout = NULL, 
                                 verbose = NULL, threshold = NULL, threshold_label = NULL, 
                                 validation_split = NULL, enable_output_logs = NULL, output_logs_path = NULL,
                 uid = random_string("sentiment_dl_")) {
  stage <- nlp_sentiment_dl.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    label_col = label_col,
    max_epochs = max_epochs,
    lr = lr,
    batch_size = batch_size,
    dropout = dropout,
    verbose = verbose,
    validation_split = validation_split,
    enable_output_logs = enable_output_logs,
    output_logs_path = output_logs_path,
    threshold = threshold,
    threshold_label = threshold_label,
    uid = uid
  )

  stage %>% sparklyr::ml_fit(x)
}
#' @import forge
validator_nlp_sentiment_dl <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["label_col"]] <- cast_nullable_string(args[["label_col"]])
  args[["max_epochs"]] <- cast_nullable_integer(args[["max_epochs"]])
  args[["lr"]] <- cast_nullable_double(args[["lr"]])
  args[["batch_size"]] <- cast_nullable_integer(args[["batch_size"]])
  args[["dropout"]] <- cast_nullable_double(args[["dropout"]])
  args[["verbose"]] <- cast_nullable_integer(args[["verbose"]])
  args[["validation_split"]] <- cast_nullable_double(args[["validation_split"]])
  args[["enable_output_logs"]] <- cast_nullable_logical(args[["enable_output_logs"]])
  args[["output_logs_path"]] <- cast_nullable_string(args[["output_logs_path"]])
  args[["threshold"]] <- cast_nullable_double(args[["threshold"]])
  args[["threshold_label"]] <- cast_nullable_string(args[["threshold_label"]])
  args
}

new_nlp_sentiment_dl <- function(jobj) {
  sparklyr::new_ml_estimator(jobj, class = "nlp_sentiment_dl")
}

new_nlp_sentiment_dl_model <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_sentiment_dl_model")
}



#' Load a pretrained Spark NLP Sentiment DL model
#' 
#' Create a pretrained Spark NLP \code{SentimentDLModel} model
#' 
#' @template roxlate-pretrained-params
#' @template roxlate-inputs-output-params
#' @param include_confidence whether to include confidence values
#' @export
nlp_sentiment_dl_pretrained <- function(sc, input_cols, output_col,
                                   name = NULL, lang = NULL, remote_loc = NULL) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col
  )
  
  args[["input_cols"]] <- forge::cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- forge::cast_string(args[["output_col"]])

  model_class <- "com.johnsnowlabs.nlp.annotators.classifier.dl.SentimentDLModel"
  model <- pretrained_model(sc, model_class, name, lang, remote_loc)
  spark_jobj(model) %>%
    sparklyr::jobj_set_param("setInputCols", args[["input_cols"]]) %>% 
    sparklyr::jobj_set_param("setOutputCol", args[["output_col"]])

  new_nlp_sentiment_dl_model(model)
}