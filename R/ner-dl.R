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
#' @param label_col If DatasetPath is not provided, this Seq[Annotation] type of column should have labeled data per token
#' @param max_epochs Maximum number of epochs to train
#' @param lr Initial learning rate
#' @param po Learning rate decay coefficient. Real Learning Rate: lr / (1 + po * epoch)
#' @param batch_size Batch size for training
#' @param dropout Dropout coefficient
#' @param verbose Verbosity level
#' @param include_confidence whether to include confidence values
#' @param random_seed Random seed
#' 
#' @return When \code{x} is a \code{spark_connection} the function returns a NerDLApproach estimator.
#' When \code{x} is a \code{ml_pipeline} the pipeline with the NerDLApproach added. When \code{x}
#' is a \code{tbl_spark} a transformed \code{tbl_spark}  (note that the Dataframe passed in must have the input_cols specified).
#' 
#' @export
nlp_ner_dl <- function(x, input_cols, output_col,
                 label_col = NULL, max_epochs = NULL, lr = NULL, po = NULL, batch_size = NULL, dropout = NULL, 
                 verbose = NULL, include_confidence = NULL, random_seed = NULL, uid = random_string("ner_dl_")) {
  UseMethod("nlp_ner_dl")
}

#' @export
nlp_ner_dl.spark_connection <- function(x, input_cols, output_col,
                 label_col = NULL, max_epochs = NULL, lr = NULL, po = NULL, batch_size = NULL, dropout = NULL, 
                 verbose = NULL, include_confidence = NULL, random_seed = NULL, uid = random_string("ner_dl_")) {
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
    sparklyr::jobj_set_param("setRandomSeed", args[["random_seed"]]) 
  
  if (!is.null(args[["lr"]])) {
    jobj <- sparklyr::invoke_static(x, "sparknlp.Utils", "setLrParam", jobj, args[["lr"]])
  }
  
  if (!is.null(args[["po"]])) {
    jobj <- sparklyr::invoke_static(x, "sparknlp.Utils", "setPoParam", jobj, args[["po"]])
  }
  
  if (!is.null(args[["dropout"]])) {
    jobj <- sparklyr::invoke_static(x, "sparknlp.Utils", "setDropoutParam", jobj, args[["dropout"]])
  }
  
  new_nlp_ner_dl(jobj)
}

#' @export
nlp_ner_dl.ml_pipeline <- function(x, input_cols, output_col,
                 label_col = NULL, max_epochs = NULL, lr = NULL, po = NULL, batch_size = NULL, dropout = NULL, 
                 verbose = NULL, include_confidence = NULL, random_seed = NULL, uid = random_string("ner_dl_")) {

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
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_ner_dl.tbl_spark <- function(x, input_cols, output_col,
                 label_col = NULL, max_epochs = NULL, lr = NULL, po = NULL, batch_size = NULL, dropout = NULL, 
                 verbose = NULL, include_confidence = NULL, random_seed = NULL, uid = random_string("ner_dl_")) {
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
  args
}

new_nlp_ner_dl <- function(jobj) {
  sparklyr::new_ml_estimator(jobj, class = "nlp_ner_dl")
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
  
  new_ml_transformer(model)
}