#' Spark NLP RelationExtractionApproach
#'
#' Spark ML estimator that trains a TensorFlow model for relation extraction.
#' The Tensorflow graph in .pb format needs to be specified with setModelFile. 
#' The result is a RelationExtractionModel. To start training, see the parameters 
#' that need to be set in the Parameters section.
#'  
#' See \url{https://nlp.johnsnowlabs.com/docs/en/licensed_annotators#relationextraction}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param batch_size batch size
#' @param dropout dropout coefficient
#' @param epochs_number maximum number of epochs to train
#' @param feature_scaling feature scaling method
#' @param fix_imbalance Fix the imbalance in the training set by replicating examples of under represented categories
#' @param from_entity_begin_col Column for beginning of 'from' entity
#' @param from_entity_end_col Column for end of 'from' entity
#' @param from_entity_label_col Column for 'from' entity label
#' @param label_col Column with label per each document
#' @param learning_rate learning rate
#' @param model_file location of file of the model used for classification
#' @param output_logs_path path to folder to output logs
#' @param to_entity_begin_col Column for beginning of 'to' entity
#' @param to_entity_end_col Column for end of 'to' entity
#' @param to_entity_label_col Column for 'to' entity label
#' @param validation_split Choose the proportion of training dataset to be validated against the model on each Epoch.
#' 
#' @export
nlp_relation_extraction <- function(x, input_cols, output_col,
                 batch_size = NULL, dropout = NULL, epochs_number = NULL, feature_scaling = NULL,
                 fix_imbalance = NULL, from_entity_begin_col = NULL, from_entity_end_col = NULL,
                 from_entity_label_col = NULL,
                 label_col = NULL, learning_rate = NULL, model_file = NULL, output_logs_path = NULL,
                 to_entity_begin_col = NULL, to_entity_end_col = NULL, to_entity_label_col = NULL,
                 validation_split = NULL,
                 uid = random_string("relation_extraction_")) {
  UseMethod("nlp_relation_extraction")
}

#' @export
nlp_relation_extraction.spark_connection <- function(x, input_cols, output_col,
                 batch_size = NULL, dropout = NULL, epochs_number = NULL, feature_scaling = NULL,
                 fix_imbalance = NULL, from_entity_begin_col = NULL, from_entity_end_col = NULL,
                 from_entity_label_col = NULL,
                 label_col = NULL, learning_rate = NULL, model_file = NULL, output_logs_path = NULL,
                 to_entity_begin_col = NULL, to_entity_end_col = NULL, to_entity_label_col = NULL,
                 validation_split = NULL,
                 uid = random_string("relation_extraction_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    batch_size = batch_size,
    dropout = dropout,
    epochs_number = epochs_number,
    feature_scaling = feature_scaling,
    fix_imbalance = fix_imbalance,
    from_entity_begin_col = from_entity_begin_col,
    from_entity_end_col = from_entity_end_col,
    from_entity_label_col = from_entity_label_col,
    label_col = label_col,
    learning_rate = learning_rate,
    model_file = model_file,
    output_logs_path = output_logs_path,
    to_entity_begin_col = to_entity_begin_col,
    to_entity_end_col = to_entity_end_col,
    to_entity_label_col = to_entity_label_col,
    validation_split = validation_split,
    uid = uid
  ) %>%
  validator_nlp_relation_extraction()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.re.RelationExtractionApproach",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setBatchSize", args[["batch_size"]])  %>%
    sparklyr::jobj_set_param("setEpochsNumber", args[["epochs_number"]])  %>%
    sparklyr::jobj_set_param("setFeatureScaling", args[["feature_scaling"]])  %>%
    sparklyr::jobj_set_param("setFixImbalance", args[["fix_imbalance"]])  %>%
    sparklyr::jobj_set_param("setLabelColumn", args[["label_col"]])  %>%
    sparklyr::jobj_set_param("setModelFile", args[["model_file"]])  %>%
    sparklyr::jobj_set_param("setOutputLogsPath", args[["output_logs_path"]])

  jobj <- invoke(jobj, "setFromEntity", args[["from_entity_begin_col"]], args[["from_entity_end_col"]], args[["from_entity_label_col"]])
  
  jobj <- invoke(jobj, "setToEntity", args[["to_entity_begin_col"]], args[["to_entity_end_col"]], args[["to_entity_label_col"]])
  
  model <- new_nlp_relation_extraction(jobj)
  
  if (!is.null(args[["dropout"]])) {
    model <- nlp_set_param(model, "dropout", args[["dropout"]])
  }
  
  if (!is.null(args[["validation_split"]])) {
    model <- nlp_set_param(model, "validation_split", args[["validation_split"]])    
  }

  if (!is.null(args[["learning_rate"]])) {
    # There is a defect with the setter name for learningRate parameter. 
    #model <- nlp_set_param(model, "learning_rate", args[["learning_rate"]])
    model <- invoke_static(sparklyr::spark_connection(model), "sparknlp.Utils", 
                            "setFloatParam", sparklyr::spark_jobj(model), "setlearningRate", args[["learning_rate"]]) %>% 
      sparklyr::ml_call_constructor()
  }
  
  return(model)
}

#' @export
nlp_relation_extraction.ml_pipeline <- function(x, input_cols, output_col,
                 batch_size = NULL, dropout = NULL, epochs_number = NULL, feature_scaling = NULL,
                 fix_imbalance = NULL, from_entity_begin_col = NULL, from_entity_end_col = NULL,
                 from_entity_label_col = NULL,
                 label_col = NULL, learning_rate = NULL, model_file = NULL, output_logs_path = NULL,
                 to_entity_begin_col = NULL, to_entity_end_col = NULL, to_entity_label_col = NULL,
                 validation_split = NULL,
                 uid = random_string("relation_extraction_")) {

  stage <- nlp_relation_extraction.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    batch_size = batch_size,
    dropout = dropout,
    epochs_number = epochs_number,
    feature_scaling = feature_scaling,
    fix_imbalance = fix_imbalance,
    from_entity_begin_col = from_entity_begin_col,
    from_entity_end_col = from_entity_end_col,
    from_entity_label_col = from_entity_label_col,
    label_col = label_col,
    learning_rate = learning_rate,
    model_file = model_file,
    output_logs_path = output_logs_path,
    to_entity_begin_col = to_entity_begin_col,
    to_entity_end_col = to_entity_end_col,
    to_entity_label_col = to_entity_label_col,
    validation_split = validation_split,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_relation_extraction.tbl_spark <- function(x, input_cols, output_col,
                 batch_size = NULL, dropout = NULL, epochs_number = NULL, feature_scaling = NULL,
                 fix_imbalance = NULL, from_entity_begin_col = NULL, from_entity_end_col = NULL,
                 from_entity_label_col = NULL,
                 label_col = NULL, learning_rate = NULL, model_file = NULL, output_logs_path = NULL,
                 to_entity_begin_col = NULL, to_entity_end_col = NULL, to_entity_label_col = NULL,
                 validation_split = NULL,
                 uid = random_string("relation_extraction_")) {
  stage <- nlp_relation_extraction.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    batch_size = batch_size,
    dropout = dropout,
    epochs_number = epochs_number,
    feature_scaling = feature_scaling,
    fix_imbalance = fix_imbalance,
    from_entity_begin_col = from_entity_begin_col,
    from_entity_end_col = from_entity_end_col,
    from_entity_label_col = from_entity_label_col,
    label_col = label_col,
    learning_rate = learning_rate,
    model_file = model_file,
    output_logs_path = output_logs_path,
    to_entity_begin_col = to_entity_begin_col,
    to_entity_end_col = to_entity_end_col,
    to_entity_label_col = to_entity_label_col,
    validation_split = validation_split,
    uid = uid
  )

  stage %>% sparklyr::ml_fit_and_transform(x)
}
#' @import forge
validator_nlp_relation_extraction <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["batch_size"]] <- cast_nullable_integer(args[["batch_size"]])
  args[["dropout"]] <- cast_nullable_double(args[["dropout"]])
  args[["epochs_number"]] <- cast_nullable_integer(args[["epochs_number"]])
  args[["feature_scaling"]] <- cast_nullable_string(args[["feature_scaling"]])
  args[["fix_imbalance"]] <- cast_nullable_logical(args[["fix_imbalance"]])
  args[["from_entity_begin_col"]] <- cast_nullable_string(args[["from_entity_begin_col"]])
  args[["from_entity_end_col"]] <- cast_nullable_string(args[["from_entity_end_col"]])
  args[["from_entity_label_col"]] <- cast_nullable_string(args[["from_entity_label_col"]])
  args[["label_col"]] <- cast_nullable_string(args[["label_col"]])
  args[["learning_rate"]] <- cast_nullable_double(args[["learning_rate"]])
  args[["model_file"]] <- cast_nullable_string(args[["model_file"]])
  args[["output_logs_path"]] <- cast_nullable_string(args[["output_logs_path"]])
  args[["to_entity_begin_col"]] <- cast_nullable_string(args[["to_entity_begin_col"]])
  args[["to_entity_end_col"]] <- cast_nullable_string(args[["to_entity_end_col"]])
  args[["to_entity_label_col"]] <- cast_nullable_string(args[["to_entity_label_col"]])
  args[["validation_split"]] <- cast_nullable_double(args[["validation_split"]])
  args
}

nlp_float_params.nlp_relation_extraction <- function(x) {
  return(c("dropout", "learning_rate", "validation_split"))
}

new_nlp_relation_extraction <- function(jobj) {
  sparklyr::new_ml_estimator(jobj, class = "nlp_relation_extraction")
}

new_nlp_relation_extraction_model <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_relation_extraction_model")
}

nlp_float_params.nlp_relation_extraction_model <- function(x) {
  return(c("prediction_threshold"))
}

#' Load a pretrained Spark NLP Relation Extraction model
#' 
#' Create a pretrained Spark NLP \code{RelationExtractionModel} model
#' 
#' @template roxlate-pretrained-params
#' @template roxlate-inputs-output-params
#' @param max_syntactic_distance Maximal syntactic distance, as threshold (Default: 0)
#' @param feature_scaling Feature scaling method.
#' @param prediction_threshold Minimal activation of the target unit to encode a new relation instance (Default: 0.5f)
#' @param relation_pairs List of dash-separated pairs of named entities ("ENTITY1-ENTITY2",
#'  e.g. "Biomarker-RelativeDay"), which will be processed
#' 
#' @export
nlp_relation_extraction_pretrained <- function(sc, input_cols, output_col, relation_pairs, max_syntactic_distance = NULL,
                                                feature_scaling = NULL, prediction_threshold = NULL,
                                                name = NULL, lang = NULL, remote_loc = NULL) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    relation_pairs = relation_pairs,
    max_syntactic_distance = max_syntactic_distance,
    feature_scaling = feature_scaling,
    prediction_threshold = prediction_threshold
  )
  
  args[["input_cols"]] <- forge::cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- forge::cast_string(args[["output_col"]])
  args[["relation_pairs"]] <- forge::cast_string_list(args[["relation_pairs"]])
  args[["max_syntactic_distance"]] <- forge::cast_nullable_integer(args[["max_syntactic_distance"]])
  args[["feature_scaling"]] <- forge::cast_nullable_string(args[["feature_scaling"]])
  args[["prediction_threshold"]] <- forge::cast_nullable_double(args[["prediction_threshold"]])
  
  model_class <- "com.johnsnowlabs.nlp.annotators.re.RelationExtractionModel"
  model <- pretrained_model(sc, model_class, name, lang, remote_loc)
  spark_jobj(model) %>%
    sparklyr::jobj_set_param("setInputCols", args[["input_cols"]]) %>% 
    sparklyr::jobj_set_param("setOutputCol", args[["output_col"]]) %>%
    sparklyr::jobj_set_param("setRelationPairs", args[["relation_pairs"]]) %>% 
    sparklyr::jobj_set_param("setMaxSyntacticDistance", args[["max_syntactic_distance"]]) %>% 
    sparklyr::jobj_set_param("setFeatureScaling", args[["feature_scaling"]])

  model <- new_nlp_relation_extraction_model(model)
    
  if (!is.null(args[["prediction_threshold"]])) {
    model <- nlp_set_param(model, "prediction_threshold", args[["prediction_threshold"]])
  }

  return(model)
}
