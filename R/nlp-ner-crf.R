#' Spark NLP NerCrfApproach
#'
#' Spark ML estimator that allows for a generic model to be trained by utilizing a CRF machine learning algorithm. 
#' Its train data (train_ner) is either a labeled or an external CoNLL 2003 IOB based spark dataset with Annotations 
#' columns. Also the user has to provide word embeddings annotation column.
#' Optionally the user can provide an entity dictionary file for better accuracy.
#' See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#ner-crf}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param label_col If DatasetPath is not provided, this Seq[Annotation] type of column should have labeled data per token
#' @param min_epochs Minimum number of epochs to train
#' @param max_epochs Maximum number of epochs to train
#' @param l2 L2 regularization coefficient for CRF
#' @param C0 c0 defines decay speed for gradient
#' @param loss_eps If epoch relative improvement lass than this value, training is stopped
#' @param min_w Features with less weights than this value will be filtered out
#' @param external_features Path to file or folder of line separated file that has something like this: Volvo:ORG with such delimiter, readAs LINE_BY_LINE or SPARK_DATASET with options passed to the latter.
#' @param entities Array of entities to recognize
#' @param verbose Verbosity level
#' @param random_seed random seed
#' 
#' @return When \code{x} is a \code{spark_connection} the function returns a NerCrfApproach estimator.
#' When \code{x} is a \code{ml_pipeline} the pipeline with the NerCrfApproach added. When \code{x}
#' is a \code{tbl_spark} a transformed \code{tbl_spark}  (note that the Dataframe passed in must have the input_cols specified).
#' 
#' @export
nlp_ner_crf <- function(x, input_cols, output_col,
                 label_col = NULL, min_epochs = NULL, max_epochs = NULL, l2 = NULL, C0 = NULL, loss_eps = NULL, 
                 min_w = NULL, external_features = NULL, entities = NULL, verbose = NULL, random_seed = NULL,
                 uid = random_string("ner_crf_")) {
  UseMethod("nlp_ner_crf")
}

#' @export
nlp_ner_crf.spark_connection <- function(x, input_cols, output_col,
                 label_col = NULL, min_epochs = NULL, max_epochs = NULL, l2 = NULL, C0 = NULL, loss_eps = NULL, 
                 min_w = NULL, external_features = NULL, entities = NULL, verbose = NULL, random_seed = NULL,
                 uid = random_string("ner_crf_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    label_col = label_col,
    min_epochs = min_epochs,
    max_epochs = max_epochs,
    l2 = l2,
    C0 = C0,
    loss_eps = loss_eps,
    min_w = min_w,
    external_features = external_features,
    entities = entities,
    verbose = verbose,
    random_seed = random_seed,
    uid = uid
  ) %>%
  validator_nlp_ner_crf()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.ner.crf.NerCrfApproach",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setLabelColumn", args[["label_col"]])  %>%
    sparklyr::jobj_set_param("setMinEpochs", args[["min_epochs"]])  %>%
    sparklyr::jobj_set_param("setMaxEpochs", args[["max_epochs"]])  %>%
    sparklyr::jobj_set_param("setL2", args[["l2"]])  %>%
    sparklyr::jobj_set_param("setC0", args[["C0"]])  %>%
    sparklyr::jobj_set_param("setLossEps", args[["loss_eps"]])  %>%
    sparklyr::jobj_set_param("setMinW", args[["min_w"]])  %>%
    sparklyr::jobj_set_param("setExternalFeatures", args[["external_features"]])  %>%
    sparklyr::jobj_set_param("setEntities", args[["entities"]])  %>%
    sparklyr::jobj_set_param("setVerbose", args[["verbose"]])  %>%
    sparklyr::jobj_set_param("setRandomSeed", args[["random_seed"]]) 

  new_nlp_ner_crf(jobj)
}

#' @export
nlp_ner_crf.ml_pipeline <- function(x, input_cols, output_col,
                 label_col = NULL, min_epochs = NULL, max_epochs = NULL, l2 = NULL, C0 = NULL, loss_eps = NULL, min_w = NULL, external_features = NULL, entities = NULL, verbose = NULL, random_seed = NULL,
                 uid = random_string("ner_crf_")) {

  stage <- nlp_ner_crf.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    label_col = label_col,
    min_epochs = min_epochs,
    max_epochs = max_epochs,
    l2 = l2,
    C0 = C0,
    loss_eps = loss_eps,
    min_w = min_w,
    external_features = external_features,
    entities = entities,
    verbose = verbose,
    random_seed = random_seed,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_ner_crf.tbl_spark <- function(x, input_cols, output_col,
                 label_col = NULL, min_epochs = NULL, max_epochs = NULL, l2 = NULL, C0 = NULL, loss_eps = NULL, min_w = NULL, external_features = NULL, entities = NULL, verbose = NULL, random_seed = NULL,
                 uid = random_string("ner_crf_")) {
  stage <- nlp_ner_crf.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    label_col = label_col,
    min_epochs = min_epochs,
    max_epochs = max_epochs,
    l2 = l2,
    C0 = C0,
    loss_eps = loss_eps,
    min_w = min_w,
    external_features = external_features,
    entities = entities,
    verbose = verbose,
    random_seed = random_seed,
    uid = uid
  )

  stage %>% sparklyr::ml_fit_and_transform(x)
}
#' @import forge
validator_nlp_ner_crf <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["label_col"]] <- cast_nullable_string(args[["label_col"]])
  args[["min_epochs"]] <- cast_nullable_integer(args[["min_epochs"]])
  args[["max_epochs"]] <- cast_nullable_integer(args[["max_epochs"]])
  args[["l2"]] <- cast_nullable_double(args[["l2"]])
  args[["C0"]] <- cast_nullable_integer(args[["C0"]])
  args[["loss_eps"]] <- cast_nullable_double(args[["loss_eps"]])
  args[["min_w"]] <- cast_nullable_double(args[["min_w"]])
  args[["external_features"]] <- cast_nullable_string_list(args[["external_features"]])
  args[["entities"]] <- cast_nullable_string_list(args[["entities"]])
  args[["verbose"]] <- cast_nullable_integer(args[["verbose"]])
  args[["random_seed"]] <- cast_nullable_integer(args[["random_seed"]])
  args
}

new_nlp_ner_crf <- function(jobj) {
  sparklyr::new_ml_estimator(jobj, class = "nlp_ner_crf")
}
