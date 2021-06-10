#' Spark NLP DocumentLogRegClassifierApproach
#'
#' Spark ML estimator that 
#' See \url{https://nlp.johnsnowlabs.com/licensed/api/com/johnsnowlabs/nlp/annotators/classification/DocumentLogRegClassifierApproach.html}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param fit_intercept whether to fit an intercept term (Default: true)
#' @param label_column column with the value result we are trying to predict.
#' @param labels array to output the label in the original form.
#' @param max_iter maximum number of iterations (Default: 10)
#' @param merge_chunks whether to merge all chunks in a document or not (Default: false)
#' @param tol convergence tolerance after each iteration (Default: 1e-6)
#' 
#' @export
nlp_document_logreg_classifier <- function(x, input_cols, output_col,
                 fit_intercept = NULL, label_column = NULL, labels = NULL, max_iter = NULL, merge_chunks = NULL, tol = NULL,
                 uid = random_string("document_logreg_classifier_")) {
  UseMethod("nlp_document_logreg_classifier")
}

#' @export
nlp_document_logreg_classifier.spark_connection <- function(x, input_cols, output_col,
                 fit_intercept = NULL, label_column = NULL, labels = NULL, max_iter = NULL, merge_chunks = NULL, tol = NULL,
                 uid = random_string("document_logreg_classifier_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    fit_intercept = fit_intercept,
    label_column = label_column,
    labels = labels,
    max_iter = max_iter,
    merge_chunks = merge_chunks,
    tol = tol,
    uid = uid
  ) %>%
  validator_nlp_document_logreg_classifier()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.classification.DocumentLogRegClassifierApproach",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setFitIntercept", args[["fit_intercept"]])  %>%
    sparklyr::jobj_set_param("setLabelCol", args[["label_column"]])  %>%
    sparklyr::jobj_set_param("setLabels", args[["labels"]])  %>%
    sparklyr::jobj_set_param("setMaxIter", args[["max_iter"]])  %>%
    sparklyr::jobj_set_param("setMergeChunks", args[["merge_chunks"]])  %>%
    sparklyr::jobj_set_param("setTol", args[["tol"]]) 

  new_nlp_document_logreg_classifier(jobj)
}

#' @export
nlp_document_logreg_classifier.ml_pipeline <- function(x, input_cols, output_col,
                 fit_intercept = NULL, label_column = NULL, labels = NULL, max_iter = NULL, merge_chunks = NULL, tol = NULL,
                 uid = random_string("document_logreg_classifier_")) {

  stage <- nlp_document_logreg_classifier.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    fit_intercept = fit_intercept,
    label_column = label_column,
    labels = labels,
    max_iter = max_iter,
    merge_chunks = merge_chunks,
    tol = tol,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_document_logreg_classifier.tbl_spark <- function(x, input_cols, output_col,
                 fit_intercept = NULL, label_column = NULL, labels = NULL, max_iter = NULL, merge_chunks = NULL, tol = NULL,
                 uid = random_string("document_logreg_classifier_")) {
  stage <- nlp_document_logreg_classifier.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    fit_intercept = fit_intercept,
    label_column = label_column,
    labels = labels,
    max_iter = max_iter,
    merge_chunks = merge_chunks,
    tol = tol,
    uid = uid
  )

  stage %>% sparklyr::ml_fit_and_transform(x)
}
#' @import forge
validator_nlp_document_logreg_classifier <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["fit_intercept"]] <- cast_nullable_logical(args[["fit_intercept"]])
  args[["label_column"]] <- cast_nullable_string(args[["label_column"]])
  args[["labels"]] <- cast_nullable_string_list(args[["labels"]])
  args[["max_iter"]] <- cast_nullable_integer(args[["max_iter"]])
  args[["merge_chunks"]] <- cast_nullable_logical(args[["merge_chunks"]])
  args[["tol"]] <- cast_nullable_double(args[["tol"]])
  args
}

nlp_float_params.nlp_document_logreg_classifier <- function(x) {
  return(c())
}
new_nlp_document_logreg_classifier <- function(jobj) {
  sparklyr::new_ml_estimator(jobj, class = "nlp_document_logreg_classifier")
}
new_nlp_document_logreg_classifier_model <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_document_logreg_classifier_model")
}
nlp_float_params.nlp_document_logreg_classifier_model <- function(x) {
  return(c())
}
