#' Spark NLP AssertionLogRegApproach
#'
#' Spark ML estimator that 
#' See \url{https://nlp.johnsnowlabs.com/docs/en/licensed_annotators#assertionlogreg}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param label_column Column with one label per document
#' @param max_iter Max number of iterations for algorithm
#' @param reg Regularization parameter
#' @param enet Elastic net parameter
#' @param before Amount of tokens from the context before the target
#' @param after Amount of tokens from the context after the target
#' @param start_col Column that contains the token number for the start of the target
#' @param end_col Column that contains the token number for the end of the target
#' @param lazy_annotator a Param in Annotators that allows them to stand idle in the Pipeline and do nothing. Can be called by other Annotators in a RecursivePipeline
#' 
#' @export
nlp_assertion_logreg <- function(x, input_cols, output_col,
                 label_column = NULL, max_iter = NULL, reg = NULL, enet = NULL, before = NULL, after = NULL, start_col = NULL, end_col = NULL, lazy_annotator = NULL,
                 uid = random_string("assertion_logreg_")) {
  UseMethod("nlp_assertion_logreg")
}

#' @export
nlp_assertion_logreg.spark_connection <- function(x, input_cols, output_col,
                 label_column = NULL, max_iter = NULL, reg = NULL, enet = NULL, before = NULL, after = NULL, start_col = NULL, end_col = NULL, lazy_annotator = NULL,
                 uid = random_string("assertion_logreg_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    label_column = label_column,
    max_iter = max_iter,
    reg = reg,
    enet = enet,
    before = before,
    after = after,
    start_col = start_col,
    end_col = end_col,
    lazy_annotator = lazy_annotator,
    uid = uid
  ) %>%
  validator_nlp_assertion_logreg()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.assertion.logreg.AssertionLogRegApproach",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setLabelCol", args[["label_column"]])  %>%
    sparklyr::jobj_set_param("setMaxIter", args[["max_iter"]])  %>%
    sparklyr::jobj_set_param("setReg", args[["reg"]])  %>%
    sparklyr::jobj_set_param("setEnet", args[["enet"]])  %>%
    sparklyr::jobj_set_param("setBefore", args[["before"]])  %>%
    sparklyr::jobj_set_param("setAfter", args[["after"]])  %>%
    sparklyr::jobj_set_param("setStartCol", args[["start_col"]])  %>%
    sparklyr::jobj_set_param("setEndCol", args[["end_col"]])  %>%
    sparklyr::jobj_set_param("setLazyAnnotator", args[["lazy_annotator"]]) 

  new_nlp_assertion_logreg(jobj)
}

#' @export
nlp_assertion_logreg.ml_pipeline <- function(x, input_cols, output_col,
                 label_column = NULL, max_iter = NULL, reg = NULL, enet = NULL, before = NULL, after = NULL, start_col = NULL, end_col = NULL, lazy_annotator = NULL,
                 uid = random_string("assertion_logreg_")) {

  stage <- nlp_assertion_logreg.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    label_column = label_column,
    max_iter = max_iter,
    reg = reg,
    enet = enet,
    before = before,
    after = after,
    start_col = start_col,
    end_col = end_col,
    lazy_annotator = lazy_annotator,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_assertion_logreg.tbl_spark <- function(x, input_cols, output_col,
                 label_column = NULL, max_iter = NULL, reg = NULL, enet = NULL, before = NULL, after = NULL, start_col = NULL, end_col = NULL, lazy_annotator = NULL,
                 uid = random_string("assertion_logreg_")) {
  stage <- nlp_assertion_logreg.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    label_column = label_column,
    max_iter = max_iter,
    reg = reg,
    enet = enet,
    before = before,
    after = after,
    start_col = start_col,
    end_col = end_col,
    lazy_annotator = lazy_annotator,
    uid = uid
  )

  stage %>% sparklyr::ml_fit_and_transform(x)
}
#' @import forge
validator_nlp_assertion_logreg <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["label_column"]] <- cast_nullable_string(args[["label_column"]])
  args[["max_iter"]] <- cast_nullable_integer(args[["max_iter"]])
  args[["reg"]] <- cast_nullable_double(args[["reg"]])
  args[["enet"]] <- cast_nullable_double(args[["enet"]])
  args[["before"]] <- cast_nullable_integer(args[["before"]])
  args[["after"]] <- cast_nullable_integer(args[["after"]])
  args[["start_col"]] <- cast_nullable_string(args[["start_col"]])
  args[["end_col"]] <- cast_nullable_string(args[["end_col"]])
  args[["lazy_annotator"]] <- cast_nullable_logical(args[["lazy_annotator"]])
  args
}

#' Load a pretrained Spark NLP Assertion LogReg model
#' 
#' Create a pretrained Spark NLP \code{AssertionLogRegModel} model
#' 
#' @template roxlate-pretrained-params
#' @template roxlate-inputs-output-params
#' @param before Amount of tokens from the context before the target
#' @param after Amount of tokens from the context after the target
#' @param start_col Column that contains the token number for the start of the target
#' @param end_col Column that contains the token number for the end of the target
#' @param lazy_annotator a Param in Annotators that allows them to stand idle in the Pipeline and do nothing. Can be called by other Annotators in a RecursivePipeline
#' @param storage_ref storage reference for embeddings
#' @export
nlp_assertion_logreg_pretrained <- function(sc, input_cols, output_col, before = NULL,
                                            after = NULL, start_col = NULL, end_col = NULL,
                                            lazy_annotator = NULL, storage_ref = NULL,
                                        name = NULL, lang = NULL, remote_loc = NULL) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    before = before,
    after = after,
    start_col = start_col,
    end_col = end_col,
    lazy_annotator = lazy_annotator,
    storage_ref = storage_ref
  )
  
  args[["input_cols"]] <- forge::cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- forge::cast_string(args[["output_col"]])
  args[["before"]] <- cast_nullable_integer(args[["before"]])
  args[["after"]] <- cast_nullable_integer(args[["after"]])
  args[["start_col"]] <- cast_nullable_string(args[["start_col"]])
  args[["end_col"]] <- cast_nullable_string(args[["end_col"]])
  args[["lazy_annotator"]] <- cast_nullable_logical(args[["lazy_annotator"]])
  args[["storage_ref"]] <- forge::cast_nullable_string(args[["storage_ref"]])
  
  model_class <- "com.johnsnowlabs.nlp.annotators.assertion.logreg.AssertionLogRegModel"
  model <- pretrained_model(sc, model_class, name, lang, remote_loc)
  spark_jobj(model) %>%
    sparklyr::jobj_set_param("setInputCols", args[["input_cols"]]) %>% 
    sparklyr::jobj_set_param("setOutputCol", args[["output_col"]]) %>%
    sparklyr::jobj_set_param("setBefore", args[["before"]])  %>%
    sparklyr::jobj_set_param("setAfter", args[["after"]])  %>%
    sparklyr::jobj_set_param("setStartCol", args[["start_col"]])  %>%
    sparklyr::jobj_set_param("setEndCol", args[["end_col"]])  %>%
    sparklyr::jobj_set_param("setLazyAnnotator", args[["lazy_annotator"]]) %>% 
    sparklyr::jobj_set_param("setStorageRef", args[["storage_ref"]])

  new_nlp_assertion_logreg_model(model)
}

nlp_float_params.nlp_assertion_logreg <- function(x) {
  return(c())
}
new_nlp_assertion_logreg <- function(jobj) {
  sparklyr::new_ml_estimator(jobj, class = "nlp_assertion_logreg")
}
new_nlp_assertion_logreg_model <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_assertion_logreg_model")
}
nlp_float_params.nlp_assertion_logreg_model <- function(x) {
  return(c())
}
