#' Spark NLP RelationExtractionDLModel
#'
#' Spark ML transformer that extracts and classifies instances of relations 
#' between named entities. In contrast with RelationExtractionModel, 
#' RelationExtractionDLModel is based on BERT. For pretrained models please 
#' see the Models Hub for available models.
#' 
#' See \url{https://nlp.johnsnowlabs.com/licensed/api/com/johnsnowlabs/nlp/annotators/re/RelationExtractionDLModel.html}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param category_names list of relation names
#' @param max_sentence_length Max sentence length to process (Default: 128)
#' @param prediction_threshold Minimal activation of the target unit to encode a new relation instance (Default: 0.5f)
#' 
#' @export
nlp_relation_extraction_dl <- function(x, input_cols, output_col,
                 category_names = NULL, max_sentence_length = NULL, prediction_threshold = NULL,
                 uid = random_string("relation_extraction_dl_")) {
  UseMethod("nlp_relation_extraction_dl")
}

#' @export
nlp_relation_extraction_dl.spark_connection <- function(x, input_cols, output_col,
                 category_names = NULL, max_sentence_length = NULL, prediction_threshold = NULL,
                 uid = random_string("relation_extraction_dl_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    category_names = category_names,
    max_sentence_length = max_sentence_length,
    prediction_threshold = prediction_threshold,
    uid = uid
  ) %>%
  validator_nlp_relation_extraction_dl()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.re.RelationExtractionDLModel",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setCategoryNames", args[["category_names"]])  %>%
    sparklyr::jobj_set_param("setMaxSentenceLength", args[["max_sentence_length"]])

  model <- new_nlp_relation_extraction_dl(jobj)
  
  if (!is.null(args[["prediction_threshold"]])) {
    model <- nlp_set_param(model, "prediction_threshold", args[["prediction_threshold"]])
  }
  
  return(model)
}

#' @export
nlp_relation_extraction_dl.ml_pipeline <- function(x, input_cols, output_col,
                 category_names = NULL, max_sentence_length = NULL, prediction_threshold = NULL,
                 uid = random_string("relation_extraction_dl_")) {

  stage <- nlp_relation_extraction_dl.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    category_names = category_names,
    max_sentence_length = max_sentence_length,
    prediction_threshold = prediction_threshold,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_relation_extraction_dl.tbl_spark <- function(x, input_cols, output_col,
                 category_names = NULL, max_sentence_length = NULL, prediction_threshold = NULL,
                 uid = random_string("relation_extraction_dl_")) {
  stage <- nlp_relation_extraction_dl.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    category_names = category_names,
    max_sentence_length = max_sentence_length,
    prediction_threshold = prediction_threshold,
    uid = uid
  )

  stage %>% sparklyr::ml_transform(x)
}
#' @import forge
validator_nlp_relation_extraction_dl <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["category_names"]] <- cast_nullable_string_list(args[["category_names"]])
  args[["max_sentence_length"]] <- cast_nullable_integer(args[["max_sentence_length"]])
  args[["prediction_threshold"]] <- cast_nullable_double(args[["prediction_threshold"]])
  args
}

#' Load a pretrained Spark NLP Relation Extraction DL model
#' 
#' Create a pretrained Spark NLP \code{RelationExtractionDLModel} model
#' 
#' @template roxlate-pretrained-params
#' @template roxlate-inputs-output-params
#' @param prediction_threshold Minimal activation of the target unit to encode a new relation instance (Default: 0.5f)
#' 
#' @export
nlp_relation_extraction_dl_pretrained <- function(sc, input_cols, output_col,
                                                  prediction_threshold = NULL,
                                                  name = NULL, lang = NULL, remote_loc = NULL) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    prediction_threshold = prediction_threshold
  )
  
  args[["input_cols"]] <- forge::cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- forge::cast_string(args[["output_col"]])
  args[["prediction_threshold"]] <- forge::cast_nullable_double(args[["prediction_threshold"]])

  model_class <- "com.johnsnowlabs.nlp.annotators.re.RelationExtractionDLModel"
  model <- pretrained_model(sc, model_class, name, lang, remote_loc)
  spark_jobj(model) %>%
    sparklyr::jobj_set_param("setInputCols", args[["input_cols"]]) %>% 
    sparklyr::jobj_set_param("setOutputCol", args[["output_col"]])
  
  model <- new_nlp_relation_extraction_dl(model)
  
  if (!is.null(args[["prediction_threshold"]])) {
    model <- nlp_set_param(model, "prediction_threshold", args[["prediction_threshold"]])
  }
  
  return(model)
}

nlp_float_params.nlp_relation_extraction_dl <- function(x) {
  return(c("prediction_threshold"))
}
new_nlp_relation_extraction_dl <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_relation_extraction_dl")
}
