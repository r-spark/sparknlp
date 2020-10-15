#' Load a pretrained Spark NLP LanguageDetectorDL model
#' 
#' Create a pretrained Spark NLP \code{LanguageDetectorDL} model
#' 
#' @template roxlate-pretrained-params
#' @template roxlate-inputs-output-params
#' @param alphabet alphabet used to feed the TensorFlow model for prediction (Map[String, Int]) This should be an R environment
#' @param coelesce_sentences If sets to true the output of all sentences will be averaged to one output instead of one output per sentence. (boolean)
#' @param language used to map prediction to two-letter (ISO 639-1) language codes (Map[String, Int]) This should be an R environment
#' @param threshold The minimum threshold for the final result otheriwse it will be either Unknown or the value set in thresholdLabel.
#' @param threshold_label In case the score of prediction is less than threshold, what should be the label.
#' @export
nlp_language_detector_dl_pretrained <- function(sc, input_cols, output_col, alphabet = NULL, coelesce_sentences = NULL,
                                             language = NULL, threshold = NULL, threshold_label = NULL,
                                      name = NULL, lang = NULL, remote_loc = NULL) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    alphabet = alphabet,
    coelesce_sentences = coelesce_sentences,
    language = language,
    threshold = threshold,
    threshold_label = threshold_label
  ) %>%
    validator_nlp_language_detector_dl()
  
  model_class <- "com.johnsnowlabs.nlp.annotators.ld.dl.LanguageDetectorDL"
  model <- pretrained_model(sc, model_class, name, lang, remote_loc)
  spark_jobj(model) %>%
    sparklyr::jobj_set_param("setInputCols", args[["input_cols"]]) %>% 
    sparklyr::jobj_set_param("setOutputCol", args[["output_col"]]) %>% 
    sparklyr::jobj_set_param("setAlphabet", args[["alphabet"]]) %>% 
    sparklyr::jobj_set_param("setCoelesceSentences", args[["coelesce_sentences"]]) %>% 
    sparklyr::jobj_set_param("setLanguage", args[["language"]]) %>% 
    sparklyr::jobj_set_param("setThresholdLabel", args[["threshold_label"]])

  model <- new_nlp_language_detector_dl(model)
    
  if (!is.null(threshold)) {
    model <- nlp_set_param(model, "threshold", args[["threshold"]])
  }
  
  return(model)
}

#' @import forge
validator_nlp_language_detector_dl <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
#  args[["alphabet"]] <- cast_nullable_(args[["alphabet"]])
  args[["coelesce_sentences"]] <- cast_nullable_logical(args[["coelesce_sentences"]])
#  args[["language"]] <- cast_nullable_(args[["language"]])
  args[["threshold"]] <- cast_nullable_double(args[["threshold"]])
  args[["threshold_label"]] <- cast_nullable_string(args[["threshold_label"]])
  args
}

nlp_float_params.nlp_language_detector_dl <- function(x) {
  return(c("threshold"))
}

new_nlp_language_detector_dl <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_language_detector_dl")
}

