#' Spark NLP ContextSpellCheckerApproach
#'
#' Spark ML estimator that Implements Noisy Channel Model Spell Algorithm. Correction candidates are extracted combining
#' context information and word information
#' See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#context-spellchecker}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param batch_size batch size for training in NLM. Defaults to 24
#' @param blacklist_min_freq blacklist
#' @param case_strategy What case combinations to try when generating candidates. ALL_UPPER_CASE = 0, 
#' FIRST_LETTER_CAPITALIZED = 1, ALL = 2. Defaults to 2.
#' @param class_threshold class threshold
#' @param epochs Number of epochs to train the language model. Defaults to 2.
#' @param error_threshold Threshold perplexity for a word to be considered as an error. Defaults to 10f.
#' @param final_learning_rate Final learning rate for the LM. Defaults to 0.0005
#' @param initial_learning_rate Initial learning rate for the LM. Defaults to 0.7
#' @param lm_classes Number of classes to use during factorization of the softmax output in the LM. Defaults to 2000.
#' @param lazy_annotator lazy annotator
#' @param max_candidates Maximum number of candidates for every word. Defaults to 6.
#' @param max_window_len Maximum size for the window used to remember history prior to every correction. Defaults to 5.
#' @param min_count Min number of times a token should appear to be included in vocab. Defaults to 3.0f.
#' @param tradeoff Tradeoff between the cost of a word error and a transition in the language model. Defaults to 18.0f.
#' @param validation_fraction Percentage of datapoints to use for validation. Defaults to .1f.
#' @param weights The path to the file containing the weights for the levenshtein distance.
#' @param word_max_dist Maximum distance for the generated candidates for every word. Defaults to 3.
#' 
#' @export
nlp_context_spell_checker <- function(x, input_cols, output_col,
                 batch_size = NULL, blacklist_min_freq = NULL, case_strategy = NULL, class_threshold = NULL,
                 epochs = NULL, error_threshold = NULL, final_learning_rate = NULL, initial_learning_rate = NULL, 
                 lm_classes = NULL, lazy_annotator = NULL, max_candidates = NULL, max_window_len = NULL, 
                 min_count = NULL, tradeoff = NULL, validation_fraction = NULL, weights = NULL, word_max_dist = NULL,
                 uid = random_string("context_spell_checker_")) {
  UseMethod("nlp_context_spell_checker")
}

#' @export
nlp_context_spell_checker.spark_connection <- function(x, input_cols, output_col,
                 batch_size = NULL, blacklist_min_freq = NULL, case_strategy = NULL, class_threshold = NULL,
                 epochs = NULL, error_threshold = NULL, final_learning_rate = NULL, initial_learning_rate = NULL, 
                 lm_classes = NULL, lazy_annotator = NULL, max_candidates = NULL, max_window_len = NULL, 
                 min_count = NULL, tradeoff = NULL, validation_fraction = NULL, weights = NULL, word_max_dist = NULL,
                 uid = random_string("context_spell_checker_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    batch_size = batch_size,
    blacklist_min_freq = blacklist_min_freq,
    case_strategy = case_strategy,
    class_threshold = class_threshold,
    epochs = epochs,
    error_threshold = error_threshold,
    final_learning_rate = final_learning_rate,
    initial_learning_rate = initial_learning_rate,
    lm_classes = lm_classes,
    lazy_annotator = lazy_annotator,
    max_candidates = max_candidates,
    max_window_len = max_window_len,
    min_count = min_count,
    tradeoff = tradeoff,
    validation_fraction = validation_fraction,
    weights = weights,
    word_max_dist = word_max_dist,
    uid = uid
  ) %>%
  validator_nlp_context_spell_checker()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.spell.context.ContextSpellCheckerApproach",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setBatchSize", args[["batch_size"]])  %>%
    sparklyr::jobj_set_param("setBlackListMinFreq", args[["blacklist_min_freq"]])  %>%
    sparklyr::jobj_set_param("setCaseStrategy", args[["case_strategy"]])  %>%
    sparklyr::jobj_set_param("setClassThreshold", args[["class_threshold"]])  %>%
    sparklyr::jobj_set_param("setEpochs", args[["epochs"]])  %>%
    sparklyr::jobj_set_param("setLMClasses", args[["lm_classes"]])  %>%
    sparklyr::jobj_set_param("setLazyAnnotator", args[["lazy_annotator"]])  %>%
    sparklyr::jobj_set_param("setMaxCandidates", args[["max_candidates"]])  %>%
    sparklyr::jobj_set_param("setMaxWindowLen", args[["max_window_len"]])  %>%
    sparklyr::jobj_set_param("setMinCount", args[["min_count"]])  %>%
    sparklyr::jobj_set_param("setWeights", args[["weights"]])  %>%
    sparklyr::jobj_set_param("setWordMaxDist", args[["word_max_dist"]]) 
  
  if (!is.null(error_threshold)) {
    jobj <- sparklyr::invoke_static(x, "sparknlp.Utils", "setCSCerrorThreshold", jobj, args[["error_threshold"]])
  }
  
  if (!is.null(final_learning_rate)) {
    jobj <- sparklyr::invoke_static(x, "sparknlp.Utils", "setCSCFinalLR", jobj, args[["final_learning_rate"]])
  }
  
  if (!is.null(initial_learning_rate)) {
    jobj <- sparklyr::invoke_static(x, "sparknlp.Utils", "setCSCinitialLR", jobj, args[["initial_learning_rate"]])
  }
  
  if (!is.null(tradeoff)) {
    jobj <- sparklyr::invoke_static(x, "sparknlp.Utils", "setCSCtradeoff", jobj, args[["tradeoff"]])
  }
  
  if (!is.null(validation_fraction)) {
    jobj <- sparklyr::invoke_static(x, "sparknlp.Utils", "setCSCvalidFraction", jobj, args[["validation_fraction"]])
  }

  new_nlp_context_spell_checker(jobj)
}

#' @export
nlp_context_spell_checker.ml_pipeline <- function(x, input_cols, output_col,
                 batch_size = NULL, blacklist_min_freq = NULL, case_strategy = NULL, class_threshold = NULL, 
                 epochs = NULL, error_threshold = NULL, final_learning_rate = NULL, initial_learning_rate = NULL,
                 lm_classes = NULL, lazy_annotator = NULL, max_candidates = NULL, max_window_len = NULL,
                 min_count = NULL, tradeoff = NULL, validation_fraction = NULL, weights = NULL, word_max_dist = NULL,
                 uid = random_string("context_spell_checker_")) {

  stage <- nlp_context_spell_checker.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    batch_size = batch_size,
    blacklist_min_freq = blacklist_min_freq,
    case_strategy = case_strategy,
    class_threshold = class_threshold,
    epochs = epochs,
    error_threshold = error_threshold,
    final_learning_rate = final_learning_rate,
    initial_learning_rate = initial_learning_rate,
    lm_classes = lm_classes,
    lazy_annotator = lazy_annotator,
    max_candidates = max_candidates,
    max_window_len = max_window_len,
    min_count = min_count,
    tradeoff = tradeoff,
    validation_fraction = validation_fraction,
    weights = weights,
    word_max_dist = word_max_dist,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_context_spell_checker.tbl_spark <- function(x, input_cols, output_col,
                 batch_size = NULL, blacklist_min_freq = NULL, case_strategy = NULL, class_threshold = NULL, epochs = NULL, error_threshold = NULL, final_learning_rate = NULL, initial_learning_rate = NULL, lm_classes = NULL, lazy_annotator = NULL, max_candidates = NULL, max_window_len = NULL, min_count = NULL, tradeoff = NULL, validation_fraction = NULL, weights = NULL, word_max_dist = NULL,
                 uid = random_string("context_spell_checker_")) {
  stage <- nlp_context_spell_checker.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    batch_size = batch_size,
    blacklist_min_freq = blacklist_min_freq,
    case_strategy = case_strategy,
    class_threshold = class_threshold,
    epochs = epochs,
    error_threshold = error_threshold,
    final_learning_rate = final_learning_rate,
    initial_learning_rate = initial_learning_rate,
    lm_classes = lm_classes,
    lazy_annotator = lazy_annotator,
    max_candidates = max_candidates,
    max_window_len = max_window_len,
    min_count = min_count,
    tradeoff = tradeoff,
    validation_fraction = validation_fraction,
    weights = weights,
    word_max_dist = word_max_dist,
    uid = uid
  )

  stage %>% sparklyr::ml_fit_and_transform(x)
}
#' @import forge
validator_nlp_context_spell_checker <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["batch_size"]] <- cast_nullable_integer(args[["batch_size"]])
  args[["blacklist_min_freq"]] <- cast_nullable_integer(args[["blacklist_min_freq"]])
  args[["case_strategy"]] <- cast_nullable_integer(args[["case_strategy"]])
  args[["class_threshold"]] <- cast_nullable_double(args[["class_threshold"]])
  args[["epochs"]] <- cast_nullable_integer(args[["epochs"]])
  args[["error_threshold"]] <- cast_nullable_double(args[["error_threshold"]])
  args[["final_learning_rate"]] <- cast_nullable_double(args[["final_learning_rate"]])
  args[["initial_learning_rate"]] <- cast_nullable_double(args[["initial_learning_rate"]])
  args[["lm_classes"]] <- cast_nullable_integer(args[["lm_classes"]])
  args[["lazy_annotator"]] <- cast_nullable_logical(args[["lazy_annotator"]])
  args[["max_candidates"]] <- cast_nullable_integer(args[["max_candidates"]])
  args[["max_window_len"]] <- cast_nullable_integer(args[["max_window_len"]])
  args[["min_count"]] <- cast_nullable_double(args[["min_count"]])
  args[["tradeoff"]] <- cast_nullable_double(args[["tradeoff"]])
  args[["validation_fraction"]] <- cast_nullable_double(args[["validation_fraction"]])
  args[["weights"]] <- cast_nullable_string(args[["weights"]])
  args[["word_max_dist"]] <- cast_nullable_integer(args[["word_max_dist"]])
  args
}

new_nlp_context_spell_checker <- function(jobj) {
  sparklyr::new_ml_estimator(jobj, class = "nlp_context_spell_checker")
}


