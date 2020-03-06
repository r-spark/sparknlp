#' Spark NLP NorvigSweetingApproach
#'
#' Spark ML estimator that  retrieves tokens and makes corrections automatically if not found in an English dictionary
#' See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#norvig-spellchecker}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param dictionary_path path to file with properly spelled words
#' @param dictionary_token_pattern tokenPattern is the regex pattern to identify them in text,
#' @param dictionary_read_as TEXT or SPARK_DATASET
#' @param dictionary_options options passed to Spark reader 
#' @param case_sensitive defaults to false. Might affect accuracy
#' @param double_variants enables extra check for word combinations, more accuracy at performance
#' @param short_circuit  faster but less accurate mode
#' @param word_size_ignore Minimum size of word before moving on. Defaults to 3.
#' @param dups_limit Maximum duplicate of characters to account for. Defaults to 2.
#' @param reduct_limit Word reduction limit. Defaults to 3
#' @param intersections Hamming intersections to attempt. Defaults to 10.
#' @param vowel_swap_limit Vowel swap attempts. Defaults to 6.
#' 
#' @export
nlp_norvig_spell_checker <- function(x, input_cols, output_col,
                 dictionary_path = NULL, dictionary_token_pattern = "\\S+", dictionary_read_as = "TEXT", 
                 dictionary_options = list("format" = "text"), 
                 case_sensitive = NULL, double_variants = NULL,
                 short_circuit = NULL, word_size_ignore = NULL, dups_limit = NULL, reduct_limit = NULL, 
                 intersections = NULL, vowel_swap_limit = NULL,
                 uid = random_string("norvig_spell_checker_")) {
  UseMethod("nlp_norvig_spell_checker")
}

#' @export
nlp_norvig_spell_checker.spark_connection <- function(x, input_cols, output_col,
                                                      dictionary_path = NULL, dictionary_token_pattern = "\\S+", dictionary_read_as = "TEXT", 
                                                      dictionary_options = list("format" = "text"), 
                                                      case_sensitive = NULL, double_variants = NULL,
                                                      short_circuit = NULL, word_size_ignore = NULL, dups_limit = NULL, reduct_limit = NULL, 
                                                      intersections = NULL, vowel_swap_limit = NULL,
                                                      uid = random_string("norvig_spell_checker_")){
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    dictionary_path = dictionary_path,
    dictionary_token_pattern = dictionary_token_pattern,
    dictionary_read_as = dictionary_read_as,
    dictionary_options = dictionary_options,
    case_sensitive = case_sensitive,
    double_variants = double_variants,
    short_circuit = short_circuit,
    word_size_ignore = word_size_ignore,
    dups_limit = dups_limit,
    reduct_limit = reduct_limit,
    intersections = intersections,
    vowel_swap_limit = vowel_swap_limit,
    uid = uid
  ) %>%
  validator_nlp_norvig_spell_checker()
  
  if (!is.null(args[["dictionary_options"]])) {
    args[["dictionary_options"]] = list2env(args[["dictionary_options"]])
  }

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.spell.norvig.NorvigSweetingApproach",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setCaseSensitive", args[["case_sensitive"]])  %>%
    sparklyr::jobj_set_param("setDoubleVariants", args[["double_variants"]])  %>%
    sparklyr::jobj_set_param("setShortCircuit", args[["short_circuit"]])  %>%
    sparklyr::jobj_set_param("setWordSizeIgnore", args[["word_size_ignore"]])  %>%
    sparklyr::jobj_set_param("setDupsLimit", args[["dups_limit"]])  %>%
    sparklyr::jobj_set_param("setReductLimit", args[["reduct_limit"]])  %>%
    sparklyr::jobj_set_param("setIntersections", args[["intersections"]])  %>%
    sparklyr::jobj_set_param("setVowelSwapLimit", args[["vowel_swap_limit"]]) 
  
  if (!is.null(args[["dictionary_path"]])) {
    sparklyr::invoke(jobj, "setDictionary", args[["dictionary_path"]], args[["dictionary_token_pattern"]],
                     read_as(x, args[["dictionary_read_as"]]), args[["dictionary_options"]])
  }

  new_nlp_norvig_spell_checker(jobj)
}

#' @export
nlp_norvig_spell_checker.ml_pipeline <- function(x, input_cols, output_col,
                                                 dictionary_path = NULL, dictionary_token_pattern = "\\S+", dictionary_read_as = "TEXT", 
                                                 dictionary_options = list("format" = "text"), 
                                                 case_sensitive = NULL, double_variants = NULL,
                                                 short_circuit = NULL, word_size_ignore = NULL, dups_limit = NULL, reduct_limit = NULL, 
                                                 intersections = NULL, vowel_swap_limit = NULL,
                                                 uid = random_string("norvig_spell_checker_")) {

  stage <- nlp_norvig_spell_checker.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    dictionary_path = dictionary_path,
    dictionary_token_pattern = dictionary_token_pattern,
    dictionary_read_as = dictionary_read_as,
    dictionary_options = dictionary_options,
    case_sensitive = case_sensitive,
    double_variants = double_variants,
    short_circuit = short_circuit,
    word_size_ignore = word_size_ignore,
    dups_limit = dups_limit,
    reduct_limit = reduct_limit,
    intersections = intersections,
    vowel_swap_limit = vowel_swap_limit,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_norvig_spell_checker.tbl_spark <- function(x, input_cols, output_col,
                                               dictionary_path = NULL, dictionary_token_pattern = "\\S+", dictionary_read_as = "TEXT", 
                                               dictionary_options = list("format" = "text"), 
                                               case_sensitive = NULL, double_variants = NULL,
                                               short_circuit = NULL, word_size_ignore = NULL, dups_limit = NULL, reduct_limit = NULL, 
                                               intersections = NULL, vowel_swap_limit = NULL,
                                               uid = random_string("norvig_spell_checker_")) {
  stage <- nlp_norvig_spell_checker.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    dictionary_path = dictionary_path,
    dictionary_token_pattern = dictionary_token_pattern,
    dictionary_read_as = dictionary_read_as,
    dictionary_options = dictionary_options,
    case_sensitive = case_sensitive,
    double_variants = double_variants,
    short_circuit = short_circuit,
    word_size_ignore = word_size_ignore,
    dups_limit = dups_limit,
    reduct_limit = reduct_limit,
    intersections = intersections,
    vowel_swap_limit = vowel_swap_limit,
    uid = uid
  )

  stage %>% sparklyr::ml_fit_and_transform(x)
}

#' Load a pretrained Spark NLP model
#' 
#' Create a pretrained Spark NLP \code{NorvigSweetingModel} model
#' 
#' @template roxlate-pretrained-params
#' @template roxlate-inputs-output-params
#' @export
nlp_norvig_spell_checker_pretrained <- function(sc, input_cols, output_col,
                                      name = NULL, lang = NULL, remote_loc = NULL) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col
  ) %>%
    validator_nlp_norvig_spell_checker()
  
  model_class <- "com.johnsnowlabs.nlp.annotators.spell.norvig.NorvigSweetingModel"
  model <- pretrained_model(sc, model_class, name, lang, remote_loc)
  spark_jobj(model) %>%
    sparklyr::jobj_set_param("setInputCols", args[["input_cols"]]) %>% 
    sparklyr::jobj_set_param("setOutputCol", args[["output_col"]])
  
  new_ml_transformer(model)
}

#' @import forge
validator_nlp_norvig_spell_checker <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["dictionary_path"]] <- cast_nullable_string(args[["dictionary_path"]])
  args[["dictionary_token_pattern"]] <- cast_nullable_string(args[["dictionary_token_pattern"]])
  args[["case_sensitive"]] <- cast_nullable_logical(args[["case_sensitive"]])
  args[["double_variants"]] <- cast_nullable_logical(args[["double_variants"]])
  args[["short_circuit"]] <- cast_nullable_logical(args[["short_circuit"]])
  args[["word_size_ignore"]] <- cast_nullable_integer(args[["word_size_ignore"]])
  args[["dups_limit"]] <- cast_nullable_integer(args[["dups_limit"]])
  args[["reduct_limit"]] <- cast_nullable_integer(args[["reduct_limit"]])
  args[["intersections"]] <- cast_nullable_integer(args[["intersections"]])
  args[["vowel_swap_limit"]] <- cast_nullable_integer(args[["vowel_swap_limit"]])
  args
}

new_nlp_norvig_spell_checker <- function(jobj) {
  sparklyr::new_ml_estimator(jobj, class = "nlp_norvig_spell_checker")
}
