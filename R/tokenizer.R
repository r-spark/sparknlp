#' Spark NLP Tokenizer approach
#' 
#' Spark ML estimator that identifies tokens with tokenization open standards. A few rules will help customizing
#' it if defaults do not fit user needs. See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#tokenizer}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param exceptions String array. List of tokens to not alter at all. Allows composite tokens like two worded tokens that the user may not want to split.
#' @param exceptions_path NOTE: NOT IMPLEMENTED. String. Path to txt file with list of token exceptions
#' @param case_sensitive_exceptions Boolean. Whether to follow case sensitiveness for matching exceptions in text
#' @param context_chars String array. Whether to follow case sensitiveness for matching exceptions in text
#' @param split_chars String array.  List of 1 character string to rip off from tokens, such as parenthesis or question marks. Ignored if using prefix, infix or suffix patterns.
#' @param target_pattern String. Basic regex rule to identify a candidate for tokenization. Defaults to `\\S+` which means anything not a space
#' @param suffix_pattern String. Regex to identify subtokens that are in the end of the token. Regex has to end with `\\z` and must contain groups (). Each group will become a separate token within the prefix. Defaults to non-letter characters. e.g. quotes or parenthesis
#' @param prefix_pattern String. Regex to identify subtokens that come in the beginning of the token. Regex has to start with `\\A` and must contain groups (). Each group will become a separate token within the prefix. Defaults to non-letter characters. e.g. quotes or parenthesis
#' @param infix_patterns String array. extension pattern regex with groups to the top of the rules (will target first, from more specific to the more general).
#' 
#' @export
nlp_tokenizer <- function(x, input_cols, output_col,
                 exceptions = NULL, exceptions_path = NULL, case_sensitive_exceptions = NULL, context_chars = NULL,
                 split_chars = NULL, target_pattern = NULL, suffix_pattern = NULL, prefix_pattern = NULL, 
                 infix_patterns = NULL,
                 uid = random_string("tokenizer_")) {
  UseMethod("nlp_tokenizer")
}

#' @export
nlp_tokenizer.spark_connection <- function(x, input_cols, output_col,
                                           exceptions = NULL, exceptions_path = NULL, case_sensitive_exceptions = NULL, context_chars = NULL,
                                           split_chars = NULL, target_pattern = NULL, suffix_pattern = NULL, prefix_pattern = NULL, 
                                           infix_patterns = NULL,
                                           uid = random_string("tokenizer_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    exceptions = exceptions,
    exceptions_path = exceptions_path,
    case_sensitive_exceptions = case_sensitive_exceptions,
    context_chars = context_chars,
    split_chars = split_chars,
    target_pattern = target_pattern,
    suffix_pattern = suffix_pattern,
    prefix_pattern = prefix_pattern,
    infix_patterns = infix_patterns,
    uid = uid
  ) %>% 
   validator_nlp_tokenizer()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.Tokenizer",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setExceptions", args[["exceptions"]]) %>%
    #sparklyr::jobj_set_param("setExceptionsPath", args[["exceptions_path"]]) %>%
    sparklyr::jobj_set_param("setCaseSensitiveExceptions", args[["case_sensitive_exceptions"]]) %>%
    sparklyr::jobj_set_param("setContextChars", args[["context_chars"]]) %>%
    sparklyr::jobj_set_param("setSplitChars", args[["split_chars"]]) %>%
    sparklyr::jobj_set_param("setTargetPattern", args[["target_pattern"]]) %>%
    sparklyr::jobj_set_param("setSuffixPattern", args[["suffix_pattern"]]) %>%
    sparklyr::jobj_set_param("setPrefixPattern", args[["prefix_pattern"]]) %>%
    sparklyr::jobj_set_param("setInfixPatterns", args[["infix_patterns"]])

  new_nlp_tokenizer(jobj)
}

#' @export
nlp_tokenizer.ml_pipeline <- function(x, input_cols, output_col,
                             exceptions = NULL, exceptions_path = NULL, case_sensitive_exceptions = NULL, context_chars = NULL,
                             split_chars = NULL, target_pattern = NULL, suffix_pattern = NULL, prefix_pattern = NULL, 
                             infix_patterns = NULL,
                             uid = random_string("tokenizer_")) {

  stage <- nlp_tokenizer.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    exceptions = exceptions,
    #exceptions_path = exceptions_path,
    case_sensitive_exceptions = case_sensitive_exceptions,
    context_chars = context_chars,
    split_chars = split_chars,
    target_pattern = target_pattern,
    suffix_pattern = suffix_pattern,
    prefix_pattern = prefix_pattern,
    infix_patterns = infix_patterns,
    uid = uid
  )
  
  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_tokenizer.tbl_spark <- function(x, input_cols, output_col,
                           exceptions = NULL, exceptions_path = NULL, case_sensitive_exceptions = NULL, context_chars = NULL,
                           split_chars = NULL, target_pattern = NULL, suffix_pattern = NULL, prefix_pattern = NULL, 
                           infix_patterns = NULL,
                           uid = random_string("tokenizer_")) {
  stage <- nlp_tokenizer.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    exceptions = exceptions,
    #exceptions_path = exceptions_path,
    case_sensitive_exceptions = case_sensitive_exceptions,
    context_chars = context_chars,
    split_chars = split_chars,
    target_pattern = target_pattern,
    suffix_pattern = suffix_pattern,
    prefix_pattern = prefix_pattern,
    infix_patterns = infix_patterns,
    uid = uid
  )
  
  stage %>% 
    sparklyr::ml_fit_and_transform(x)
}

#' @import forge
validator_nlp_tokenizer <- function(args) {
  # Input checking, much of these can be factored out; can be composed
  #   with other input checkers to avoid redundancy
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["exceptions"]] <- cast_nullable_string_list(args[["exceptions"]])
  args[["exceptions_path"]] <- cast_nullable_string(args[["exceptions_path"]])
  args[["case_sensitive_exceptions"]] <- cast_nullable_logical(args[["case_sensitive_exceptions"]])
  args[["context_chars"]] <- cast_nullable_string_list(args[["context_chars"]])
  args[["split_chars"]] <- cast_nullable_string_list(args[["split_chars"]])
  args[["target_pattern"]] <- cast_nullable_string(args[["target_pattern"]])
  args[["suffix_pattern"]] <- cast_nullable_string(args[["suffix_pattern"]])
  args[["prefix_pattern"]] <- cast_nullable_string(args[["prefix_pattern"]])
  args[["infix_patterns"]] <- cast_nullable_string_list(args[["infix_patterns"]])
  args
}

new_nlp_tokenizer <- function(jobj) {
  sparklyr::new_ml_estimator(jobj, class = "nlp_tokenizer")
}