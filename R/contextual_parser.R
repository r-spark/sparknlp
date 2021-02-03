#' Spark NLP ContextualParserApproach
#'
#' Spark ML estimator that provides Regex + Contextual matching based on a JSON file
#' See \url{https://nlp.johnsnowlabs.com/docs/en/licensed_annotators#contextual-parser}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param json_path path to json file with rules
#' @param dictionary path to dictionary file in tsv or csv format
#' @param read_as the format of the file, can be one of {TEXT, SPARK, BINARY}.
#' @param options an named list containing additional parameters used when reading the dictionary file
#' @param case_sensitive whether to use case sensitive when matching values
#' @param prefix_and_suffix_match whether to force both before AND after the regex match to annotate the hit
#' @param context_match whether to include prior and next context to annotate the hit
#' @param update_tokenizer Whether to update tokenizer from pipeline when detecting multiple words on dictionary values
#' 
#' @export
nlp_contextual_parser <- function(x, input_cols, output_col,
                 json_path = NULL, dictionary = NULL, read_as = "TEXT", options = NULL,
                 case_sensitive = NULL, prefix_and_suffix_match = NULL, context_match = NULL, update_tokenizer = NULL,
                 uid = random_string("contextual_parser_")) {
  UseMethod("nlp_contextual_parser")
}

#' @export
nlp_contextual_parser.spark_connection <- function(x, input_cols, output_col,
                 json_path = NULL, dictionary = NULL, read_as = "TEXT", options = NULL,
                 case_sensitive = NULL, prefix_and_suffix_match = NULL, context_match = NULL, update_tokenizer = NULL,
                 uid = random_string("contextual_parser_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    json_path = json_path,
    dictionary = dictionary,
    read_as = read_as,
    options = options,
    case_sensitive = case_sensitive,
    prefix_and_suffix_match = prefix_and_suffix_match,
    context_match = context_match,
    update_tokenizer = update_tokenizer,
    uid = uid
  ) %>%
  validator_nlp_contextual_parser()
  
  if (!is.null(args[["options"]])) {
    args[["options"]] <- list2env(args[["options"]])
  }

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.context.ContextualParserApproach",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setJsonPath", args[["json_path"]])  %>%
    #sparklyr::jobj_set_param("setDictionary", args[["dictionary"]])  %>%
    sparklyr::jobj_set_param("setCaseSensitive", args[["case_sensitive"]])  %>%
    sparklyr::jobj_set_param("setPrefixAndSuffixMatch", args[["prefix_and_suffix_match"]])  %>%
    sparklyr::jobj_set_param("setContextMatch", args[["context_match"]])  %>%
    sparklyr::jobj_set_param("setUpdateTokenizer", args[["update_tokenizer"]]) %>% 
    sparklyr::invoke("setDictionary", args[["dictionary"]], read_as(x, args[["read_as"]]), args[["options"]])

  new_nlp_contextual_parser(jobj)
}

#' @export
nlp_contextual_parser.ml_pipeline <- function(x, input_cols, output_col,
                 json_path = NULL, dictionary = NULL, read_as = "TEXT", options = NULL,
                 case_sensitive = NULL, prefix_and_suffix_match = NULL, context_match = NULL, update_tokenizer = NULL,
                 uid = random_string("contextual_parser_")) {

  stage <- nlp_contextual_parser.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    json_path = json_path,
    dictionary = dictionary,
    read_as = read_as,
    options = options,
    case_sensitive = case_sensitive,
    prefix_and_suffix_match = prefix_and_suffix_match,
    context_match = context_match,
    update_tokenizer = update_tokenizer,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_contextual_parser.tbl_spark <- function(x, input_cols, output_col,
                 json_path = NULL, dictionary = NULL, read_as = "TEXT", options = NULL,
                 case_sensitive = NULL, prefix_and_suffix_match = NULL, context_match = NULL, update_tokenizer = NULL,
                 uid = random_string("contextual_parser_")) {
  stage <- nlp_contextual_parser.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    json_path = json_path,
    dictionary = dictionary,
    read_as = read_as,
    options = options,
    case_sensitive = case_sensitive,
    prefix_and_suffix_match = prefix_and_suffix_match,
    context_match = context_match,
    update_tokenizer = update_tokenizer,
    uid = uid
  )

  stage %>% sparklyr::ml_fit_and_transform(x)
}
#' @import forge
validator_nlp_contextual_parser <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["json_path"]] <- cast_nullable_string(args[["json_path"]])
  args[["dictionary"]] <- cast_nullable_string(args[["dictionary"]])
  args[["read_as"]] <- cast_choice(args[["read_as"]], choices = c("TEXT", "SPARK", "BINARY"))
  #args[["options"]] <- cast_nullable_string_list(args[["options"]])
  args[["case_sensitive"]] <- cast_nullable_logical(args[["case_sensitive"]])
  args[["prefix_and_suffix_match"]] <- cast_nullable_logical(args[["prefix_and_suffix_match"]])
  args[["context_match"]] <- cast_nullable_logical(args[["context_match"]])
  args[["update_tokenizer"]] <- cast_nullable_logical(args[["update_tokenizer"]])
  args
}

nlp_float_params.nlp_contextual_parser <- function(x) {
  return(c())
}
new_nlp_contextual_parser <- function(jobj) {
  sparklyr::new_ml_estimator(jobj, class = "nlp_contextual_parser")
}
new_nlp_contextual_parser_model <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_contextual_parser_model")
}
nlp_float_params.nlp_contextual_parser_model <- function(x) {
  return(c())
}
