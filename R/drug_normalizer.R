#' Spark NLP DrugNormalizer
#'
#' Spark ML transformer that normalizes raw text from clinical documents, e.g. 
#' scraped web pages or xml documents, from document type columns into Sentence.
#' Removes all dirty characters from text following one or more input regex 
#' patterns. Can apply non wanted character removal which a specific policy. 
#' Can apply lower case normalization.
#' See \url{https://nlp.johnsnowlabs.com/licensed/api/index.html#com.johnsnowlabs.nlp.annotators.DrugNormalizer}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param lower_case whether to convert strings to lowercase
#' @param policy removalPolicy to remove patterns from text with a given policy
#' 
#' @export
nlp_drug_normalizer <- function(x, input_cols, output_col,
                 lower_case = NULL, policy = NULL,
                 uid = random_string("drug_normalizer_")) {
  UseMethod("nlp_drug_normalizer")
}

#' @export
nlp_drug_normalizer.spark_connection <- function(x, input_cols, output_col,
                 lower_case = NULL, policy = NULL,
                 uid = random_string("drug_normalizer_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    lower_case = lower_case,
    policy = policy,
    uid = uid
  ) %>%
  validator_nlp_drug_normalizer()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.DrugNormalizer",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setLowercase", args[["lower_case"]])  %>%
    sparklyr::jobj_set_param("setPolicy", args[["policy"]]) 

  new_nlp_drug_normalizer(jobj)
}

#' @export
nlp_drug_normalizer.ml_pipeline <- function(x, input_cols, output_col,
                 lower_case = NULL, policy = NULL,
                 uid = random_string("drug_normalizer_")) {

  stage <- nlp_drug_normalizer.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    lower_case = lower_case,
    policy = policy,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_drug_normalizer.tbl_spark <- function(x, input_cols, output_col,
                 lower_case = NULL, policy = NULL,
                 uid = random_string("drug_normalizer_")) {
  stage <- nlp_drug_normalizer.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    lower_case = lower_case,
    policy = policy,
    uid = uid
  )

  stage %>% sparklyr::ml_transform(x)
}
#' @import forge
validator_nlp_drug_normalizer <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["lower_case"]] <- cast_nullable_logical(args[["lower_case"]])
  args[["policy"]] <- cast_nullable_string(args[["policy"]])
  args
}

nlp_float_params.nlp_drug_normalizer <- function(x) {
  return(c())
}
new_nlp_drug_normalizer <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_drug_normalizer")
}
