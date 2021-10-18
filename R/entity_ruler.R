#' Spark NLP EntityRulerApproach
#'
#' Spark ML estimator that 
#' See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#entityruler}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param case_sensitive Whether to ignore case in index lookups (Default depends on model)
#' @param enable_pattern_regex Enables regex pattern match (Default: false).
#' @param patterns_resource_path Resource in JSON or CSV format to map entities to patterns (Default: null).
#' @param patterns_resource_read_as TEXT or SPARK_DATASET
#' @param patterns_resource_options options passed to the reader. (Default: list("format" = "JSON"))
#' @param storage_path Path to the external resource.
#' @param storage_ref Unique identifier for storage (Default: this.uid)
#' @param use_storage Whether to use RocksDB storage to serialize patterns (Default: true).
#' 
#' @export
nlp_entity_ruler <- function(x, input_cols, output_col,
                 case_sensitive = NULL, enable_pattern_regex = NULL, patterns_resource_path = NULL,
                 patterns_resource_read_as = NULL, patterns_resource_options = NULL,
                 storage_path = NULL, storage_ref = NULL, use_storage = NULL,
                 uid = random_string("entity_ruler_")) {
  UseMethod("nlp_entity_ruler")
}

#' @export
nlp_entity_ruler.spark_connection <- function(x, input_cols, output_col,
                 case_sensitive = NULL, enable_pattern_regex = NULL, patterns_resource_path = NULL,
                 patterns_resource_read_as = NULL, patterns_resource_options = NULL,
                 storage_path = NULL, storage_ref = NULL, use_storage = NULL,
                 uid = random_string("entity_ruler_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    case_sensitive = case_sensitive,
    enable_pattern_regex = enable_pattern_regex,
    patterns_resource_path = patterns_resource_path,
    patterns_resource_read_as = patterns_resource_read_as,
    patterns_resource_options = patterns_resource_options,
    storage_path = storage_path,
    storage_ref = storage_ref,
    use_storage = use_storage,
    uid = uid
  ) %>%
  validator_nlp_entity_ruler()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.er.EntityRulerApproach",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setCaseSensitive", args[["case_sensitive"]])  %>%
    sparklyr::jobj_set_param("setEnablePatternRegex", args[["enable_pattern_regex"]])  %>%
    sparklyr::jobj_set_param("setStoragePath", args[["storage_path"]])  %>%
    sparklyr::jobj_set_param("setStorageRef", args[["storage_ref"]])  %>%
    sparklyr::jobj_set_param("setUseStorage", args[["use_storage"]]) 
  
  if (!is.null(patterns_resource_path)) {
    sparklyr::invoke(jobj, "setPatternsResource", args[["patterns_resource_path"]], 
                     read_as(x, args[["patterns_resource_read_as"]]), list2env(args[["patterns_resource_options"]]))
  }

  new_nlp_entity_ruler(jobj)
}

#' @export
nlp_entity_ruler.ml_pipeline <- function(x, input_cols, output_col,
                 case_sensitive = NULL, enable_pattern_regex = NULL, patterns_resource_path = NULL,
                 patterns_resource_read_as = NULL, patterns_resource_options = NULL,
                 storage_path = NULL, storage_ref = NULL, use_storage = NULL,
                 uid = random_string("entity_ruler_")) {

  stage <- nlp_entity_ruler.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    case_sensitive = case_sensitive,
    enable_pattern_regex = enable_pattern_regex,
    patterns_resource_path = patterns_resource_path,
    patterns_resource_read_as = patterns_resource_read_as,
    patterns_resource_options = patterns_resource_options,
    storage_path = storage_path,
    storage_ref = storage_ref,
    use_storage = use_storage,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_entity_ruler.tbl_spark <- function(x, input_cols, output_col,
                 case_sensitive = NULL, enable_pattern_regex = NULL, patterns_resource_path = NULL,
                 patterns_resource_read_as = NULL, patterns_resource_options = NULL,
                 storage_path = NULL, storage_ref = NULL, use_storage = NULL,
                 uid = random_string("entity_ruler_")) {
  stage <- nlp_entity_ruler.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    case_sensitive = case_sensitive,
    enable_pattern_regex = enable_pattern_regex,
    patterns_resource_path = patterns_resource_path,
    patterns_resource_read_as = patterns_resource_read_as,
    patterns_resource_options = patterns_resource_options,
    storage_path = storage_path,
    storage_ref = storage_ref,
    use_storage = use_storage,
    uid = uid
  )

  stage %>% sparklyr::ml_fit_and_transform(x)
}
#' @import forge
validator_nlp_entity_ruler <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["case_sensitive"]] <- cast_nullable_logical(args[["case_sensitive"]])
  args[["enable_pattern_regex"]] <- cast_nullable_logical(args[["enable_pattern_regex"]])
  args[["patterns_resource_path"]] <- cast_nullable_string(args[["patterns_resource_path"]])
  args[["patterns_resource_read_as"]] <- cast_choice(args[["patterns_resource_read_as"]], choices = c("SPARK", "TEXT"))
  args[["storage_path"]] <- cast_nullable_string(args[["storage_path"]])
  args[["storage_ref"]] <- cast_nullable_string(args[["storage_ref"]])
  args[["use_storage"]] <- cast_nullable_logical(args[["use_storage"]])
  args
}

nlp_float_params.nlp_entity_ruler <- function(x) {
  return(c())
}
new_nlp_entity_ruler <- function(jobj) {
  sparklyr::new_ml_estimator(jobj, class = "nlp_entity_ruler")
}
new_nlp_entity_ruler_model <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_entity_ruler_model")
}
nlp_float_params.nlp_entity_ruler_model <- function(x) {
  return(c())
}
