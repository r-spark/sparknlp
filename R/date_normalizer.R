#' Spark NLP DateNormalizer
#'
#' Spark ML transformer that tries to normalize dates in chunks annotations. The expected format for the 
#' date will be YYYY/MM/DD. If the date is normalized then field normalized in metadata will be true else will 
#' be false.
#' See \url{https://nlp.johnsnowlabs.com/licensed/api/com/johnsnowlabs/nlp/annotators/normalizer/DateNormalizer.html}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param anchor_date_day Add an anchor day for the relative dates such as a day after tomorrow (Default: -1). By default it will use the current day. The first day of the month has value 1
#' @param anchor_date_month Add an anchor month for the relative dates such as a day after tomorrow (Default: -1). By default it will use the current month. Month values start from 1, so 1 stands for January.
#' @param anchor_date_year Add an anchor year for the relative dates such as a day after tomorrow (Default: -1). If it is not set, the by default it will use the current year. Example: 2021
#' 
#' @export
nlp_date_normalizer <- function(x, input_cols, output_col,
                 anchor_date_day = NULL, anchor_date_month = NULL, anchor_date_year = NULL,
                 uid = random_string("date_normalizer_")) {
  UseMethod("nlp_date_normalizer")
}

#' @export
nlp_date_normalizer.spark_connection <- function(x, input_cols, output_col,
                 anchor_date_day = NULL, anchor_date_month = NULL, anchor_date_year = NULL,
                 uid = random_string("date_normalizer_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    anchor_date_day = anchor_date_day,
    anchor_date_month = anchor_date_month,
    anchor_date_year = anchor_date_year,
    uid = uid
  ) %>%
  validator_nlp_date_normalizer()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.normalizer.DateNormalizer",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setAnchorDateDay", args[["anchor_date_day"]])  %>%
    sparklyr::jobj_set_param("setAnchorDateMonth", args[["anchor_date_month"]])  %>%
    sparklyr::jobj_set_param("setAnchorDateYear", args[["anchor_date_year"]]) 

  new_nlp_date_normalizer(jobj)
}

#' @export
nlp_date_normalizer.ml_pipeline <- function(x, input_cols, output_col,
                 anchor_date_day = NULL, anchor_date_month = NULL, anchor_date_year = NULL,
                 uid = random_string("date_normalizer_")) {

  stage <- nlp_date_normalizer.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    anchor_date_day = anchor_date_day,
    anchor_date_month = anchor_date_month,
    anchor_date_year = anchor_date_year,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_date_normalizer.tbl_spark <- function(x, input_cols, output_col,
                 anchor_date_day = NULL, anchor_date_month = NULL, anchor_date_year = NULL,
                 uid = random_string("date_normalizer_")) {
  stage <- nlp_date_normalizer.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    anchor_date_day = anchor_date_day,
    anchor_date_month = anchor_date_month,
    anchor_date_year = anchor_date_year,
    uid = uid
  )

  stage %>% sparklyr::ml_transform(x)
}
#' @import forge
validator_nlp_date_normalizer <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["anchor_date_day"]] <- cast_nullable_integer(args[["anchor_date_day"]])
  args[["anchor_date_month"]] <- cast_nullable_integer(args[["anchor_date_month"]])
  args[["anchor_date_year"]] <- cast_nullable_integer(args[["anchor_date_year"]])
  args
}

nlp_float_params.nlp_date_normalizer <- function(x) {
  return(c())
}
new_nlp_date_normalizer <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_date_normalizer")
}
