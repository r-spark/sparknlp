#' Spark NLP MultiDateMatcher
#'
#' Spark ML transformer that reads from different forms of date and time expressions and converts them to a provided 
#' date format. E
#' See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#multidatematcher}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param format Output format of parsed date. Defaults to yyyy/MM/dd
#' @param anchor_date_day Add an anchor day for the relative dates such as a day after tomorrow (Default: -1).
#' By default it will use the current day. The first day of the month has value 1.
#' @param anchor_date_month Add an anchor month for the relative dates such as a day after tomorrow (Default: -1). 
#' By default it will use the current month. Month values start from 1, so 1 stands for January.
#' @param anchor_date_year Add an anchor year for the relative dates such as a day after tomorrow (Default: -1). 
#' If it is not set, the by default it will use the current year. Example: 2021
#' @param default_day_when_missing Which day to set when it is missing from parsed input (Default: 1)
#' @param read_month_first Whether to interpret dates as MM/DD/YYYY instead of DD/MM/YYYY (Default: true)
#' 
#' @export
nlp_multi_date_matcher <- function(x, input_cols, output_col,
                             anchor_date_day = NULL, anchor_date_month = NULL, anchor_date_year = NULL,
                             default_day_when_missing = NULL, read_month_first = NULL, format = NULL,
                             uid = random_string("multi_date_matcher_")) {
  UseMethod("nlp_multi_date_matcher")
}

#' @export
nlp_multi_date_matcher.spark_connection <- function(x, input_cols, output_col,
                                              anchor_date_day = NULL, anchor_date_month = NULL, anchor_date_year = NULL,
                                              default_day_when_missing = NULL, read_month_first = NULL, format = NULL,
                                              uid = random_string("multi_date_matcher_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    anchor_date_day = anchor_date_day,
    anchor_date_month = anchor_date_month,
    anchor_date_year = anchor_date_year,
    default_day_when_missing = default_day_when_missing,
    read_month_first = read_month_first,
    format = format,
    uid = uid
  ) %>%
  validator_nlp_date_matcher()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.MultiDateMatcher",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setFormat", args[["format"]]) %>% 
    sparklyr::jobj_set_param("setAnchorDateDay", args[["anchor_date_day"]]) %>% 
    sparklyr::jobj_set_param("setAnchorDateMonth", args[["anchor_date_month"]]) %>% 
    sparklyr::jobj_set_param("setAnchorDateYear", args[["anchor_date_year"]]) %>% 
    sparklyr::jobj_set_param("setDefaultDayWhenMissing", args[["default_day_when_missing"]]) %>% 
    sparklyr::jobj_set_param("setReadMonthFirst", args[["read_month_first"]])
    
  new_nlp_date_matcher(jobj)
}

#' @export
nlp_multi_date_matcher.ml_pipeline <- function(x, input_cols, output_col,
                                         anchor_date_day = NULL, anchor_date_month = NULL, anchor_date_year = NULL,
                                         default_day_when_missing = NULL, read_month_first = NULL, format = NULL,
                                         uid = random_string("multi_date_matcher_")) {

  stage <- nlp_multi_date_matcher.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    anchor_date_day = anchor_date_day,
    anchor_date_month = anchor_date_month,
    anchor_date_year = anchor_date_year,
    default_day_when_missing = default_day_when_missing,
    read_month_first = read_month_first,
    format = format,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_multi_date_matcher.tbl_spark <- function(x, input_cols, output_col,
                                       anchor_date_day = NULL, anchor_date_month = NULL, anchor_date_year = NULL,
                                       default_day_when_missing = NULL, read_month_first = NULL, format = NULL,
                                       uid = random_string("multi_date_matcher_")) {
  stage <- nlp_multi_date_matcher.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    anchor_date_day = anchor_date_day,
    anchor_date_month = anchor_date_month,
    anchor_date_year = anchor_date_year,
    default_day_when_missing = default_day_when_missing,
    read_month_first = read_month_first,
    format = format,
    uid = uid
  )

  stage %>% sparklyr::ml_transform(x)
}
#' @import forge
validator_nlp_multi_date_matcher <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["anchor_date_day"]] <- cast_nullable_integer(args[["anchor_date_day"]])
  args[["anchor_date_month"]] <- cast_nullable_integer(args[["anchor_date_month"]])
  args[["anchor_date_year"]] <- cast_nullable_integer(args[["anchor_date_year"]])
  args[["default_day_when_missing"]] <- cast_nullable_integer(args[["default_day_when_missing"]])
  args[["read_month_first"]] <- cast_nullable_logical(args[["read_month_first"]])
  args[["format"]] <- cast_nullable_string(args[["format"]])
  args
}

new_nlp_multi_date_matcher <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_multi_date_matcher")
}
