setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")

  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "document")
  date_chunks <- nlp_regex_matcher(sc, input_cols = c("document"), output_col = "chunk_date", 
                                   rules_path = here::here("tests", "testthat", "data", "regex_match.txt"),
                                   rules_path_delimiter = ",")

  pipeline <- ml_pipeline(assembler, date_chunks)
  test_data <- ml_fit_and_transform(pipeline, text_tbl)

  assign("sc", sc, envir = parent.frame())
  assign("pipeline", pipeline, envir = parent.frame())
  assign("test_data", test_data, envir = parent.frame())
})

teardown({
  rm(sc, envir = .GlobalEnv)
  rm(pipeline, envir = .GlobalEnv)
  rm(test_data, envir = .GlobalEnv)
})

test_that("date_normalizer param setting", {
# TODO: edit these to make them legal values for the parameters
  test_args <- list(
    input_cols = c("string1"),
    output_col = "string2",
    anchor_date_day = 15,
    #anchor_date_month = 5L, bug reported in https://github.com/JohnSnowLabs/spark-nlp/issues/5814
    anchor_date_year = 2021
  )

  test_param_setting(sc, nlp_date_normalizer, test_args)
})

test_that("nlp_date_normalizer spark_connection", {
  test_annotator <- nlp_date_normalizer(sc, input_cols = c("chunk_date"), output_col = "date")
  transformed_data <- ml_transform(test_annotator, test_data)
  expect_true("date" %in% colnames(transformed_data))
  expect_true(inherits(test_annotator, "nlp_date_normalizer"))
})

test_that("nlp_date_normalizer ml_pipeline", {
  test_annotator <- nlp_date_normalizer(pipeline, input_cols = c("chunk_date"), output_col = "date")
  transformed_data <- ml_fit_and_transform(test_annotator, test_data)
  expect_true("date" %in% colnames(transformed_data))
})

test_that("nlp_date_normalizer tbl_spark", {
  transformed_data <- nlp_date_normalizer(test_data, input_cols = c("chunk_date"), output_col = "date")
  expect_true("date" %in% colnames(transformed_data))
})

