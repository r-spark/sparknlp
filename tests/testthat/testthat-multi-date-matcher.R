setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")

  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "document")
  sentdetect <- nlp_sentence_detector(sc, input_cols = c("document"), output_col = "sentence")

  pipeline <- ml_pipeline(assembler, sentdetect)
  test_data <- ml_fit_and_transform(pipeline, text_tbl)

  assign("sc", sc, envir = parent.frame())
  assign("pipeline", pipeline, envir = parent.frame())
  assign("test_data", test_data, envir = parent.frame())
})

teardown({
  spark_disconnect(sc)
  rm(sc, envir = .GlobalEnv)
  rm(pipeline, envir = .GlobalEnv)
  rm(test_data, envir = .GlobalEnv)
})

test_that("multi_date_matcher param setting", {
  test_args <- list(
    input_cols = c("string1"),
    output_col = "string1",
    anchor_date_day = 11,
    #anchor_date_month = 4, # due to a bug https://github.com/JohnSnowLabs/spark-nlp/issues/5673
    anchor_date_year = 2020,
    default_day_when_missing = 1,
    read_month_first = FALSE,
    format = "yyyy/MM/dd"
  )

  test_param_setting(sc, nlp_multi_date_matcher, test_args)
})

test_that("nlp_multi_date_matcher spark_connection", {
  test_annotator <- nlp_multi_date_matcher(sc, input_cols = c("sentence"), output_col = "date")
  transformed_data <- ml_transform(test_annotator, test_data)
  expect_true("date" %in% colnames(transformed_data))
})

test_that("nlp_multi_date_matcher ml_pipeline", {
  test_annotator <- nlp_multi_date_matcher(pipeline, input_cols = c("sentence"), output_col = "date")
  transformed_data <- ml_fit_and_transform(test_annotator, test_data)
  expect_true("date" %in% colnames(transformed_data))
})

test_that("nlp_multi_date_matcher tbl_spark", {
  transformed_data <- nlp_multi_date_matcher(test_data, input_cols = c("sentence"), output_col = "date")
  expect_true("date" %in% colnames(transformed_data))
})

