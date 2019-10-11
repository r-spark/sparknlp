setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")

  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "document")
  sentdetect <- nlp_sentence_detector(sc, input_cols = c("document"), output_col = "sentence")
  tokenizer <- nlp_tokenizer(sc, input_cols = c("sentence"), output_col = "token")

  pipeline <- ml_pipeline(assembler, sentdetect, tokenizer)
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

test_that("finisher param setting", {
  test_args <- list(
    input_cols = c("token"),
    output_cols = c("finisher_token"),
    clean_annotations = TRUE,
    value_split_symbol = "#",
    annotation_split_symbol = "@",
    include_metadata = TRUE,
    output_as_array = FALSE
  )

  test_param_setting(sc, nlp_finisher, test_args)
})

test_that("nlp_finisher spark_connection", {
  test_annotator <- nlp_finisher(sc, input_cols = c("token"))
  transformed_data <- ml_transform(test_annotator, test_data)
  print(colnames(transformed_data))
  expect_true("finished_token" %in% colnames(transformed_data))
})

test_that("nlp_finisher ml_pipeline", {
  test_annotator <- nlp_finisher(sc, input_cols = c("token"))
  transformed_data <- ml_transform(test_annotator, test_data)
  expect_true("finished_token" %in% colnames(transformed_data))
})

test_that("nlp_finisher tbl_spark", {
  transformed_data <- nlp_finisher(test_data, input_cols = c("token"))
  expect_true("finished_token" %in% colnames(transformed_data))
})

