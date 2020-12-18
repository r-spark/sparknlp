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
  spark_disconnect(sc)
  rm(sc, envir = .GlobalEnv)
  rm(pipeline, envir = .GlobalEnv)
  rm(test_data, envir = .GlobalEnv)
})

test_that("sentiment_detector param setting", {
  test_args <- list(
    input_cols = c("string1", "string2"),
    output_col = "string1",
    decrement_multiplier = -2.0,
    enable_score = TRUE,
    increment_multiplier = 2.0,
    negative_multiplier = -2.0,
    positive_multiplier = 3.0,
    reverse_multiplier = -1.0
  )

  test_param_setting(sc, purrr::partial(nlp_sentiment_detector, dictionary_path = here::here("tests", "testthat", "data", "sentiment_dictionary.txt")),
                     test_args)
})

test_that("nlp_sentiment_detector spark_connection", {
  test_annotator <- nlp_sentiment_detector(sc, input_cols = c("token", "sentence"), output_col = "sentiment", 
                                           dictionary_path = here::here("tests", "testthat", "data", "sentiment_dictionary.txt"))
  fit_model <- ml_fit(test_annotator, test_data)
  transformed_data <- ml_transform(fit_model, test_data)
  expect_true("sentiment" %in% colnames(transformed_data))
})

test_that("nlp_sentiment_detector ml_pipeline", {
  test_annotator <- nlp_sentiment_detector(pipeline, input_cols = c("token", "sentence"), output_col = "sentiment", 
                                           dictionary_path = here::here("tests", "testthat", "data", "sentiment_dictionary.txt"))
  transformed_data <- ml_fit_and_transform(test_annotator, test_data)
  expect_true("sentiment" %in% colnames(transformed_data))
})

test_that("nlp_sentiment_detector tbl_spark", {
  transformed_data <- nlp_sentiment_detector(test_data, input_cols = c("token", "sentence"), output_col = "sentiment", 
                                             dictionary_path = here::here("tests", "testthat", "data", "sentiment_dictionary.txt"))
  expect_true("sentiment" %in% colnames(transformed_data))
})

