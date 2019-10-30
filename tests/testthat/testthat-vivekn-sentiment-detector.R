setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")

  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "document")
  sentdetect <- nlp_sentence_detector(sc, input_cols = c("document"), output_col = "sentence")
  tokenizer <- nlp_tokenizer(sc, input_cols = c("sentence"), output_col = "token")

  pipeline <- ml_pipeline(assembler, sentdetect, tokenizer)
  test_data <- ml_fit_and_transform(pipeline, text_tbl)
  
  sent_data <- spark_read_parquet(sc, here::here("tests", "testthat", "data", "sentiment.parquet"))
  training_data <- ml_fit_and_transform(pipeline, sent_data)

  assign("sc", sc, envir = parent.frame())
  assign("pipeline", pipeline, envir = parent.frame())
  assign("test_data", test_data, envir = parent.frame())
  assign("training_data", training_data, envir = parent.frame())
})

teardown({
  rm(sc, envir = .GlobalEnv)
  rm(pipeline, envir = .GlobalEnv)
  rm(test_data, envir = .GlobalEnv)
  rm(training_data, envir = .GlobalEnv)
})

test_that("vivekn_sentiment_detector param setting", {
  test_args <- list(
    input_cols = c("string1", "string2"),
    output_col = "string1",
    sentiment_col = "string1",
    prune_corpus = 3,
    feature_limit = 3,
    unimportant_feature_step = 0.8,
    important_feature_ratio = 0.6
  )

  test_param_setting(sc, nlp_vivekn_sentiment_detector, test_args)
})

test_that("nlp_vivekn_sentiment_detector spark_connection", {
  test_annotator <- nlp_vivekn_sentiment_detector(sc, input_cols = c("token", "sentence"), output_col = "sentiment", sentiment_col = "sentiment_label")
  fit_model <- ml_fit(test_annotator, training_data)
  transformed_data <- ml_transform(fit_model, test_data)
  expect_true("sentiment" %in% colnames(transformed_data))
})

test_that("nlp_vivekn_sentiment_detector ml_pipeline", {
  test_annotator <- nlp_vivekn_sentiment_detector(pipeline, input_cols = c("token", "sentence"), output_col = "sentiment", sentiment_col = "sentiment_label")
  fit_model <- ml_fit(test_annotator, training_data)
  transformed_data <- ml_transform(fit_model, test_data)
  expect_true("sentiment" %in% colnames(transformed_data))
})

test_that("nlp_vivekn_sentiment_detector tbl_spark", {
  fit_model <- nlp_vivekn_sentiment_detector(training_data, input_cols = c("token", "sentence"), output_col = "sentiment", sentiment_col = "sentiment_label")
  transformed_data <- ml_transform(fit_model, test_data)
  expect_true("sentiment" %in% colnames(transformed_data))
})

test_that("nlp_vivekn_sentiment pretrained", {
  model <- nlp_vivekn_sentiment_pretrained(sc, input_cols = c("token", "sentence"), output_col = "sentiment")
  transformed_data <- ml_transform(model, test_data)
  expect_true("sentiment" %in% colnames(transformed_data))
})
