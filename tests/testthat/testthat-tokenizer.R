setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "document")
  sentdetect <- nlp_sentence_detector(sc, input_cols = c("document"), output_col = "sentence")
  pipeline <- ml_pipeline(assembler, sentdetect)
  sentence_data <- ml_fit_and_transform(pipeline, text_tbl)
  
  assign("sc", sc, envir = parent.frame())
  assign("pipeline", pipeline, envir = parent.frame())
  assign("sentence_data", sentence_data, envir = parent.frame())
})

teardown({
  rm(sc, envir = .GlobalEnv)
  rm(pipeline, envir = .GlobalEnv)
  rm(sentence_data, envir = .GlobalEnv)
})

test_that("nlp_tokenizer() param setting", {
  test_args <- list(
    input_cols = c("sentences"), 
    output_col = "token",
    exceptions = c("new york", "bunny boots"),
    #exceptions_path = "/exceptions.txt",
    case_sensitive_exceptions = TRUE,
    context_chars = c("(", ")"),
    split_chars = c("-", "/"),
    target_pattern = "\\s+",
    suffix_pattern = "([a-z])\\z",
    prefix_pattern = "\\A([0-9])",
    infix_patterns = c("julius")
    )
  test_param_setting(sc, nlp_tokenizer, test_args)
})

test_that("nlp_tokenizer() spark_connection", {
  tokenizer <- nlp_tokenizer(sc, input_cols = c("sentence"), output_col = "token")
  fit_model <- ml_fit(tokenizer, sentence_data)
  transformed_data <- ml_transform(fit_model, sentence_data)
  expect_true("token" %in% colnames(transformed_data))
})

test_that("nlp_tokenizer() ml_pipeline", {
  tokenizer <- nlp_tokenizer(pipeline, input_cols = c("sentence"), output_col = "token")
  transformed_data <- ml_fit_and_transform(tokenizer, sentence_data)
  expect_true("token" %in% colnames(transformed_data))
})


test_that("nlp_tokenizer() tbl_spark", {
  fit_model <- nlp_tokenizer(sentence_data, input_cols = c("sentence"), output_col = "token")
  transformed_data <- ml_transform(fit_model, sentence_data)
  expect_true("token" %in% colnames(transformed_data))
})
