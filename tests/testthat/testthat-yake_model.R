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

# test_that("yake_model param setting", {
#   test_args <- list(
#     input_cols = c("string1"),
#     output_col = "string1",
#     min_ngrams = 1,
#     max_ngrams = 3,
#     n_keywords = 20,
#     stop_words = c("string1", "string2"),
#     #threshold = 0.5, --- no getter
#     #window_size = 5 ---- no getter
#   )
# 
#   test_param_setting(sc, nlp_yake_model, test_args)
# })

test_that("nlp_yake_model spark_connection", {
  test_annotator <- nlp_yake_model(sc, input_cols = c("token"), output_col = "keywords",
                                   min_ngrams = 1, max_ngrams = 3, n_keywords = 20,
                                   window_size = 5, threshold = 0.5)
  
  yake_pipeline <- ml_add_stage(pipeline, test_annotator)
  transformed_data <- ml_fit_and_transform(yake_pipeline, test_data)
  expect_true("keywords" %in% colnames(transformed_data))

  expect_true(inherits(test_annotator, "nlp_yake_model"))
})

test_that("nlp_yake_model ml_pipeline", {
  test_annotator <- nlp_yake_model(pipeline, input_cols = c("token"), output_col = "keywords")
  transformed_data <- ml_fit_and_transform(test_annotator, test_data)
  expect_true("keywords" %in% colnames(transformed_data))
})

test_that("nlp_yake_model tbl_spark", {
  transformed_data <- nlp_yake_model(test_data, input_cols = c("token"), output_col = "keywords")
  expect_true("keywords" %in% colnames(transformed_data))
})

