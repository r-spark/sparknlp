setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")

  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "document")
  sentdetect <- nlp_sentence_detector(sc, input_cols = c("document"), output_col = "sentence")
  tokenizer <- nlp_tokenizer(sc, input_cols = c("document"), output_col = "token")

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

# test_that("text_matcher param setting", {
#   test_args <- list(
#     input_cols = c("string1", "string2"),
#     output_col = "string1",
#     build_from_tokens = TRUE,
#     path = "/tmp/path"
#     #read_as = "LINE_BY_LINE"
#     #options = list(format = "text")
#   )
# 
#   test_param_setting(sc, nlp_text_matcher, test_args)
# })

test_that("nlp_text_matcher spark_connection", {
  test_annotator <- nlp_text_matcher(sc, input_cols = c("sentence", "token"), output_col = "entities", 
                                     path = here::here("tests", "testthat", "data", "entities.txt"))
  fit_model <- ml_fit(test_annotator, test_data)
  transformed_data <- ml_transform(fit_model, test_data)
  expect_true("entities" %in% colnames(transformed_data))
  
  expect_true(inherits(test_annotator, "nlp_text_matcher"))
  expect_true(inherits(fit_model, "nlp_text_matcher_model"))
})

test_that("nlp_text_matcher ml_pipeline", {
  test_annotator <- nlp_text_matcher(pipeline, input_cols = c("sentence", "token"), output_col = "entities",
                                     path = here::here("tests", "testthat", "data", "entities.txt"))
  transformed_data <- ml_fit_and_transform(test_annotator, test_data)
  expect_true("entities" %in% colnames(transformed_data))
})

test_that("nlp_text_matcher tbl_spark", {
  fit_model <- nlp_text_matcher(test_data, input_cols = c("sentence", "token"), output_col = "entities",
                                       path = here::here("tests", "testthat", "data", "entities.txt"))
  transformed_data <- ml_transform(fit_model, test_data)
  expect_true("entities" %in% colnames(transformed_data))
})


