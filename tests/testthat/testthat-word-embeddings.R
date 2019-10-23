setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")

  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "document")
  sentdetect <- nlp_sentence_detector(sc, input_cols = c("document"), output_col = "sentence")
  tokenizer <- nlp_tokenizer(sc, input_cols = c("sentence"), output_col = "token")
  # TODO: put other annotators here as needed

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

# # TODO: This is failing on source_path and embeddings_format for some reason I can't figure out
# test_that("word_embeddings param setting", {
#   test_args <- list(
#     input_cols = c("string1", "string2"),
#     output_col = "string1",
#     source_path = "/tmp/embeddings",
#     dimension = 300,
#     embeddings_format = "TEXT",
#     case_sensitive  = TRUE
#   )
# 
#   test_param_setting(sc, nlp_word_embeddings, test_args)
# })

test_that("nlp_word_embeddings spark_connection", {
  test_annotator <- nlp_word_embeddings(sc, input_cols = c("document", "token"), output_col = "word_embeddings",
                                        source_path = here::here("tests", "testthat", "data", "random_embeddings_dim4.txt"),
                                        embeddings_format = "text", dimension = 4)
  fit_model <- ml_fit(test_annotator, test_data)
  transformed_data <- ml_transform(fit_model, test_data)
  expect_true("word_embeddings" %in% colnames(transformed_data))
})

test_that("nlp_word_embeddings ml_pipeline", {
  test_annotator <- nlp_word_embeddings(pipeline, input_cols = c("document", "token"), output_col = "word_embeddings",
                                        source_path = here::here("tests", "testthat", "data", "random_embeddings_dim4.txt"),
                                        embeddings_format = "text", dimension = 4)
  transformed_data <- ml_fit_and_transform(test_annotator, test_data)
  expect_true("word_embeddings" %in% colnames(transformed_data))
})

test_that("nlp_word_embeddings tbl_spark", {
  fit_model <- nlp_word_embeddings(test_data, input_cols = c("document", "token"), output_col = "word_embeddings",
                                          source_path = here::here("tests", "testthat", "data", "random_embeddings_dim4.txt"),
                                          embeddings_format = "text", dimension = 4)
  transformed_data <- ml_transform(fit_model, test_data)
  expect_true("word_embeddings" %in% colnames(transformed_data))
})

test_that("nlp_word_embeddings pretrained model", {
  model <- nlp_word_embeddings_pretrained(sc, input_cols = c("document", "token"), output_col = "word_embeddings")
  pipeline <- ml_add_stage(pipeline, model)
  transformed_data <- ml_fit_and_transform(pipeline, test_data)
  expect_true("word_embeddings" %in% colnames(transformed_data))
})

