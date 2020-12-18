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

# test_that("word_embeddings param setting", {
#   test_args <- list(
#     input_cols = c("string1", "string2"),
#     output_col = "string1",
#     storage_path = "/tmp/embeddings",
#     storage_path_format = "TEXT",
#     dimension = 300,
#     storage_ref = "string1",
#     lazy_annotator = FALSE,
#     read_cache_size = 1000,
#     write_buffer_size = 1000,
#     include_storage = FALSE,
#     case_sensitive  = TRUE
#   )
# 
#   test_param_setting(sc, nlp_word_embeddings, test_args)
# })

test_that("nlp_word_embeddings spark_connection", {
  test_annotator <- nlp_word_embeddings(sc, input_cols = c("document", "token"), output_col = "word_embeddings",
                                        storage_path = here::here("tests", "testthat", "data", "random_embeddings_dim4.txt"),
                                        storage_path_format = "TEXT", dimension = 4)
  fit_model <- ml_fit(test_annotator, test_data)
  transformed_data <- ml_transform(fit_model, test_data)
  expect_true("word_embeddings" %in% colnames(transformed_data))
  
  expect_true(inherits(test_annotator, "nlp_word_embeddings"))
  expect_true(inherits(fit_model, "nlp_word_embeddings_model"))
})

test_that("nlp_word_embeddings ml_pipeline", {
  test_annotator <- nlp_word_embeddings(pipeline, input_cols = c("document", "token"), output_col = "word_embeddings",
                                        storage_path = here::here("tests", "testthat", "data", "random_embeddings_dim4.txt"),
                                        storage_path_format = "TEXT", dimension = 4)
  transformed_data <- ml_fit_and_transform(test_annotator, test_data)
  expect_true("word_embeddings" %in% colnames(transformed_data))
})

test_that("nlp_word_embeddings tbl_spark", {
  fit_model <- nlp_word_embeddings(test_data, input_cols = c("document", "token"), output_col = "word_embeddings",
                                   storage_path = here::here("tests", "testthat", "data", "random_embeddings_dim4.txt"),
                                   storage_path_format = "TEXT", dimension = 4)
  transformed_data <- ml_transform(fit_model, test_data)
  expect_true("word_embeddings" %in% colnames(transformed_data))
})

test_that("nlp_word_embeddings pretrained model", {
  model <- nlp_word_embeddings_pretrained(sc, input_cols = c("document", "token"), output_col = "word_embeddings")
  pipeline <- ml_add_stage(pipeline, model)
  transformed_data <- ml_fit_and_transform(pipeline, test_data)
  expect_true("word_embeddings" %in% colnames(transformed_data))
  
  expect_true(inherits(model, "nlp_word_embeddings_model"))
})

test_that("nlp_word_embeddings_model", {
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "document")
  sentdetect <- nlp_sentence_detector(sc, input_cols = c("document"), output_col = "sentence")
  tokenizer <- nlp_tokenizer(sc, input_cols = c("sentence"), output_col = "token")
  embeddings_path <- here::here("tests", "testthat", "data", "random_embeddings_dim4.txt")
  #embeddings_helper <- nlp_load_embeddings(sc, path = embeddings_path, format = "TEXT", dims = 4, reference = "embeddings_ref")
  embeddings <- nlp_word_embeddings_model(sc, input_cols = c("sentence", "token"), output_col = "embeddings", 
                                          dimension = 4)
  emb_pipeline <- ml_pipeline(assembler, sentdetect, tokenizer, embeddings)
  transformed_data <- ml_fit_and_transform(emb_pipeline, test_data)
  expect_true("embeddings" %in% colnames(transformed_data))
  
  model <- ml_stage(emb_pipeline, ml_stages(emb_pipeline)[[4]])
  expect_true(inherits(model, "nlp_word_embeddings_model"))
})

