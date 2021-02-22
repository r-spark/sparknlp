setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")

  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "document")
  sentdetect <- nlp_sentence_detector(sc, input_cols = c("document"), output_col = "sentence")
  tokenizer <- nlp_tokenizer(sc, input_cols = c("sentence"), output_col = "token")
  embeddings <- nlp_word_embeddings_pretrained(sc, name = "embeddings_clinical", input_cols = c("sentence", "token"),
                                               output_col = "embeddings", remote_loc = "clinical/models")
  ner_model <- nlp_ner_dl_pretrained(sc, name = "ner_radiology", input_cols = c("sentence", "token", "embeddings"),
                                     output_col = "ner", lang = "en", remote_loc = "clinical/models")

  pipeline <- ml_pipeline(assembler, sentdetect, tokenizer, embeddings, ner_model)
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

test_that("ner_chunker param setting", {
  test_args <- list(
    input_cols = c("string1", "string2"),
    output_col = "string1",
    regex_parsers = c("string1", "string2")
  )

  test_param_setting(sc, nlp_ner_chunker, test_args)
})

test_that("nlp_ner_chunker spark_connection", {
  test_annotator <- nlp_ner_chunker(sc, input_cols = c("sentence","ner"), output_col = "ner_chunk")
  transformed_data <- ml_transform(test_annotator, test_data)
  expect_true("ner_chunk" %in% colnames(transformed_data))
  expect_true(inherits(test_annotator, "nlp_ner_chunker"))
})

test_that("nlp_ner_chunker ml_pipeline", {
  test_annotator <- nlp_ner_chunker(pipeline, input_cols = c("sentence","ner"), output_col = "ner_chunk")
  transformed_data <- ml_fit_and_transform(test_annotator, test_data)
  expect_true("ner_chunk" %in% colnames(transformed_data))
})

test_that("nlp_ner_chunker tbl_spark", {
  transformed_data <- nlp_ner_chunker(test_data, input_cols = c("sentence","ner"), output_col = "ner_chunk")
  expect_true("ner_chunk" %in% colnames(transformed_data))
})

