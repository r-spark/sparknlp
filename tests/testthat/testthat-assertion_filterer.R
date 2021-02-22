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
  ner_converter <- nlp_ner_converter(sc, input_cols = c("sentence", "token", "ner"), output_col = "ner_chunk")
  clinical_assertion <- nlp_assertion_dl_pretrained(sc, name = "assertion_dl", remote_loc = "clinical/models",
                                                    input_cols = c("sentence", "ner_chunk", "embeddings"),
                                                    output_col = "assertion")

  pipeline <- ml_pipeline(assembler, sentdetect, tokenizer, embeddings, ner_model, ner_converter,
                          clinical_assertion)
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

test_that("assertion_filterer param setting", {
  test_args <- list(
    input_cols = c("string1", "string2", "string3"),
    output_col = "string1",
    criteria = "string1",
    whitelist = c("string1"),
    regex = c("string1")
  )

  test_param_setting(sc, nlp_assertion_filterer, test_args)
})

test_that("nlp_assertion_filterer spark_connection", {
  test_annotator <- nlp_assertion_filterer(sc, input_cols = c("sentence","ner_chunk","assertion"), output_col = "assertion_filtered")
  transformed_data <- ml_transform(test_annotator, test_data)
  expect_true("assertion_filtered" %in% colnames(transformed_data))
  expect_true(inherits(test_annotator, "nlp_assertion_filterer"))
})

test_that("nlp_assertion_filterer ml_pipeline", {
  test_annotator <- nlp_assertion_filterer(pipeline, input_cols = c("sentence","ner_chunk","assertion"), output_col = "assertion_filtered")
  transformed_data <- ml_fit_and_transform(test_annotator, test_data)
  expect_true("assertion_filtered" %in% colnames(transformed_data))
})

test_that("nlp_assertion_filterer tbl_spark", {
  transformed_data <- nlp_assertion_filterer(test_data, input_cols = c("sentence","ner_chunk","assertion"), output_col = "assertion_filtered")
  expect_true("assertion_filtered" %in% colnames(transformed_data))
})

