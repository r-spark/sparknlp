setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")

  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "document")
  sentdetect <- nlp_sentence_detector(sc, input_cols = c("document"), output_col = "sentence")
  tokenizer <- nlp_tokenizer(sc, input_cols = c("sentence"), output_col = "token")
  embeddings <- nlp_word_embeddings_pretrained(sc, output_col = "embeddings")
  ner <- nlp_ner_dl_pretrained(sc, input_cols = c("sentence", "token", "embeddings"), output_col = "ner")

  pipeline <- ml_pipeline(assembler, sentdetect, tokenizer, embeddings, ner)
  test_data <- ml_fit_and_transform(pipeline, text_tbl)

  assign("sc", sc, envir = parent.frame())
  assign("pipeline", pipeline, envir = parent.frame())
  assign("text_tbl", text_tbl, envir = parent.frame())
  assign("test_data", test_data, envir = parent.frame())
})

teardown({
  spark_disconnect(sc)
  rm(sc, envir = .GlobalEnv)
  rm(pipeline, envir = .GlobalEnv)
  rm(text_tbl, envir = .GlobalEnv)
  rm(test_data, envir = .GlobalEnv)
})

test_that("ner_converter param setting", {
  test_args <- list(
    input_cols = c("string1", "string2", "string3"),
    output_col = "string1",
    #white_list = c("string1", "string2"), # no getter
    #preserve_position = TRUE # no getter
    lazy_annotator = FALSE
  )

  test_param_setting(sc, nlp_ner_converter, test_args)
})

test_that("nlp_ner_converter spark_connection", {
  test_annotator <- nlp_ner_converter(sc, input_cols = c("document","token","ner"), output_col = "ner_span")
  transformed_data <- ml_transform(test_annotator, test_data)
  expect_true("ner_span" %in% colnames(transformed_data))
})

test_that("nlp_ner_converter ml_pipeline", {
  test_annotator <- nlp_ner_converter(pipeline, input_cols = c("document","token","ner"), output_col = "ner_span")
  transformed_data <- ml_fit_and_transform(test_annotator, text_tbl)
  expect_true("ner_span" %in% colnames(transformed_data))
})

test_that("nlp_ner_converter tbl_spark", {
  transformed_data <- nlp_ner_converter(test_data, input_cols = c("document","token","ner"), output_col = "ner_span")
  expect_true("ner_span" %in% colnames(transformed_data))
})

