setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")

  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "document")
  sentence <- nlp_sentence_detector(sc, input_cols = c("document"), output_col = "sentence")
  tokenizer <- nlp_tokenizer(sc, input_cols = c("sentence"), output_col = "token")
  word_embeddings <- nlp_bert_embeddings_pretrained(sc, input_cols = c("sentence", "token"), output_col = "word_embeddings",
                                                    name = "biobert_pubmed_base_cased")
  ner_model <- nlp_medical_ner_pretrained(sc, input_cols = c("sentence", "token", "word_embeddings"), output_col = "ner",
                               name = "ner_clinical_biobert", remote_loc = "clinical/models")
  ner_converter <- nlp_ner_converter(sc, input_cols = c("sentence", "token", "ner"), output_col = "ner_chunk")

  pipeline <- ml_pipeline(assembler, sentence, tokenizer, word_embeddings, ner_model, ner_converter)
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

test_that("nlp_bert_sentence_embeddings pretrained", {
  model <- nlp_bert_sentence_chunk_embeddings_pretrained(sc, input_cols = c("sentence", "ner_chunk"), output_col = "bert_sentence_chunk_embeddings")
  transformed_data <- ml_transform(model, test_data)
  expect_true("bert_sentence_chunk_embeddings" %in% colnames(transformed_data))
  
  expect_true(inherits(model, "nlp_bert_sentence_chunk_embeddings"))
})

