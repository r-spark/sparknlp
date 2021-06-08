setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")

  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "document")
  sentdetect <- nlp_sentence_detector(sc, input_cols = c("document"), output_col = "sentence")
  tokenizer <- nlp_tokenizer(sc, input_cols = c("sentence"), output_col = "tokens")
  word_embeddings <- nlp_word_embeddings_pretrained(sc, input_cols = c("sentence", "tokens"), 
                                                    output_col = "embeddings", name = "embeddings_clinical",
                                                    remote_loc = "clinical/models")
  pos_tagger <- nlp_perceptron_pretrained(sc, input_cols = c("sentence", "tokens"), output_col = "pos_tags",
                                          name = "pos_clinical", remote_loc = "clinical/models")
  dependency_parser <- nlp_dependency_parser_pretrained(sc, input_cols = c("sentence", "pos_tags", "tokens"),
                                                        output_col = "dependencies",
                                                        name = "dependency_conllu")
  ner_tagger <- nlp_medical_ner_pretrained(sc, input_cols = c("sentence", "tokens", "embeddings"),
                                           output_col = "ner_tags", name = "jsl_ner_wip_greedy_clinical",
                                           remote_loc = "clinical/models")
  ner_chunker <- nlp_ner_converter(sc, input_cols = c("sentence", "tokens", "ner_tags"),
                                   output_col = "ner_chunks")
  
  pipeline <- ml_pipeline(assembler, sentdetect, tokenizer, word_embeddings, pos_tagger,
                          dependency_parser, ner_tagger, ner_chunker)
  
  test_data <- ml_fit_and_transform(pipeline, text_tbl)

  assign("sc", sc, envir = parent.frame())
  assign("pipeline", pipeline, envir = parent.frame())
  assign("test_data", test_data, envir = parent.frame())
  assign("text_tbl", text_tbl, envir = parent.frame())
})

teardown({
  rm(sc, envir = .GlobalEnv)
  rm(pipeline, envir = .GlobalEnv)
  rm(test_data, envir = .GlobalEnv)
  rm(text_tbl, envir = .GlobalEnv)
})

test_that("re_ner_chunks_filter param setting", {
  test_args <- list(
    input_cols = c("string1", "string2"),
    output_col = "string1",
    max_syntactic_distance = 4,
    relation_pairs = c("string1-string2")
  )

  test_param_setting(sc, nlp_re_ner_chunks_filter, test_args)
})

test_that("nlp_re_ner_chunks_filter spark_connection", {
  relpairs <- c("direction-external_body_part_or_region",
                "external_body_part_or_region-direction",
                "direction-internal_organ_or_component",
                "internal_organ_or_component-direction")
  
  test_annotator <- nlp_re_ner_chunks_filter(sc, input_cols = c("ner_chunks","dependencies"), output_col = "re_ner_chunks",
                                             relation_pairs = relpairs, max_syntactic_distance = 4)
  transformed_data <- ml_transform(test_annotator, test_data)
  expect_true("re_ner_chunks" %in% colnames(transformed_data))
  expect_true(inherits(test_annotator, "nlp_re_ner_chunks_filter"))
})

test_that("nlp_re_ner_chunks_filter ml_pipeline", {
  relpairs <- c("direction-external_body_part_or_region",
                "external_body_part_or_region-direction",
                "direction-internal_organ_or_component",
                "internal_organ_or_component-direction")
  
  test_annotator <- nlp_re_ner_chunks_filter(pipeline, input_cols = c("ner_chunks","dependencies"), output_col = "re_ner_chunks",
                                             relation_pairs = relpairs, max_syntactic_distance = 4)
  fit_data <- ml_fit(test_annotator, test_data)
  
  transformed_data <- ml_fit_and_transform(test_annotator, text_tbl)
  expect_true("re_ner_chunks" %in% colnames(transformed_data))
})

test_that("nlp_re_ner_chunks_filter tbl_spark", {
  relpairs <- c("direction-external_body_part_or_region",
                "external_body_part_or_region-direction",
                "direction-internal_organ_or_component",
                "internal_organ_or_component-direction")
  
  transformed_data <- nlp_re_ner_chunks_filter(test_data, input_cols = c("ner_chunks","dependencies"), output_col = "re_ner_chunks", 
                                               relation_pairs = relpairs, max_syntactic_distance = 4)
  expect_true("re_ner_chunks" %in% colnames(transformed_data))
})

