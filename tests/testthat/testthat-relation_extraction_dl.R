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
  
  re_chunk_filterer <- nlp_re_ner_chunks_filter(sc, input_cols = c("ner_chunks", "dependencies"),
                                                output_col = "re_ner_chunks",
                                                max_syntactic_distance = 4,
                                                relation_pairs = c("internal_organ_or_component-direction"))
  
  pipeline <- ml_pipeline(assembler, sentdetect, tokenizer, word_embeddings, pos_tagger,
                          dependency_parser, ner_tagger, ner_chunker, re_chunk_filterer)

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

test_that("relation_extraction_dl param setting", {
  test_args <- list(
    input_cols = c("string1", "string2"),
    output_col = "string1",
    category_names = c("string1", "string2"),
    max_sentence_length = 100,
    prediction_threshold = 0.75
  )

  test_param_setting(sc, nlp_relation_extraction_dl, test_args)
})

test_that("nlp_relation_extraction_dl spark_connection", {
  test_annotator <- nlp_relation_extraction_dl(sc, input_cols = c("re_ner_chunks","sentence"), output_col = "relations",
                                               prediction_threshold = 0.25)
  transformed_data <- ml_transform(test_annotator, test_data)
  expect_true("relations" %in% colnames(transformed_data))
  expect_true(inherits(test_annotator, "nlp_relation_extraction_dl"))
})

test_that("nlp_relation_extraction_dl ml_pipeline", {
  test_annotator <- nlp_relation_extraction_dl(pipeline, input_cols = c("re_ner_chunks","sentence"), output_col = "relations",
                                               prediction_threshold = 0.25)
  transformed_data <- ml_fit_and_transform(test_annotator, text_tbl)
  expect_true("relations" %in% colnames(transformed_data))
})

test_that("nlp_relation_extraction_dl tbl_spark", {
  transformed_data <- nlp_relation_extraction_dl(test_data, input_cols = c("re_ner_chunks","sentence"), output_col = "relations",
                                                 prediction_threshold = 0.25)
  expect_true("relations" %in% colnames(transformed_data))
})

test_that("nlp_relation_extraction_dl pretrained", {
  model <- nlp_relation_extraction_dl_pretrained(sc, input_cols = c("re_ner_chunks", "sentence"), output_col = "relations",
                                                 name = "redl_bodypart_direction_biobert", remote_loc = "clinical/models",
                                                 prediction_threshold = 0.75)
  transformed_data <- ml_transform(model, test_data)
  expect_true("relations" %in% colnames(transformed_data))
  
  expect_true(inherits(model, "nlp_relation_extraction_dl"))
})
