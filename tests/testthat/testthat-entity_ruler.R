setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")

  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "document")
  tokenizer <- nlp_tokenizer(sc, input_cols = c("document"), output_col = "token")

  pipeline <- ml_pipeline(assembler, tokenizer)
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

# test_that("entity_ruler param setting", {
#   test_args <- list(
#     input_cols = c("string1", "string2"),
#     output_col = "string1",
#     case_sensitive = TRUE,
#     enable_pattern_regex = FALSE,
#     #patterns_resource_path = "string1",
#     #patterns_resource_read_as = "TEXT",
#     #patterns_resource_options = list("format" = "CSV"),
#     storage_path = "string1",
#     storage_ref = "string1",
#     use_storage = TRUE
#   )
# 
#   test_param_setting(sc, nlp_entity_ruler, test_args)
# })

test_that("nlp_entity_ruler spark_connection", {
  test_annotator <- nlp_entity_ruler(sc, input_cols = c("document", "token"), output_col = "entities",
                                     patterns_resource_path = here::here("tests", "testthat", "data", "entity_ruler", "patterns.csv"),
                                     patterns_resource_read_as = "TEXT", patterns_resource_options = list("format" = "csv", "delimiter"= "\\|"))
  fit_model <- ml_fit(test_annotator, test_data)
  transformed_data <- ml_transform(fit_model, test_data)
  expect_true("entities" %in% colnames(transformed_data))
  expect_true(inherits(test_annotator, "nlp_entity_ruler"))
  expect_true(inherits(fit_model, "nlp_entity_ruler_model"))
})

test_that("nlp_entity_ruler ml_pipeline", {
  test_annotator <- nlp_entity_ruler(pipeline, input_cols = c("document", "token"), output_col = "entities",
                                     patterns_resource_path = here::here("tests", "testthat", "data", "entity_ruler", "patterns.csv"),
                                     patterns_resource_read_as = "TEXT", patterns_resource_options = list("format" = "csv", "delimiter"= "\\|"))
  transformed_data <- ml_fit_and_transform(test_annotator, test_data)
  expect_true("entities" %in% colnames(transformed_data))
})

test_that("nlp_entity_ruler tbl_spark", {
  transformed_data <- nlp_entity_ruler(test_data, input_cols = c("document", "token"), output_col = "entities",
                                       patterns_resource_path = here::here("tests", "testthat", "data", "entity_ruler", "patterns.csv"),
                                       patterns_resource_read_as = "TEXT", patterns_resource_options = list("format" = "csv", "delimiter"= "\\|"))
  expect_true("entities" %in% colnames(transformed_data))
})

