setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")

  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "document")

  pipeline <- ml_pipeline(assembler)
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

# test_that("regex_matcher param setting", {
#   test_args <- list(
#     input_cols = c("string1"),
#     output_col = "string1",
#     strategy = "MATCH_ALL",
#     rules_path = "string2",
#     rules_path_delimiter = ","
#   )
# 
#   test_param_setting(sc, nlp_regex_matcher, test_args)
# })

test_that("nlp_regex_matcher spark_connection", {
  test_annotator <- nlp_regex_matcher(sc, input_cols = c("document"), output_col = "regex", 
                                      rules_path = here::here("tests", "testthat", "data", "regex_match.txt"),
                                      rules_path_delimiter = ",")
  fit_model <- ml_fit(test_annotator, test_data)
  transformed_data <- ml_transform(fit_model, test_data)
  expect_true("regex" %in% colnames(transformed_data))
  
  expect_true(inherits(test_annotator, "nlp_regex_matcher"))
  expect_true(inherits(fit_model, "nlp_regex_matcher_model"))
})

test_that("nlp_regex_matcher ml_pipeline", {
  test_annotator <- nlp_regex_matcher(pipeline, input_cols = c("document"), output_col = "regex", 
                                      rules_path = here::here("tests", "testthat", "data", "regex_match.txt"),
                                      rules_path_delimiter = ",")
  transformed_data <- ml_fit_and_transform(test_annotator, test_data)
  expect_true("regex" %in% colnames(transformed_data))
})

test_that("nlp_regex_matcher tbl_spark", {
  transformed_data <- nlp_regex_matcher(test_data, input_cols = c("document"), output_col = "regex",
                                        rules_path = here::here("tests", "testthat", "data", "regex_match.txt"),
                                        rules_path_delimiter = ",")
  expect_true("regex" %in% colnames(transformed_data))
})

