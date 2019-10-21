#' @import forge
read_as <- function(value) {
  value <- cast_choice(value, c("LINE_BY_LINE", "SPARK_DATASET"))
  invoke_static(sc, "com.johnsnowlabs.nlp.util.io.ReadAs", value)
}