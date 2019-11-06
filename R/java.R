#' @import forge
read_as <- function(sc, value) {
  value <- forge::cast_choice(value, c("LINE_BY_LINE", "SPARK_DATASET"))
  invoke_static(sc, "com.johnsnowlabs.nlp.util.io.ReadAs", value)
}

# As of Spark NLP 2.3.0 these functions are no longer necessary
# # Function to return default argument values for Scala constructors and static methods. Use "constructor" for the
# # method name if you want default constructor argument values
# default_argument_static <- function(sc, class_name, method_name, arg_num) {
#   module <- invoke_static(sc, paste0(class_name, "$"), "MODULE$")
#   
#   if (method_name == "constructor") {
#     method_name = "apply"
#   }
#   
#   default_name <- paste0(method_name, "$default$", arg_num)
#   invoke(module, default_name)
# }
# 
# # Function to return default argument values for Scala instance methods
# default_argument <- function(x, method_name, arg_num) {
#   default_name <- paste0(method_name, "$default$", arg_num)
#   invoke(x, default_name) 
# }
