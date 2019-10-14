# Get a pretrained model.
# The model_class is the Scala class for the model.
pretrained_model <- function(sc, model_class, name = NULL, lang = NULL, remote_loc = NULL) {
  module <- invoke_static(sc, paste0(model_class, "$"), "MODULE$")
  default_name <- invoke(module, "pretrained$default$1")
  default_lang <- invoke(module, "pretrained$default$2")
  default_remote_loc <- invoke(module, "pretrained$default$3")
  
  if (is.null(name)) name = default_name
  if (is.null(lang)) lang = default_lang
  if (is.null(remote_loc)) remote_loc = default_remote_loc
  
  invoke_static(sc, model_class, "pretrained", name, lang, remote_loc)
}