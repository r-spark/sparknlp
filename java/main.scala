package sparknlp

import com.johnsnowlabs.nlp.annotators.ner.dl.NerDLApproach

object Utils {
  def setLrParam(nerDLApproach: NerDLApproach, lr: Double) : NerDLApproach = {
    nerDLApproach.setLr(lr.toFloat)
  }
  
  def setPoParam(nerDLApproach: NerDLApproach, po: Double) : NerDLApproach = {
    nerDLApproach.setPo(po.toFloat)
  }
  
  def setDropoutParam(nerDLApproach: NerDLApproach, dropout: Double) : NerDLApproach = {
    nerDLApproach.setDropout(dropout.toFloat)  
  }
}