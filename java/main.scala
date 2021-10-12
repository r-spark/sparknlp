package sparknlp

import com.johnsnowlabs.nlp.annotators.classifier.dl.ClassifierDLApproach
import com.johnsnowlabs.nlp.annotators.ner.dl.NerDLApproach
import com.johnsnowlabs.nlp.annotators.classifier.dl.SentimentDLApproach
import com.johnsnowlabs.nlp.annotators.spell.context.ContextSpellCheckerApproach
import com.johnsnowlabs.nlp.RecursivePipeline
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import com.johnsnowlabs.nlp.LightPipeline
import com.johnsnowlabs.nlp.JavaAnnotation
import java.lang.reflect.Method
import java.lang.Class
import org.apache.spark.ml._
import collection.JavaConverters._

object Utils {
  // Generic function to call Scala functions that need a single Float parameter
  def setFloatParam(obj: Object, method: String, param: Double): Object = {
    var cls: Class[_] = obj.getClass()
    var meth: Method = cls.getMethod(method, java.lang.Float.TYPE)
    var retobj: Object = meth.invoke(obj, new java.lang.Float(param.toFloat))
    return retobj
  }
  
  // NER DL
  def setNerLrParam(nerDLApproach: NerDLApproach, lr: Double) : NerDLApproach = {
    nerDLApproach.setLr(lr.toFloat)
  }
  
  def setNerPoParam(nerDLApproach: NerDLApproach, po: Double) : NerDLApproach = {
    nerDLApproach.setPo(po.toFloat)
  }
  
  def setNerDropoutParam(nerDLApproach: NerDLApproach, dropout: Double) : NerDLApproach = {
    nerDLApproach.setDropout(dropout.toFloat)  
  }
  
  def setNerValidationSplitParam(nerDLApproach: NerDLApproach, validationSplit: Double) : NerDLApproach = {
    nerDLApproach.setValidationSplit(validationSplit.toFloat)
  }
  
  // ClassifierDL
  def setCDLLrParam(classifierDLApproach: ClassifierDLApproach, lr: Double) : ClassifierDLApproach = {
    classifierDLApproach.setLr(lr.toFloat)
  }

  def setCDLDropoutParam(classifierDLApproach: ClassifierDLApproach, dropout: Double) : ClassifierDLApproach = {
    classifierDLApproach.setDropout(dropout.toFloat)  
  }

  def setCDLValidationSplitParam(classifierDLApproach: ClassifierDLApproach, validation_split: Double) : ClassifierDLApproach = {
    classifierDLApproach.setValidationSplit(validation_split.toFloat)
  }
  
  // SentimentDL
  def setSentimentLrParam(sentimentDLApproach: SentimentDLApproach, lr: Double) : SentimentDLApproach = {
    sentimentDLApproach.setLr(lr.toFloat)
  }

  def setSentimentDropoutParam(sentimentDLApproach: SentimentDLApproach, dropout: Double) : SentimentDLApproach = {
    sentimentDLApproach.setDropout(dropout.toFloat)  
  }
  
  def setSentimentValidationSplitParam(sentimentDLApproach: SentimentDLApproach, validationSplit: Double) : SentimentDLApproach = {
    sentimentDLApproach.setValidationSplit(validationSplit.toFloat)
  }
  
  def setSentimentThreshold(sentimentDLApproach: SentimentDLApproach, threshold: Double) : SentimentDLApproach = {
    sentimentDLApproach.setThreshold(threshold.toFloat)
  }
  
  // ContextSpellChecker
  def setCSCerrorThreshold(approach: ContextSpellCheckerApproach, threshold: Double) : ContextSpellCheckerApproach = {
    approach.setErrorThreshold(threshold.toFloat)
  }
  
  def setCSCFinalLR(approach: ContextSpellCheckerApproach, lr: Double) : ContextSpellCheckerApproach = {
    approach.setFinalRate(lr.toFloat)
  }
  
  def setCSCinitialLR(approach: ContextSpellCheckerApproach, lr: Double) : ContextSpellCheckerApproach = {
    approach.setInitialRate(lr.toFloat)
  }
  
  def setCSCtradeoff(approach: ContextSpellCheckerApproach, tradeoff: Double) : ContextSpellCheckerApproach = {
    approach.setTradeoff(tradeoff.toFloat)
  }
  
  def setCSCvalidFraction(approach: ContextSpellCheckerApproach, fraction: Double) : ContextSpellCheckerApproach = {
    approach.setValidationFraction(fraction.toFloat)
  }


  def createRecursivePipelineFromStages(uid: String, stages: PipelineStage*): RecursivePipeline = {
    new RecursivePipeline(uid)
      .setStages(stages.toArray)
  }
  
  def pretrainedPipeline(downloadName: String, lang: String, source: String, parseEmbeddingsVectors: Boolean, 
                         diskLocation: String): PretrainedPipeline = {
    var diskLocationOpt = None: Option[String]
    
    if (diskLocation != null) {
     diskLocationOpt = Some(diskLocation)
    }
                           
    new PretrainedPipeline(downloadName, lang, source, parseEmbeddingsVectors, diskLocationOpt);
  }
  
  def annotateList(lp: LightPipeline, target: Array[String]): java.util.List[java.util.Map[String, java.util.List[String]]] = {
    val targetList: java.util.ArrayList[String] = new java.util.ArrayList[String](target.toList.asJava)
    lp.annotateJava(targetList)
  }
  
  def fullAnnotateList(lp: LightPipeline, target: Array[String]): java.util.List[java.util.Map[String, java.util.List[JavaAnnotation]]] = {
    val targetList: java.util.ArrayList[String] = new java.util.ArrayList[String](target.toList.asJava)
    lp.fullAnnotateJava(targetList)
  }
  
  def setStoragePath(obj: com.johnsnowlabs.nlp.embeddings.WordEmbeddings, path: String, format: String): Object = {
    obj.setStoragePath(path, com.johnsnowlabs.nlp.util.io.ReadAs.withName(format))
  }
}