package sparknlp

import com.johnsnowlabs.nlp.annotators.ner.dl.NerDLApproach
import com.johnsnowlabs.nlp.RecursivePipeline
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import org.apache.spark.ml._

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
  
  def setStoragePath(obj: com.johnsnowlabs.nlp.embeddings.WordEmbeddings, path: String, format: String): Object = {
    obj.setStoragePath(path, com.johnsnowlabs.nlp.util.io.ReadAs.withName(format))
  }
}