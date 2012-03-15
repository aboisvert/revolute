/*
 * Copyright (c) 2012 Bizo inc.  All Rights Reserved.
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
 *
 * This file is derived from the Cascading project's TemplateTap.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package revolute.tap

import java.io.IOException
import java.util.HashSet
import java.util.LinkedHashMap
import java.util.Map
import java.util.Set

import cascading.flow.FlowProcess
import cascading.flow.hadoop.HadoopFlowProcess
import cascading.scheme.Scheme
import cascading.scheme.SinkCall
import cascading.scheme.SourceCall
import cascading.tap.SinkMode
import cascading.tap.SinkTap
//import cascading.tap.Tap
import cascading.tap.TapException
import cascading.tap.hadoop._
import cascading.tuple.Fields
import cascading.tuple.Tuple
import cascading.tuple.TupleEntry
import cascading.tuple.TupleEntryCollector
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.RecordReader
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.collection._

import revolute.util.Compat.Tap

/*
object HiveTemplateTap {
  private final val LOG = LoggerFactory.getLogger(classOf[HiveTemplateTap])

  private final val defaultOpenTapsThreshold = 300

  class TemplateScheme(scheme: Scheme[_ <: FlowProcess[_], _, _, _, _, _], _pathFields: Fields = null)
    extends Scheme[HadoopFlowProcess, JobConf, RecordReader[_, _], OutputCollector[_, _], Object, Object]
  {
    val pathFields = {
      if (_pathFields == null || _pathFields.isAll) null
      else if (_pathFields.isDefined) _pathFields
      else throw new IllegalArgumentException("pathFields must be defined or the ALL substitution, got: " + _pathFields.printVerbose())
    }

    def getSinkFields(): Fields = {
      if (pathFields == null) return scheme.getSinkFields
      Fields.merge(scheme.getSinkFields, pathFields)
    }

    def setSinkFields(sinkFields: Fields) { scheme.setSinkFields(sinkFields) }

    def getSourceFields: Fields = { return scheme.getSourceFields }

    def setSourceFields(sourceFields: Fields) { scheme.setSourceFields(sourceFields) }

    def getNumSinkParts = scheme.getNumSinkParts

    def setNumSinkParts(numSinkParts: Int) { scheme.setNumSinkParts(numSinkParts) }

    override def sourceConfInit(flowProcess: HadoopFlowProcess, tap: Tap, conf: JobConf) {
      scheme.sourceConfInit(flowProcess, tap, conf)
    }

    override def sourcePrepare(flowProcess: HadoopFlowProcess, sourceCall: SourceCall[Object, RecordReader[_, _]]) {
      scheme.sourcePrepare(flowProcess, sourceCall)
    }

    override def source(flowProcess: HadoopFlowProcess, sourceCall: SourceCall[_, _]): Boolean = {
      throw new UnsupportedOperationException("not supported")
    }

    override def sourceCleanup(flowProcess: HadoopFlowProcess, sourceCall: SourceCall[Object, RecordReader[_, _]]) {
      scheme.sourceCleanup(flowProcess, sourceCall)
      }

    override def sinkConfInit(flowProcess: HadoopFlowProcess, tap: Tap, conf: JobConf) {
      scheme.sinkConfInit(flowProcess, tap, conf)
      }

    override def sinkPrepare(flowProcess: HadoopFlowProcess, sinkCall: SinkCall[Any, OutputCollector[_, _]]) {
      scheme.sinkPrepare(flowProcess, sinkCall)
    }

    override def sink(flowProcess: HadoopFlowProcess, sinkCall: SinkCall[Any, OutputCollector[_, _]]) {
      throw new UnsupportedOperationException("should never be called")
    }

    override def sinkCleanup(flowProcess: HadoopFlowProcess, sinkCall: SinkCall[Any, OutputCollector[_, _]]) {
      scheme.sinkCleanup(flowProcess, sinkCall)
    }
  }
}

class HiveTemplateTap(parent: Hfs, pathTemplate: String) extends SinkTap[HadoopFlowProcess, JobConf, OutputCollector[_, _]] {
  import HiveTemplateTap._

  private var keepParentOnDelete = false
  private var openTapsThreshold = defaultOpenTapsThreshold
  private val collectors = mutable.Map[String, TupleEntryCollector]()

  private class TemplateCollector(val flowProcess: HadoopFlowProcess) extends TupleEntryCollector(Fields.asDeclaration(getSinkFields)) with OutputCollector[_, _] {
    private final val conf = flowProcess.getJobConf
    private final val parentFields = parent.getSinkFields
    private final val pathFields = getScheme.asInstanceOf[TemplateScheme].pathFields

    private def getCollector(path: String): TupleEntryCollector = {
      var collector: Collector = collectors(path)
      if (collector != null) return collector

      try {
        val fullPath = new Path(parent.getFullIdentifier(conf), path)

        LOG.debug("creating collector for path: {}", fullPath)

        collector = new HadoopTapCollector(flowProcess, parent, path)

        collector.asInstanceOf[HadoopTapCollector].prepare()
      } catch { case e: IOException =>
        throw new TapException("unable to open template path: " + path, e)
      }

      if (collectors.size > openTapsThreshold) {
        purgeCollectors()
      }

      collectors.put(path, collector)

      if (LOG.isInfoEnabled && collectors.size % 100 == 0)
        LOG.info("caching {} open Taps", collectors.size)

      return collector
    }

    private def purgeCollectors() {
      val numToClose = Math.max(1, openTapsThreshold / 10)

      if (LOG.isInfoEnabled)
        LOG.info("removing {} open Taps from cache of size {}", numToClose, collectors.size)

      val removeKeys = mutable.Set[String]()
      val keys = collectors.keySet

      for (key <- keys) {
        if (numToClose-- == 0)
          break

        removeKeys.add(key)
      }

      for (removeKey <- removeKeys) {
        collectors.remove(removeKey) foreach { closeCollector(_) }
      }
    }

    override def close() {
      super.close()
      try {
        collectors.values foreach { closeCollector(_) }
      } finally {
        collectors.clear()
      }
    }

    private def closeCollector(collector: TupleEntryCollector) {
      if (collector == null) return
      try {
        collector.asInstanceOf[TupleEntryCollector].close()
      } catch { case e: Exception =>
        // do nothing
      }
    }

    protected def collect(tupleEntry: TupleEntry) {
      if (pathFields != null) {
        val pathValues = tupleEntry.selectTuple(pathFields)
        val path = pathValues.format(pathTemplate)
        getCollector(path).add(tupleEntry.selectTuple(parentFields))
      } else {
        val path = tupleEntry.getTuple.format(pathTemplate)
        getCollector(path).add(tupleEntry)
      }
    }

    def collect(key: Object, value: Object) {
      throw new UnsupportedOperationException("unimplemented")
    }
  }


  TemplateTap(Hfs parent, String pathTemplate) {
    this(parent, pathTemplate, defaultOpenTapsThreshold)
    }

  TemplateTap(Hfs parent, String pathTemplate, int openTapsThreshold) {
    super(new TemplateScheme(parent.getScheme))
    this.parent = parent
    this.pathTemplate = pathTemplate
    this.openTapsThreshold = openTapsThreshold
    }

  TemplateTap(Hfs parent, String pathTemplate, SinkMode sinkMode) {
    super(new TemplateScheme(parent.getScheme), sinkMode)
    this.parent = parent
    this.pathTemplate = pathTemplate
    }

  TemplateTap(Hfs parent, String pathTemplate, SinkMode sinkMode, boolean keepParentOnDelete) {
    this(parent, pathTemplate, sinkMode, keepParentOnDelete, defaultOpenTapsThreshold)
    }

  TemplateTap(Hfs parent, String pathTemplate, SinkMode sinkMode, boolean keepParentOnDelete, int openTapsThreshold) {
    super(new TemplateScheme(parent.getScheme), sinkMode)
    this.parent = parent
    this.pathTemplate = pathTemplate
    this.keepParentOnDelete = keepParentOnDelete
    this.openTapsThreshold = openTapsThreshold
    }

  TemplateTap(Hfs parent, String pathTemplate, Fields pathFields)
    {
    this(parent, pathTemplate, pathFields, defaultOpenTapsThreshold)
    }

  TemplateTap(Hfs parent, String pathTemplate, Fields pathFields, int openTapsThreshold)
    {
    super(new TemplateScheme(parent.getScheme, pathFields))
    this.parent = parent
    this.pathTemplate = pathTemplate
    this.openTapsThreshold = openTapsThreshold
    }

  TemplateTap(Hfs parent, String pathTemplate, Fields pathFields, SinkMode sinkMode)
    {
    super(new TemplateScheme(parent.getScheme, pathFields), sinkMode)
    this.parent = parent
    this.pathTemplate = pathTemplate
    }

  TemplateTap(Hfs parent, String pathTemplate, Fields pathFields, SinkMode sinkMode, boolean keepParentOnDelete)
    {
    this(parent, pathTemplate, pathFields, sinkMode, keepParentOnDelete, defaultOpenTapsThreshold)
    }

  TemplateTap(Hfs parent, String pathTemplate, Fields pathFields, SinkMode sinkMode, boolean keepParentOnDelete, int openTapsThreshold)
    {
    super(new TemplateScheme(parent.getScheme, pathFields), sinkMode)
    this.parent = parent
    this.pathTemplate = pathTemplate
    this.keepParentOnDelete = keepParentOnDelete
    this.openTapsThreshold = openTapsThreshold
    }

  def getParent = parent

  def getPathTemplate = pathTemplate

  def getIdentifier = parent.getIdentifier

  def getOpenTapsThreshold = openTapsThreshold

  override def openForWrite(flowProcess: HadoopFlowProcess, output: OutputCollector[_, _]): TupleEntryCollector  = {
    new TemplateCollector(flowProcess)
  }

  def createResource(conf: JobConf) = parent.createResource(conf)

  override def deleteResource(conf: JobConf): Boolean = {
    return keepParentOnDelete || parent.deleteResource(conf)
  }

  override def commitResource(conf: JobConf) = parent.commitResource(conf)
  }

  override def rollbackResource(conf: JobConf): Boolean = parent.rollbackResource(conf)

  def resourceExists(conf: JobConf): Boolean = parent.resourceExists(conf)

  override def getModifiedTime(conf: JobConf) = parent.getModifiedTime(conf)

  override def equals(other: Any): Boolean = {
    if (this == other) return true
    if (other == null || getClass() != other.getClass()) return false
    if (!super.equals(other)) return false

    other match {
      case that: TemplateTap =>
        if (if (parent != null) !parent.equals(that.parent) else that.parent != null) return false
        if (if (pathTemplate != null) !pathTemplate.equals(that.pathTemplate) else that.pathTemplate != null) return false
        return true
    }
  }

  override def hashCode = {
    var result = super.hashCode()
    result = 31 * result + (if (parent != null) parent.hashCode() else 0)
    result = 31 * result + (if (pathTemplate != null) pathTemplate.hashCode() else 0)
    result
  }

  override def toString = getClass().getSimpleName() + "[\"" + parent + "\"]" + "[\"" + pathTemplate + "\"]"
}
*/