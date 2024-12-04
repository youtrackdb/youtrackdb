/*
 *
 *
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *
 *
 */
package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.iterator.OLazyWrapperIterator;
import com.orientechnologies.orient.core.metadata.schema.YTImmutableClass;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.YTEdge;
import com.orientechnologies.orient.core.record.YTEntity;
import com.orientechnologies.orient.core.record.YTRecord;
import com.orientechnologies.orient.core.record.YTRecordAbstract;
import com.orientechnologies.orient.core.record.YTVertex;
import java.util.Iterator;

/**
 *
 */
public class OEdgeIterator extends OLazyWrapperIterator<YTEdge> {

  private final YTVertex sourceVertex;
  private final YTVertex targetVertex;
  private final OPair<ODirection, String> connection;
  private final String[] labels;

  public OEdgeIterator(
      final YTVertex iSourceVertex,
      final Object iMultiValue,
      final Iterator<?> iterator,
      final OPair<ODirection, String> connection,
      final String[] iLabels,
      final int iSize) {
    this(iSourceVertex, null, iMultiValue, iterator, connection, iLabels, iSize);
  }

  public OEdgeIterator(
      final YTVertex iSourceVertex,
      final YTVertex iTargetVertex,
      final Object iMultiValue,
      final Iterator<?> iterator,
      final OPair<ODirection, String> connection,
      final String[] iLabels,
      final int iSize) {
    super(iterator, iSize, iMultiValue);
    this.sourceVertex = iSourceVertex;
    this.targetVertex = iTargetVertex;
    this.connection = connection;
    this.labels = iLabels;
  }

  public YTEdge createGraphElement(final Object iObject) {
    if (iObject instanceof YTEntity && ((YTEntity) iObject).isEdge()) {
      return ((YTEntity) iObject).asEdge().get();
    }

    final YTIdentifiable rec = (YTIdentifiable) iObject;

    if (rec == null) {
      // SKIP IT
      return null;
    }

    final YTRecord record;
    try {
      record = rec.getRecord();
    } catch (ORecordNotFoundException rnf) {
      // SKIP IT
      OLogManager.instance().warn(this, "Record (%s) is null", rec);
      return null;
    }

    if (!(record instanceof YTEntity value)) {
      // SKIP IT
      OLogManager.instance()
          .warn(
              this,
              "Found a record (%s) that is not an edge. Source vertex : %s, Target vertex : %s,"
                  + " Database : %s",
              rec,
              sourceVertex != null ? sourceVertex.getIdentity() : null,
              targetVertex != null ? targetVertex.getIdentity() : null,
              ((YTRecordAbstract) record).getSession().getURL());
      return null;
    }

    final YTEdge edge;
    if (value.isVertex()) {
      // DIRECT VERTEX, CREATE DUMMY EDGE
      YTDatabaseSessionInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
      YTImmutableClass clazz = null;
      if (db != null && connection.getValue() != null) {
        clazz =
            (YTImmutableClass)
                db.getMetadata().getImmutableSchemaSnapshot().getClass(connection.getValue());
      }
      if (connection.getKey() == ODirection.OUT) {
        edge =
            new YTEdgeDelegate(
                this.sourceVertex, value.asVertex().get(), clazz, connection.getValue());
      } else {
        edge =
            new YTEdgeDelegate(
                value.asVertex().get(), this.sourceVertex, clazz, connection.getValue());
      }
    } else if (value.isEdge()) {
      // EDGE
      edge = value.asEdge().get();
    } else {
      throw new IllegalStateException(
          "Invalid content found while iterating edges, value '" + value + "' is not an edge");
    }

    return edge;
  }

  @Override
  public YTEdge next() {
    return createGraphElement(super.next());
  }

  public boolean filter(final YTEdge iObject) {
    if (targetVertex != null
        && !targetVertex.equals(iObject.getVertex(connection.getKey().opposite()))) {
      return false;
    }

    return iObject.isLabeled(labels);
  }

  @Override
  public boolean canUseMultiValueDirectly() {
    return true;
  }
}
