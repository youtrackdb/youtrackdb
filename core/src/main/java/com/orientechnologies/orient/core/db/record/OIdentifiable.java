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
package com.orientechnologies.orient.core.db.record;

import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.OBlob;
import java.util.Comparator;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Base interface for identifiable objects. This abstraction is required to use ORID and ORecord in
 * many points.
 */
public interface OIdentifiable extends Comparable<OIdentifiable>, Comparator<OIdentifiable> {

  /**
   * Returns the record identity.
   *
   * @return ORID instance
   */
  ORID getIdentity();

  /**
   * Returns the record instance.
   *
   * @return ORecord instance
   * @throws com.orientechnologies.orient.core.exception.ORecordNotFoundException if the record does
   *                                                                              not exist
   */
  @Nonnull
  <T extends ORecord> T getRecord();

  /**
   * Returns the record instance, or null if the record does not exist.
   *
   * @return ORecord instance or null if the record does not exist
   * @see #getRecord()
   */
  @Nullable
  default <T extends ORecord> T getRecordSilently() {
    try {
      return getRecord();
    } catch (ORecordNotFoundException e) {
      return null;
    }
  }

  /**
   * Returns the element instance associated with given identifiable, otherwise throws exception.
   *
   * @return ORecord instance
   * @throws ODatabaseException if the record is not an element.
   */
  @Nonnull
  default OElement getElement() {
    var record = getRecord();
    if (record instanceof OElement element) {
      return element;
    }

    throw new ODatabaseException("Record " + getIdentity() + " is not an element.");
  }

  /**
   * Returns the element instance associated with given identifiable, or null if the record does not
   * exist.
   *
   * @return ORecord instance or null if the record does not exist
   * @see #getElement()
   */
  @Nullable
  default OElement getElementSilently() {
    try {
      return getElement();
    } catch (ORecordNotFoundException e) {
      return null;
    }
  }

  /**
   * Returns the blob instance associated with given identifiable, otherwise throws exception.
   *
   * @return ORecord instance
   * @throws ODatabaseException if the record is not a blob.
   */
  @Nonnull
  default OBlob getBlob() {
    var record = getRecord();
    if (record instanceof OBlob blob) {
      return blob;
    }

    throw new ODatabaseException("Record " + getIdentity() + " is not a blob.");
  }

  /**
   * Returns the blob instance associated with given identifiable, or null if the record does not
   * exist.
   *
   * @return ORecord instance or null if the record does not exist
   * @see #getBlob()
   */
  @Nullable
  default OBlob getBlobSilently() {
    try {
      return getBlob();
    } catch (ORecordNotFoundException e) {
      return null;
    }
  }

  /**
   * Returns the edge instance associated with given identifiable, otherwise throws exception.
   *
   * @return ORecord instance
   * @throws ODatabaseException if the record is not an edge.
   */
  @Nonnull
  default OEdge getEdge() {
    var record = getRecord();
    if (record instanceof OEdge edge) {
      return edge;
    }

    throw new ODatabaseException("Record " + getIdentity() + " is not an edge.");
  }

  /**
   * Returns the edge instance associated with given identifiable, or null if the record does not
   * exist.
   *
   * @return ORecord instance or null if the record does not exist
   * @see #getEdge()
   */
  @Nullable
  default OEdge getEdgeSilently() {
    try {
      return getEdge();
    } catch (ORecordNotFoundException e) {
      return null;
    }
  }

  /**
   * Returns the vertex instance associated with given identifiable, otherwise throws exception.
   *
   * @return ORecord instance
   * @throws ODatabaseException if the record is not a vertex.
   */
  @Nonnull
  default OVertex getVertex() {
    var record = getRecord();
    if (record instanceof OVertex vertex) {
      return vertex;
    }

    throw new ODatabaseException("Record " + getIdentity() + " is not a vertex.");
  }

  /**
   * Returns the vertex instance associated with given identifiable, or null if the record does not
   * exist.
   *
   * @return ORecord instance or null if the record does not exist
   * @see #getVertex()
   */
  @Nullable
  default OVertex getVertexSilently() {
    try {
      return getVertex();
    } catch (ORecordNotFoundException e) {
      return null;
    }
  }
}
