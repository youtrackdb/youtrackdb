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
package com.jetbrains.youtrack.db.api.record;

import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException;
import java.util.Comparator;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Base interface for identifiable objects. This abstraction is required to use RID and Record in
 * many points.
 */
public interface Identifiable extends Comparable<Identifiable>, Comparator<Identifiable> {

  /**
   * Returns the record identity.
   *
   * @return RID instance
   */
  RID getIdentity();

  /**
   * Returns the record instance.
   *
   * @return Record instance
   * @throws RecordNotFoundException if the record does not exist
   */
  @Nonnull
  <T extends DBRecord> T getRecord();

  /**
   * Returns the record instance, or null if the record does not exist.
   *
   * @return Record instance or null if the record does not exist
   * @see #getRecord()
   */
  @Nullable
  default <T extends DBRecord> T getRecordSilently() {
    try {
      return getRecord();
    } catch (RecordNotFoundException e) {
      return null;
    }
  }

  /**
   * Returns the entity instance associated with given identifiable, otherwise throws exception.
   *
   * @return Record instance
   * @throws DatabaseException if the record is not an element.
   */
  @Nonnull
  default Entity getEntity() {
    var record = getRecord();
    if (record instanceof Entity element) {
      return element;
    }

    throw new DatabaseException("Record " + getIdentity() + " is not an entity.");
  }

  /**
   * Returns the entity instance associated with given identifiable, or null if the record does not
   * exist.
   *
   * @return Record instance or null if the record does not exist
   * @see #getEntity()
   */
  @Nullable
  default Entity getElementSilently() {
    try {
      return getEntity();
    } catch (RecordNotFoundException e) {
      return null;
    }
  }

  /**
   * Returns the blob instance associated with given identifiable, otherwise throws exception.
   *
   * @return Record instance
   * @throws DatabaseException if the record is not a blob.
   */
  @Nonnull
  default Blob getBlob() {
    var record = getRecord();
    if (record instanceof Blob blob) {
      return blob;
    }

    throw new DatabaseException("Record " + getIdentity() + " is not a blob.");
  }

  /**
   * Returns the blob instance associated with given identifiable, or null if the record does not
   * exist.
   *
   * @return Record instance or null if the record does not exist
   * @see #getBlob()
   */
  @Nullable
  default Blob getBlobSilently() {
    try {
      return getBlob();
    } catch (RecordNotFoundException e) {
      return null;
    }
  }

  /**
   * Returns the edge instance associated with given identifiable, otherwise throws exception.
   *
   * @return Record instance
   * @throws DatabaseException if the record is not an edge.
   */
  @Nonnull
  default Edge getEdge() {
    var record = getRecord();
    if (record instanceof Edge edge) {
      return edge;
    }

    throw new DatabaseException("Record " + getIdentity() + " is not an edge.");
  }

  /**
   * Returns the edge instance associated with given identifiable, or null if the record does not
   * exist.
   *
   * @return Record instance or null if the record does not exist
   * @see #getEdge()
   */
  @Nullable
  default Edge getEdgeSilently() {
    try {
      return getEdge();
    } catch (RecordNotFoundException e) {
      return null;
    }
  }

  /**
   * Returns the vertex instance associated with given identifiable, otherwise throws exception.
   *
   * @return Record instance
   * @throws DatabaseException if the record is not a vertex.
   */
  @Nonnull
  default Vertex getVertex() {
    var record = getRecord();
    if (record instanceof Vertex vertex) {
      return vertex;
    }

    throw new DatabaseException("Record " + getIdentity() + " is not a vertex.");
  }

  /**
   * Returns the vertex instance associated with given identifiable, or null if the record does not
   * exist.
   *
   * @return Record instance or null if the record does not exist
   * @see #getVertex()
   */
  @Nullable
  default Vertex getVertexSilently() {
    try {
      return getVertex();
    } catch (RecordNotFoundException e) {
      return null;
    }
  }
}
