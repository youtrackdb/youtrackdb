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
package com.orientechnologies.orient.core.iterator;

import com.orientechnologies.common.util.OResettable;
import com.orientechnologies.common.util.OSizeable;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Iterator that created wrapped objects during browsing.
 */
public abstract class OLazyWrapperIterator<T>
    implements Iterator<T>, Iterable<T>, OResettable, OSizeable {

  protected final Iterator<?> iterator;
  protected OIdentifiable nextRecord;
  protected T nextElement;
  protected final int size; // -1 = UNKNOWN
  protected Object multiValue;

  public OLazyWrapperIterator(final Iterator<?> iterator) {
    this.iterator = iterator;
    this.size = -1;
  }

  public OLazyWrapperIterator(
      final Iterator<?> iterator, final int iSize, final Object iOriginalValue) {
    this.iterator = iterator;
    this.size = iSize;
    this.multiValue = iOriginalValue;
  }

  public abstract boolean filter(T iObject);

  public abstract boolean canUseMultiValueDirectly();

  public abstract T createGraphElement(Object iObject);

  public OIdentifiable getGraphElementRecord(final Object iObject) {
    return (OIdentifiable) iObject;
  }

  @Override
  public Iterator<T> iterator() {
    reset();
    return this;
  }

  public int size() {
    if (size > -1) {
      return size;
    }

    if (iterator instanceof OSizeable) {
      return ((OSizeable) iterator).size();
    }

    return 0;
  }

  @Override
  public void reset() {
    if (iterator instanceof OResettable)
    // RESET IT FOR MULTIPLE ITERATIONS
    {
      ((OResettable) iterator).reset();
    }
    nextElement = null;
  }

  @Override
  public boolean hasNext() {
    // ACT ON RECORDS (FASTER & LIGHTER)
    while (nextRecord == null && iterator.hasNext()) {
      nextRecord = getGraphElementRecord(iterator.next());
    }

    return nextRecord != null;
  }

  @Override
  public T next() {
    if (hasNext())
    // ACT ON RECORDS (FASTER & LIGHTER)
    {
      try {
        return (T) nextRecord;
      } finally {
        nextRecord = null;
      }
    }

    throw new NoSuchElementException();
  }

  @Override
  public void remove() {
    iterator.remove();
  }

  public Object getMultiValue() {
    return multiValue;
  }
}
