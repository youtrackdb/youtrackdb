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

package com.orientechnologies.core.db.record.ridbag;

import com.orientechnologies.common.collection.OCollection;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OUUIDSerializer;
import com.orientechnologies.common.util.OSizeable;
import com.orientechnologies.core.config.YTContextConfiguration;
import com.orientechnologies.core.config.YTGlobalConfiguration;
import com.orientechnologies.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.core.db.record.OMultiValueChangeTimeLine;
import com.orientechnologies.core.db.record.OTrackedMultiValue;
import com.orientechnologies.core.db.record.RecordElement;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.db.record.ridbag.embedded.EmbeddedRidBag;
import com.orientechnologies.core.exception.YTDatabaseException;
import com.orientechnologies.core.exception.YTSerializationException;
import com.orientechnologies.core.id.YTRecordId;
import com.orientechnologies.core.record.YTRecord;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import com.orientechnologies.core.serialization.serializer.record.binary.BytesContainer;
import com.orientechnologies.core.serialization.serializer.string.OStringBuilderSerializable;
import com.orientechnologies.core.storage.index.sbtreebonsai.local.OSBTreeBonsai;
import com.orientechnologies.core.storage.ridbag.RemoteTreeRidBag;
import com.orientechnologies.core.storage.ridbag.sbtree.Change;
import com.orientechnologies.core.storage.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.core.storage.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.core.storage.ridbag.sbtree.OSBTreeRidBag;
import java.util.Base64;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nonnull;

/**
 * A collection that contain links to {@link YTIdentifiable}. Bag is similar to set but can contain
 * several entering of the same object.<br>
 *
 * <p>Could be tree based and embedded representation.<br>
 *
 * <ul>
 *   <li><b>Embedded</b> stores its content directly to the document that owns it.<br>
 *       It better fits for cases when only small amount of links are stored to the bag.<br>
 *   <li><b>Tree-based</b> implementation stores its content in a separate data structure called
 *       {@link OSBTreeBonsai}.<br>
 *       It fits great for cases when you have a huge amount of links.<br>
 * </ul>
 *
 * <br>
 * The representation is automatically converted to tree-based implementation when top threshold is
 * reached. And backward to embedded one when size is decreased to bottom threshold. <br>
 * The thresholds could be configured by {@link
 * YTGlobalConfiguration#RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD} and {@link
 * YTGlobalConfiguration#RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD}. <br>
 * <br>
 * This collection is used to efficiently manage relationships in graph model.<br>
 * <br>
 * Does not implement {@link Collection} interface because some operations could not be efficiently
 * implemented and that's why should be avoided.<br>
 *
 * @since 1.7rc1
 */
public class RidBag
    implements OStringBuilderSerializable,
    Iterable<YTIdentifiable>,
    OSizeable,
    OTrackedMultiValue<YTIdentifiable, YTIdentifiable>,
    OCollection<YTIdentifiable>,
    RecordElement {

  private RidBagDelegate delegate;
  private YTRecordId ownerRecord;
  private String fieldName;

  private int topThreshold;
  private int bottomThreshold;

  private UUID uuid;

  public RidBag(YTDatabaseSessionInternal session, final RidBag ridBag) {
    initThresholds(session);
    init();
    for (YTIdentifiable identifiable : ridBag) {
      add(identifiable);
    }
  }

  public RidBag(YTDatabaseSessionInternal session) {
    initThresholds(session);
    init();
  }

  public RidBag(YTDatabaseSessionInternal session, UUID uuid) {
    initThresholds(session);
    init();
    this.uuid = uuid;
  }

  public RidBag(YTDatabaseSessionInternal session, OBonsaiCollectionPointer pointer,
      Map<YTIdentifiable, Change> changes, UUID uuid) {
    initThresholds(session);
    delegate = new OSBTreeRidBag(pointer, changes);
    this.uuid = uuid;
  }

  private RidBag(YTDatabaseSessionInternal session, final byte[] stream) {
    initThresholds(session);
    fromStream(stream);
  }

  public RidBag(YTDatabaseSessionInternal session, RidBagDelegate delegate) {
    initThresholds(session);
    this.delegate = delegate;
  }

  public static RidBag fromStream(YTDatabaseSessionInternal session, final String value) {
    final byte[] stream = Base64.getDecoder().decode(value);
    return new RidBag(session, stream);
  }

  public RidBag copy(YTDatabaseSessionInternal session) {
    final RidBag copy = new RidBag(session);
    copy.topThreshold = topThreshold;
    copy.bottomThreshold = bottomThreshold;
    copy.uuid = uuid;

    if (delegate instanceof OSBTreeRidBag)
    // ALREADY MULTI-THREAD
    {
      copy.delegate = delegate;
    } else {
      copy.delegate = ((EmbeddedRidBag) delegate).copy();
    }

    return copy;
  }

  /**
   * THIS IS VERY EXPENSIVE METHOD AND CAN NOT BE CALLED IN REMOTE STORAGE.
   *
   * @param identifiable Object to check.
   * @return true if ridbag contains at leas one instance with the same rid as passed in
   * identifiable.
   */
  public boolean contains(YTIdentifiable identifiable) {
    return delegate.contains(identifiable);
  }

  public void addAll(Collection<YTIdentifiable> values) {
    delegate.addAll(values);
  }

  @Override
  public void add(YTIdentifiable identifiable) {
    delegate.add(identifiable);
  }

  @Override
  public boolean addInternal(YTIdentifiable e) {
    return delegate.addInternal(e);
  }

  @Override
  public void remove(YTIdentifiable identifiable) {
    delegate.remove(identifiable);
  }

  public boolean isEmpty() {
    return delegate.isEmpty();
  }

  @Nonnull
  @Override
  public Iterator<YTIdentifiable> iterator() {
    return delegate.iterator();
  }

  @Override
  public int size() {
    return delegate.size();
  }

  public boolean isEmbedded() {
    return delegate instanceof EmbeddedRidBag;
  }

  public boolean isToSerializeEmbedded() {
    if (isEmbedded()) {
      return true;
    }
    if (getOwner() instanceof YTRecord && !((YTRecord) getOwner()).getIdentity().isPersistent()) {
      return true;
    }
    return bottomThreshold >= size();
  }

  public int toStream(BytesContainer bytesContainer) throws YTSerializationException {

    checkAndConvert();

    final UUID oldUuid = uuid;
    final OSBTreeCollectionManager sbTreeCollectionManager =
        ODatabaseRecordThreadLocal.instance().get().getSbTreeCollectionManager();
    if (sbTreeCollectionManager != null) {
      uuid = sbTreeCollectionManager.listenForChanges(this);
    } else {
      uuid = null;
    }

    boolean hasUuid = uuid != null;

    final int serializedSize =
        OByteSerializer.BYTE_SIZE
            + delegate.getSerializedSize()
            + ((hasUuid) ? OUUIDSerializer.UUID_SIZE : 0);
    int pointer = bytesContainer.alloc(serializedSize);
    int offset = pointer;
    final byte[] stream = bytesContainer.bytes;

    byte configByte = 0;
    if (isEmbedded()) {
      configByte |= 1;
    }

    if (hasUuid) {
      configByte |= 2;
    }

    stream[offset++] = configByte;

    if (hasUuid) {
      OUUIDSerializer.INSTANCE.serialize(uuid, stream, offset);
      offset += OUUIDSerializer.UUID_SIZE;
    }

    delegate.serialize(stream, offset, oldUuid);
    return pointer;
  }

  public void checkAndConvert() {
    YTDatabaseSessionInternal database = ODatabaseRecordThreadLocal.instance().getIfDefined();
    if (database != null && !database.isRemote()) {
      if (isEmbedded()
          && ODatabaseRecordThreadLocal.instance().get().getSbTreeCollectionManager() != null
          && delegate.size() >= topThreshold) {
        convertToTree();
      } else if (bottomThreshold >= 0 && !isEmbedded() && delegate.size() <= bottomThreshold) {
        convertToEmbedded();
      }
    }
  }

  private void convertToEmbedded() {
    RidBagDelegate oldDelegate = delegate;
    boolean isTransactionModified = oldDelegate.isTransactionModified();
    delegate = new EmbeddedRidBag();

    final RecordElement owner = oldDelegate.getOwner();
    delegate.disableTracking(owner);
    for (YTIdentifiable identifiable : oldDelegate) {
      delegate.add(identifiable);
    }

    delegate.setOwner(owner);

    delegate.setTracker(oldDelegate.getTracker());
    oldDelegate.disableTracking(owner);

    delegate.setDirty();
    delegate.setTransactionModified(isTransactionModified);
    delegate.enableTracking(owner);

    oldDelegate.requestDelete();
  }

  private void convertToTree() {
    RidBagDelegate oldDelegate = delegate;
    boolean isTransactionModified = oldDelegate.isTransactionModified();
    delegate = new OSBTreeRidBag();

    final RecordElement owner = oldDelegate.getOwner();
    delegate.disableTracking(owner);
    for (YTIdentifiable identifiable : oldDelegate) {
      delegate.add(identifiable);
    }

    delegate.setOwner(owner);

    delegate.setTracker(oldDelegate.getTracker());
    oldDelegate.disableTracking(owner);
    delegate.setDirty();
    delegate.setTransactionModified(isTransactionModified);
    delegate.enableTracking(owner);

    oldDelegate.requestDelete();
  }

  @Override
  public OStringBuilderSerializable toStream(StringBuilder output) throws YTSerializationException {
    final BytesContainer container = new BytesContainer();
    toStream(container);
    output.append(Base64.getEncoder().encodeToString(container.fitBytes()));
    return this;
  }

  @Override
  public String toString() {
    return delegate.toString();
  }

  public void delete() {
    delegate.requestDelete();
  }

  @Override
  public OStringBuilderSerializable fromStream(StringBuilder input)
      throws YTSerializationException {
    final byte[] stream = Base64.getDecoder().decode(input.toString());
    fromStream(stream);
    return this;
  }

  public void fromStream(final byte[] stream) {
    fromStream(new BytesContainer(stream));
  }

  public void fromStream(BytesContainer stream) {
    final byte first = stream.bytes[stream.offset++];
    if ((first & 1) == 1) {
      delegate = new EmbeddedRidBag();
    } else {
      delegate = new OSBTreeRidBag();
    }

    if ((first & 2) == 2) {
      uuid = OUUIDSerializer.INSTANCE.deserialize(stream.bytes, stream.offset);
      stream.skip(OUUIDSerializer.UUID_SIZE);
    }

    stream.skip(delegate.deserialize(stream.bytes, stream.offset) - stream.offset);
  }

  @Override
  public Object returnOriginalState(
      YTDatabaseSessionInternal session,
      List<OMultiValueChangeEvent<YTIdentifiable, YTIdentifiable>> multiValueChangeEvents) {
    return new RidBag(session,
        (RidBagDelegate) delegate.returnOriginalState(session, multiValueChangeEvents));
  }

  @Override
  public Class<?> getGenericClass() {
    return delegate.getGenericClass();
  }

  public void setOwner(RecordElement owner) {
    if ((!(owner instanceof YTEntityImpl) && owner != null)
        || (owner != null && ((YTEntityImpl) owner).isEmbedded())) {
      throw new YTDatabaseException("RidBag are supported only at document root");
    }
    delegate.setOwner(owner);
  }

  /**
   * Temporary id of collection to track changes in remote mode.
   *
   * <p>WARNING! Method is for internal usage.
   *
   * @return UUID
   */
  public UUID getTemporaryId() {
    return uuid;
  }

  public void setTemporaryId(UUID uuid) {
    this.uuid = uuid;
  }

  /**
   * Notify collection that changes has been saved. Converts to non embedded implementation if
   * needed.
   *
   * <p>WARNING! Method is for internal usage.
   *
   * @param newPointer new collection pointer
   */
  public void notifySaved(OBonsaiCollectionPointer newPointer) {
    if (newPointer.isValid()) {
      if (isEmbedded()) {
        replaceWithSBTree(newPointer);
      } else if (delegate instanceof OSBTreeRidBag) {
        ((OSBTreeRidBag) delegate).setCollectionPointer(newPointer);
        ((OSBTreeRidBag) delegate).clearChanges();
      }
    }
  }

  public OBonsaiCollectionPointer getPointer() {
    if (isEmbedded()) {
      return OBonsaiCollectionPointer.INVALID;
    } else if (delegate instanceof RemoteTreeRidBag) {
      return ((RemoteTreeRidBag) delegate).getCollectionPointer();
    } else {
      return ((OSBTreeRidBag) delegate).getCollectionPointer();
    }
  }

  /**
   * IMPORTANT! Only for internal usage.
   */
  public boolean tryMerge(final RidBag otherValue, boolean iMergeSingleItemsOfMultiValueFields) {
    if (!isEmbedded() && !otherValue.isEmbedded()) {
      final OSBTreeRidBag thisTree = (OSBTreeRidBag) delegate;
      final OSBTreeRidBag otherTree = (OSBTreeRidBag) otherValue.delegate;
      if (thisTree.getCollectionPointer().equals(otherTree.getCollectionPointer())) {

        thisTree.mergeChanges(otherTree);

        uuid = otherValue.uuid;

        return true;
      }
    } else if (iMergeSingleItemsOfMultiValueFields) {
      for (YTIdentifiable value : otherValue) {
        if (value != null) {
          final Iterator<YTIdentifiable> localIter = iterator();
          boolean found = false;
          while (localIter.hasNext()) {
            final YTIdentifiable v = localIter.next();
            if (value.equals(v)) {
              found = true;
              break;
            }
          }
          if (!found) {
            add(value);
          }
        }
      }
      return true;
    }
    return false;
  }

  protected void initThresholds(@Nonnull YTDatabaseSessionInternal session) {
    assert session.assertIfNotActive();
    YTContextConfiguration conf = session.getConfiguration();
    topThreshold =
        conf.getValueAsInteger(YTGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD);

    bottomThreshold =
        conf.getValueAsInteger(YTGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD);
  }

  protected void init() {
    if (topThreshold < 0) {
      if (ODatabaseRecordThreadLocal.instance().isDefined()
          && !ODatabaseRecordThreadLocal.instance().get().isRemote()) {
        delegate = new OSBTreeRidBag();
      } else {
        delegate = new EmbeddedRidBag();
      }
    } else {
      delegate = new EmbeddedRidBag();
    }
  }

  /**
   * Silently replace delegate by tree implementation.
   *
   * @param pointer new collection pointer
   */
  private void replaceWithSBTree(OBonsaiCollectionPointer pointer) {
    delegate.requestDelete();
    final RemoteTreeRidBag treeBag = new RemoteTreeRidBag(pointer);
    treeBag.setRecordAndField(ownerRecord, fieldName);
    treeBag.setOwner(delegate.getOwner());
    treeBag.setTracker(delegate.getTracker());
    delegate = treeBag;
  }

  public RidBagDelegate getDelegate() {
    return delegate;
  }

  public NavigableMap<YTIdentifiable, Change> getChanges() {
    return delegate.getChanges();
  }

  @Override
  public void replace(OMultiValueChangeEvent<Object, Object> event, Object newValue) {
    // not needed do nothing
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof RidBag otherRidbag)) {
      return false;
    }

    if (!delegate.getClass().equals(otherRidbag.delegate.getClass())) {
      return false;
    }

    Iterator<YTIdentifiable> firstIter = delegate.iterator();
    Iterator<YTIdentifiable> secondIter = otherRidbag.delegate.iterator();
    while (firstIter.hasNext()) {
      if (!secondIter.hasNext()) {
        return false;
      }

      YTIdentifiable firstElement = firstIter.next();
      YTIdentifiable secondElement = secondIter.next();
      if (!Objects.equals(firstElement, secondElement)) {
        return false;
      }
    }
    return !secondIter.hasNext();
  }

  @Override
  public void enableTracking(RecordElement parent) {
    delegate.enableTracking(parent);
  }

  public void disableTracking(RecordElement document) {
    delegate.disableTracking(document);
  }

  @Override
  public void transactionClear() {
    delegate.transactionClear();
  }

  @Override
  public boolean isModified() {
    return delegate.isModified();
  }

  @Override
  public OMultiValueChangeTimeLine<Object, Object> getTimeLine() {
    return delegate.getTimeLine();
  }

  @Override
  public <RET> RET setDirty() {
    return delegate.setDirty();
  }

  @Override
  public void setDirtyNoChanged() {
    delegate.setDirtyNoChanged();
  }

  @Override
  public RecordElement getOwner() {
    return delegate.getOwner();
  }

  @Override
  public boolean isTransactionModified() {
    return delegate.isTransactionModified();
  }

  @Override
  public OMultiValueChangeTimeLine<YTIdentifiable, YTIdentifiable> getTransactionTimeLine() {
    return delegate.getTransactionTimeLine();
  }

  public void setRecordAndField(YTRecordId id, String fieldName) {
    if (this.delegate instanceof RemoteTreeRidBag) {
      ((RemoteTreeRidBag) this.delegate).setRecordAndField(id, fieldName);
    }
    this.ownerRecord = id;
    this.fieldName = fieldName;
  }

  public void makeTree() {
    if (isEmbedded()) {
      convertToTree();
    }
  }

  public void makeEmbedded() {
    if (!isEmbedded()) {
      convertToEmbedded();
    }
  }
}
