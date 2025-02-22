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

package com.jetbrains.youtrack.db.internal.core.db.record.ridbag;

import com.jetbrains.youtrack.db.api.config.ContextConfiguration;
import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import com.jetbrains.youtrack.db.api.record.DBRecord;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.internal.common.collection.DataContainer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.ByteSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.UUIDSerializer;
import com.jetbrains.youtrack.db.internal.common.util.Sizeable;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.MultiValueChangeEvent;
import com.jetbrains.youtrack.db.internal.core.db.record.MultiValueChangeTimeLine;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordElement;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedMultiValue;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.embedded.EmbeddedRidBag;
import com.jetbrains.youtrack.db.internal.core.exception.SerializationException;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.BytesContainer;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.string.StringBuilderSerializable;
import com.jetbrains.youtrack.db.internal.core.storage.index.sbtreebonsai.local.SBTreeBonsai;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.RemoteTreeRidBag;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.BonsaiCollectionPointer;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.Change;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.SBTreeCollectionManager;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.SBTreeRidBag;
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
 * A collection that contain links to {@link Identifiable}. Bag is similar to set but can contain
 * several entering of the same object.<br>
 *
 * <p>Could be tree based and embedded representation.<br>
 *
 * <ul>
 *   <li><b>Embedded</b> stores its content directly to the entity that owns it.<br>
 *       It better fits for cases when only small amount of links are stored to the bag.<br>
 *   <li><b>Tree-based</b> implementation stores its content in a separate data structure called
 *       {@link SBTreeBonsai}.<br>
 *       It fits great for cases when you have a huge amount of links.<br>
 * </ul>
 *
 * <br>
 * The representation is automatically converted to tree-based implementation when top threshold is
 * reached. And backward to embedded one when size is decreased to bottom threshold. <br>
 * The thresholds could be configured by {@link
 * GlobalConfiguration#RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD} and {@link
 * GlobalConfiguration#RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD}. <br>
 * <br>
 * This collection is used to efficiently manage relationships in graph model.<br>
 * <br>
 * Does not implement {@link Collection} interface because some operations could not be efficiently
 * implemented and that's why should be avoided.<br>
 *
 * @since 1.7rc1
 */
public class RidBag
    implements StringBuilderSerializable,
    Iterable<Identifiable>,
    Sizeable,
    TrackedMultiValue<Identifiable, Identifiable>,
    DataContainer<Identifiable>,
    RecordElement {

  private RidBagDelegate delegate;
  private RecordId ownerRecord;
  private String fieldName;

  private int topThreshold;
  private int bottomThreshold;

  private UUID uuid;

  public RidBag(DatabaseSessionInternal session, final RidBag ridBag) {
    initThresholds(session);
    init();
    for (Identifiable identifiable : ridBag) {
      add(identifiable);
    }
  }

  public RidBag(DatabaseSessionInternal session) {
    initThresholds(session);
    init();
  }

  public RidBag(DatabaseSessionInternal session, UUID uuid) {
    initThresholds(session);
    init();
    this.uuid = uuid;
  }

  public RidBag(DatabaseSessionInternal session, BonsaiCollectionPointer pointer,
      Map<Identifiable, Change> changes, UUID uuid) {
    initThresholds(session);
    delegate = new SBTreeRidBag(pointer, changes);
    this.uuid = uuid;
  }

  private RidBag(DatabaseSessionInternal session, final byte[] stream) {
    initThresholds(session);
    fromStream(stream);
  }

  public RidBag(DatabaseSessionInternal session, RidBagDelegate delegate) {
    initThresholds(session);
    this.delegate = delegate;
  }

  public static RidBag fromStream(DatabaseSessionInternal session, final String value) {
    final byte[] stream = Base64.getDecoder().decode(value);
    return new RidBag(session, stream);
  }

  public RidBag copy(DatabaseSessionInternal session) {
    final RidBag copy = new RidBag(session);
    copy.topThreshold = topThreshold;
    copy.bottomThreshold = bottomThreshold;
    copy.uuid = uuid;

    if (delegate instanceof SBTreeRidBag)
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
  public boolean contains(Identifiable identifiable) {
    return delegate.contains(identifiable);
  }

  public void addAll(Collection<Identifiable> values) {
    delegate.addAll(values);
  }

  @Override
  public void add(Identifiable identifiable) {
    delegate.add(identifiable);
  }

  @Override
  public boolean addInternal(Identifiable e) {
    return delegate.addInternal(e);
  }

  @Override
  public void remove(Identifiable identifiable) {
    delegate.remove(identifiable);
  }

  public boolean isEmpty() {
    return delegate.isEmpty();
  }

  @Nonnull
  @Override
  public Iterator<Identifiable> iterator() {
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
    if (getOwner() instanceof DBRecord && !((DBRecord) getOwner()).getIdentity().isPersistent()) {
      return true;
    }
    return bottomThreshold >= size();
  }

  public int toStream(BytesContainer bytesContainer) throws SerializationException {

    checkAndConvert();

    final UUID oldUuid = uuid;
    final SBTreeCollectionManager sbTreeCollectionManager =
        DatabaseRecordThreadLocal.instance().get().getSbTreeCollectionManager();
    if (sbTreeCollectionManager != null) {
      uuid = sbTreeCollectionManager.listenForChanges(this);
    } else {
      uuid = null;
    }

    boolean hasUuid = uuid != null;

    final int serializedSize =
        ByteSerializer.BYTE_SIZE
            + delegate.getSerializedSize()
            + ((hasUuid) ? UUIDSerializer.UUID_SIZE : 0);
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
      UUIDSerializer.INSTANCE.serialize(uuid, stream, offset);
      offset += UUIDSerializer.UUID_SIZE;
    }

    delegate.serialize(stream, offset, oldUuid);
    return pointer;
  }

  public void checkAndConvert() {
    DatabaseSessionInternal database = DatabaseRecordThreadLocal.instance().getIfDefined();
    if (database != null && !database.isRemote()) {
      if (isEmbedded()
          && DatabaseRecordThreadLocal.instance().get().getSbTreeCollectionManager() != null
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
    for (Identifiable identifiable : oldDelegate) {
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
    delegate = new SBTreeRidBag();

    final RecordElement owner = oldDelegate.getOwner();
    delegate.disableTracking(owner);
    for (Identifiable identifiable : oldDelegate) {
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
  public StringBuilderSerializable toStream(StringBuilder output) throws SerializationException {
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
  public StringBuilderSerializable fromStream(StringBuilder input)
      throws SerializationException {
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
      delegate = new SBTreeRidBag();
    }

    if ((first & 2) == 2) {
      uuid = UUIDSerializer.INSTANCE.deserialize(stream.bytes, stream.offset);
      stream.skip(UUIDSerializer.UUID_SIZE);
    }

    stream.skip(delegate.deserialize(stream.bytes, stream.offset) - stream.offset);
  }

  @Override
  public Object returnOriginalState(
      DatabaseSessionInternal session,
      List<MultiValueChangeEvent<Identifiable, Identifiable>> multiValueChangeEvents) {
    return new RidBag(session,
        (RidBagDelegate) delegate.returnOriginalState(session, multiValueChangeEvents));
  }

  @Override
  public Class<?> getGenericClass() {
    return delegate.getGenericClass();
  }

  public void setOwner(RecordElement owner) {
    if ((!(owner instanceof EntityImpl) && owner != null)
        || (owner != null && ((EntityImpl) owner).isEmbedded())) {
      throw new DatabaseException("RidBag are supported only at entity root");
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
  public void notifySaved(BonsaiCollectionPointer newPointer) {
    if (newPointer.isValid()) {
      if (isEmbedded()) {
        replaceWithSBTree(newPointer);
      } else if (delegate instanceof SBTreeRidBag) {
        ((SBTreeRidBag) delegate).setCollectionPointer(newPointer);
        ((SBTreeRidBag) delegate).clearChanges();
      }
    }
  }

  public BonsaiCollectionPointer getPointer() {
    if (isEmbedded()) {
      return BonsaiCollectionPointer.INVALID;
    } else if (delegate instanceof RemoteTreeRidBag) {
      return ((RemoteTreeRidBag) delegate).getCollectionPointer();
    } else {
      return ((SBTreeRidBag) delegate).getCollectionPointer();
    }
  }

  /**
   * IMPORTANT! Only for internal usage.
   */
  public boolean tryMerge(final RidBag otherValue, boolean iMergeSingleItemsOfMultiValueFields) {
    if (!isEmbedded() && !otherValue.isEmbedded()) {
      final SBTreeRidBag thisTree = (SBTreeRidBag) delegate;
      final SBTreeRidBag otherTree = (SBTreeRidBag) otherValue.delegate;
      if (thisTree.getCollectionPointer().equals(otherTree.getCollectionPointer())) {

        thisTree.mergeChanges(otherTree);

        uuid = otherValue.uuid;

        return true;
      }
    } else if (iMergeSingleItemsOfMultiValueFields) {
      for (Identifiable value : otherValue) {
        if (value != null) {
          final Iterator<Identifiable> localIter = iterator();
          boolean found = false;
          while (localIter.hasNext()) {
            final Identifiable v = localIter.next();
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

  protected void initThresholds(@Nonnull DatabaseSessionInternal session) {
    assert session.assertIfNotActive();
    ContextConfiguration conf = session.getConfiguration();
    topThreshold =
        conf.getValueAsInteger(GlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD);

    bottomThreshold =
        conf.getValueAsInteger(GlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD);
  }

  protected void init() {
    if (topThreshold < 0) {
      if (DatabaseRecordThreadLocal.instance().isDefined()
          && !DatabaseRecordThreadLocal.instance().get().isRemote()) {
        delegate = new SBTreeRidBag();
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
  private void replaceWithSBTree(BonsaiCollectionPointer pointer) {
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

  public NavigableMap<Identifiable, Change> getChanges() {
    return delegate.getChanges();
  }

  @Override
  public void replace(MultiValueChangeEvent<Object, Object> event, Object newValue) {
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

    Iterator<Identifiable> firstIter = delegate.iterator();
    Iterator<Identifiable> secondIter = otherRidbag.delegate.iterator();
    while (firstIter.hasNext()) {
      if (!secondIter.hasNext()) {
        return false;
      }

      Identifiable firstElement = firstIter.next();
      Identifiable secondElement = secondIter.next();
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

  public void disableTracking(RecordElement entity) {
    delegate.disableTracking(entity);
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
  public MultiValueChangeTimeLine<Object, Object> getTimeLine() {
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
  public MultiValueChangeTimeLine<Identifiable, Identifiable> getTransactionTimeLine() {
    return delegate.getTransactionTimeLine();
  }

  public void setRecordAndField(RecordId id, String fieldName) {
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
