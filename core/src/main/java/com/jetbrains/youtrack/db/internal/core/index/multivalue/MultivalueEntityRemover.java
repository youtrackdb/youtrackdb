package com.jetbrains.youtrack.db.internal.core.index.multivalue;

import com.jetbrains.youtrack.db.internal.common.types.ModifiableBoolean;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.index.IndexKeyUpdater;
import com.jetbrains.youtrack.db.internal.core.index.IndexUpdateAction;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.IndexRIDContainerSBTree;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.MixedIndexRIDContainer;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class MultivalueEntityRemover implements IndexKeyUpdater<Object> {

  private final Identifiable value;
  private final ModifiableBoolean removed;

  public MultivalueEntityRemover(Identifiable value, ModifiableBoolean removed) {
    this.value = value;
    this.removed = removed;
  }

  @Override
  public IndexUpdateAction<Object> update(Object persistentValue, AtomicLong bonsayFileId) {
    @SuppressWarnings("unchecked")
    Set<Identifiable> values = (Set<Identifiable>) persistentValue;
    if (value == null) {
      removed.setValue(true);

      //noinspection unchecked
      return IndexUpdateAction.remove();
    } else if (values.remove(value)) {
      removed.setValue(true);

      if (values.isEmpty()) {
        // remove tree ridbag too
        if (values instanceof MixedIndexRIDContainer) {
          ((MixedIndexRIDContainer) values).delete();
        } else if (values instanceof IndexRIDContainerSBTree) {
          ((IndexRIDContainerSBTree) values).delete();
        }

        //noinspection unchecked
        return IndexUpdateAction.remove();
      } else {
        return IndexUpdateAction.changed(values);
      }
    }

    return IndexUpdateAction.changed(values);
  }
}
