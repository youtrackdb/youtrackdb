package com.jetbrains.youtrack.db.internal.core.storage.impl.local;

import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;

/**
 * Event Listener interface invoked during storage recovering.
 */
public interface StorageRecoverEventListener {

  void onScannedEdge(EntityImpl edge);

  void onRemovedEdge(EntityImpl edge);

  void onScannedVertex(EntityImpl vertex);

  void onScannedLink(Identifiable link);

  void onRemovedLink(Identifiable link);

  void onRepairedVertex(EntityImpl vertex);
}
