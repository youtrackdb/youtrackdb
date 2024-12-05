package com.jetbrains.youtrack.db.internal.core.storage.impl.local;

import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;

/**
 * Event Listener interface invoked during storage recovering.
 */
public interface OStorageRecoverEventListener {

  void onScannedEdge(EntityImpl edge);

  void onRemovedEdge(EntityImpl edge);

  void onScannedVertex(EntityImpl vertex);

  void onScannedLink(YTIdentifiable link);

  void onRemovedLink(YTIdentifiable link);

  void onRepairedVertex(EntityImpl vertex);
}
