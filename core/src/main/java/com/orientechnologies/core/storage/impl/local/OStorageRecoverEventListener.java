package com.orientechnologies.core.storage.impl.local;

import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.record.impl.YTEntityImpl;

/**
 * Event Listener interface invoked during storage recovering.
 */
public interface OStorageRecoverEventListener {

  void onScannedEdge(YTEntityImpl edge);

  void onRemovedEdge(YTEntityImpl edge);

  void onScannedVertex(YTEntityImpl vertex);

  void onScannedLink(YTIdentifiable link);

  void onRemovedLink(YTIdentifiable link);

  void onRepairedVertex(YTEntityImpl vertex);
}
