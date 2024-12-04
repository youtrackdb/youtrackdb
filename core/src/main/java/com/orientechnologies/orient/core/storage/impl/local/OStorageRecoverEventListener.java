package com.orientechnologies.orient.core.storage.impl.local;

import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.record.impl.YTDocument;

/**
 * Event Listener interface invoked during storage recovering.
 */
public interface OStorageRecoverEventListener {

  void onScannedEdge(YTDocument edge);

  void onRemovedEdge(YTDocument edge);

  void onScannedVertex(YTDocument vertex);

  void onScannedLink(YTIdentifiable link);

  void onRemovedLink(YTIdentifiable link);

  void onRepairedVertex(YTDocument vertex);
}
