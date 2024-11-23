package com.orientechnologies.orient.core.db.record.ridbag;

import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 *
 */
public final class ORidBagDeleter {

  public static void deleteAllRidBags(ODocument document) {
    for (var propertyName : document.getPropertyNamesInternal()) {
      var value = document.getPropertyInternal(propertyName);
      if (value instanceof ORidBag) {
        ((ORidBag) value).delete();
      }
    }
  }
}
