package com.orientechnologies.orient.core.metadata.schema.validation;

import com.orientechnologies.orient.core.db.record.ridbag.RidBag;

public class ValidationLinkbagComparable implements Comparable<Object> {

  private final int size;

  public ValidationLinkbagComparable(int size) {
    this.size = size;
  }

  @Override
  public int compareTo(Object o) {
    return size - ((RidBag) o).size();
  }
}
