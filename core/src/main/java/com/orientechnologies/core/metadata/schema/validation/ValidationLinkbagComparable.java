package com.orientechnologies.core.metadata.schema.validation;

import com.orientechnologies.core.db.record.ridbag.RidBag;

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
