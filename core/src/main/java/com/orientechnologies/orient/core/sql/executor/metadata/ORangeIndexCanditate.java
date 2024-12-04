package com.orientechnologies.orient.core.sql.executor.metadata;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.metadata.schema.YTProperty;
import com.orientechnologies.orient.core.sql.executor.metadata.OIndexFinder.Operation;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class ORangeIndexCanditate implements OIndexCandidate {

  private final String name;
  private final YTProperty property;

  public ORangeIndexCanditate(String name, YTProperty property) {
    this.name = name;
    this.property = property;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Optional<OIndexCandidate> invert() {
    return Optional.of(this);
  }

  @Override
  public Operation getOperation() {
    return Operation.Range;
  }

  @Override
  public Optional<OIndexCandidate> normalize(OCommandContext ctx) {
    return Optional.of(this);
  }

  @Override
  public List<YTProperty> properties() {
    return Collections.singletonList(this.property);
  }
}
