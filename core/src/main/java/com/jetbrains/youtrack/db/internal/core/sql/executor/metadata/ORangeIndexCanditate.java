package com.jetbrains.youtrack.db.internal.core.sql.executor.metadata;

import com.jetbrains.youtrack.db.internal.core.command.OCommandContext;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTProperty;
import com.jetbrains.youtrack.db.internal.core.sql.executor.metadata.OIndexFinder.Operation;
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
