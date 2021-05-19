package io.raven.vertx.ranger.handlers;

import io.raven.vertx.ranger.common.RotationStatus;
import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;
import lombok.Builder;

public class OORHandler implements Handler<RoutingContext> {

  private RotationStatus rotationStatus;

  @Builder
  public OORHandler(RotationStatus rotationStatus) {
    this.rotationStatus = rotationStatus;
  }

  @Override
  public void handle(RoutingContext event) {
    rotationStatus.oor();
    event.response().end();
  }
}
