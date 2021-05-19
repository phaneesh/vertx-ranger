package io.raven.vertx.ranger.handlers;

import io.raven.vertx.ranger.ServiceDiscoveryClient;
import io.vertx.core.Handler;
import io.vertx.core.json.Json;
import io.vertx.ext.web.RoutingContext;
import lombok.Builder;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class InfoHandler implements Handler<RoutingContext> {

  private ServiceDiscoveryClient serviceDiscoveryClient;

  @Builder
  public InfoHandler(ServiceDiscoveryClient serviceDiscoveryClient) {
    this.serviceDiscoveryClient = serviceDiscoveryClient;
  }

  @Override
  public void handle(RoutingContext event) {
    serviceDiscoveryClient.getAllNodes().thenAccept( nodes -> {
      event.response().end(Json.encodeToBuffer(nodes));
    }).exceptionally( exception -> {
      log.error("Error getting ranger nodes", exception);
      event.fail(exception);
      return null;
    });

  }
}
