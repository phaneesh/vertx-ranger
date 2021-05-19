package io.raven.vertx.ranger;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.ranger.ServiceFinderBuilders;
import com.flipkart.ranger.finder.sharded.SimpleShardedServiceFinder;
import com.flipkart.ranger.model.ServiceNode;
import io.raven.vertx.ranger.common.Constants;
import io.raven.vertx.ranger.common.ShardInfo;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Client that returns a healthy node from nodes for a particular environment
 */
@Slf4j
public class ServiceDiscoveryClient {

  private final ShardInfo criteria;
  private SimpleShardedServiceFinder<ShardInfo> serviceFinder;

  @Builder(builderMethodName = "fromConnectionString", builderClassName = "FromConnectionStringBuilder")
  private ServiceDiscoveryClient(
      String namespace,
      String serviceName,
      String environment,
      ObjectMapper objectMapper,
      String connectionString,
      int refreshTimeMs,
      boolean disableWatchers) {
    this(namespace,
        serviceName,
        environment,
        objectMapper,
        CuratorFrameworkFactory.newClient(connectionString, new RetryForever(5000)),
        refreshTimeMs,
        disableWatchers);
  }

  @Builder(builderMethodName = "fromCurator", builderClassName = "FromCuratorBuilder")
  ServiceDiscoveryClient(
      String namespace,
      String serviceName,
      String environment,
      ObjectMapper objectMapper,
      CuratorFramework curator,
      int refreshTimeMs,
      boolean disableWatchers) {

    int effectiveRefreshTimeMs = refreshTimeMs;
    if (effectiveRefreshTimeMs < Constants.MINIMUM_REFRESH_TIME) {
      effectiveRefreshTimeMs = Constants.MINIMUM_REFRESH_TIME;
      log.warn("Node info update interval too low: {} ms. Has been upgraded to {} ms ",
          refreshTimeMs,
          Constants.MINIMUM_REFRESH_TIME);
    }

    this.criteria = ShardInfo.builder()
        .environment(environment)
        .build();
    this.serviceFinder = ServiceFinderBuilders.<ShardInfo>shardedFinderBuilder()
        .withCuratorFramework(curator)
        .withNamespace(namespace)
        .withServiceName(serviceName)
        .withDeserializer(data -> {
          try {
            return objectMapper.readValue(data,
                new TypeReference<>() {
                });
          }
          catch (Exception e) {
            log.warn("Could not parse node data", e);
          }
          return null;
        })
        .withNodeRefreshIntervalMs(effectiveRefreshTimeMs)
        .withDisableWatchers(disableWatchers)
        .withShardSelector(new HierarchicalEnvironmentAwareShardSelector())
        .build();
  }

  public CompletableFuture<Void> start() {
    return CompletableFuture.runAsync(() -> {
      try {
        serviceFinder.start();
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    });
  }

  public CompletableFuture<Void> stop() {
    return CompletableFuture.runAsync(() -> {
      try {
        serviceFinder.stop();
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    });
  }

  public CompletableFuture<Optional<ServiceNode<ShardInfo>>> getNode() {
    return getNode(criteria);
  }

  public CompletableFuture<List<ServiceNode<ShardInfo>>> getAllNodes() {
    return getAllNodes(criteria);
  }

  public CompletableFuture<Optional<ServiceNode<ShardInfo>>> getNode(final ShardInfo shardInfo) {
    CompletableFuture<Optional<ServiceNode<ShardInfo>>> result = new CompletableFuture<>();
    result.complete(Optional.ofNullable(serviceFinder.get(shardInfo)));
    return result;
  }

  public CompletableFuture<List<ServiceNode<ShardInfo>>> getAllNodes(final ShardInfo shardInfo) {
    CompletableFuture<List<ServiceNode<ShardInfo>>> result = new CompletableFuture<>();
    result.complete(serviceFinder.getAll(shardInfo));
    return result;
  }


}