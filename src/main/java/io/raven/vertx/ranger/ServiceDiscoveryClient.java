package io.raven.vertx.ranger;

import com.fasterxml.jackson.core.type.TypeReference;
import com.flipkart.ranger.ServiceFinderBuilders;
import com.flipkart.ranger.finder.sharded.SimpleShardedServiceFinder;
import com.flipkart.ranger.model.ServiceNode;
import io.raven.vertx.ranger.common.Constants;
import io.raven.vertx.ranger.common.ShardInfo;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.ext.auth.authentication.Credentials;
import io.vertx.ext.auth.authentication.TokenCredentials;
import io.vertx.ext.auth.authentication.UsernamePasswordCredentials;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Client wrapped with WebClient that can be used to execute HTTP requests fluently with vertx style api
 */
@Slf4j
public class ServiceDiscoveryClient {

  public enum AuthType {
    NONE,
    BASIC,
    TOKEN
  }

  private final ShardInfo criteria;
  private final SimpleShardedServiceFinder<ShardInfo> serviceFinder;

  private final WebClient client;

  private final Vertx vertx;

  private final Credentials authentication;

  @Builder(builderMethodName = "fromConnectionString", builderClassName = "FromConnectionStringBuilder")
  public ServiceDiscoveryClient(String connectionString, Vertx vertx, JsonObject clientConfig) {
    this(CuratorFrameworkFactory.newClient(connectionString,
        new RetryForever(clientConfig.getInteger("retryIntervalMs", 5000))),
        vertx, clientConfig);
  }

  @Builder(builderMethodName = "fromCurator", builderClassName = "FromCuratorBuilder")
  ServiceDiscoveryClient(CuratorFramework curator, Vertx vertx, JsonObject clientConfig) {
    int effectiveRefreshTimeMs = clientConfig.getInteger("refreshTimeMs", 10000);
    if (effectiveRefreshTimeMs < Constants.MINIMUM_REFRESH_TIME) {
      effectiveRefreshTimeMs = Constants.MINIMUM_REFRESH_TIME;
      log.warn("Node info update interval too low: {} ms. Has been upgraded to {} ms ",
          clientConfig.getInteger("refreshTimeMs"), Constants.MINIMUM_REFRESH_TIME);
    }
    AuthType authType = AuthType.valueOf(clientConfig.getString("authType", "NONE"));
    switch (authType) {
      case BASIC:
        authentication = new UsernamePasswordCredentials(clientConfig.getString("userName"),
            clientConfig.getString("password"));
        break;
      case TOKEN:
        authentication = new TokenCredentials(clientConfig.getString("authToken"));
        break;
      default:
        authentication = null;
    }
    this.vertx = vertx;
    this.criteria = ShardInfo.builder()
        .environment(clientConfig.getString("environment", "local"))
        .build();
    this.serviceFinder = ServiceFinderBuilders.<ShardInfo>shardedFinderBuilder()
        .withCuratorFramework(curator)
        .withNamespace(clientConfig.getString("namespace"))
        .withServiceName(clientConfig.getString("serviceName"))
        .withDeserializer(data -> {
          try {
            return DatabindCodec.mapper().readValue(data,
                new TypeReference<>() {
                });
          } catch (Exception e) {
            log.warn("Could not parse node data", e);
          }
          return null;
        })
        .withNodeRefreshIntervalMs(effectiveRefreshTimeMs)
        .withDisableWatchers(clientConfig.getBoolean("disableWatchers", true))
        .withShardSelector(new HierarchicalEnvironmentAwareShardSelector())
        .build();
    var clientOptions = new WebClientOptions()
        .setSsl(clientConfig.getBoolean("ssl", false))
        .setMetricsName(clientConfig.getString("clientName", "vertx-web-client"))
        .setKeepAlive(clientConfig.getBoolean("keepAlive", true))
        .setMaxPoolSize(clientConfig.getInteger("maxPoolSize", 10))
        .setProtocolVersion(HttpVersion.valueOf(clientConfig.getString("protocolVersion", HttpVersion.HTTP_1_1.name())))
        .setTcpKeepAlive(clientConfig.getBoolean("keepAlive", true))
        .setConnectTimeout(clientConfig.getInteger("connectionTimeout", 3000))
        .setIdleTimeout(clientConfig.getInteger("idleTimeout", 30))
        .setIdleTimeoutUnit(TimeUnit.SECONDS)
        .setKeepAliveTimeout(clientConfig.getInteger("keepAliveTimeout", 60))
        .setTcpFastOpen(true)
        .setVerifyHost(clientConfig.getBoolean("verifyHost", false));
    this.client = WebClient.create(vertx, clientOptions);
  }

  public Future<Void> start() {
    return vertx.executeBlocking(event -> {
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

  public void get(final String requestUri, MultiMap headers, MultiMap queryParameters,
                                          Handler<Future<HttpResponse<Buffer>>> handler) {
    withRequest(HttpMethod.GET, requestUri, headers, queryParameters,
        event -> handler.handle(event.send()));
  }

  public void head(final String requestUri, MultiMap headers, MultiMap queryParameters,
                                           Handler<Future<HttpResponse<Buffer>>> handler) {
    withRequest(HttpMethod.HEAD, requestUri, headers, queryParameters, event -> handler.handle(event.send()));
  }

  public void post(final String requestUri, MultiMap headers, MultiMap queryParameters,
                                           Buffer data, Handler<Future<HttpResponse<Buffer>>> handler) {
    withRequest(HttpMethod.POST, requestUri, headers, queryParameters, event -> {
      if (Objects.nonNull(data)) {
        handler.handle(event.sendBuffer(data));
      } else {
        handler.handle(event.send());
      }
    });
  }

  public void put(final String requestUri, MultiMap headers, MultiMap queryParameters,
                  Buffer data, Handler<Future<HttpResponse<Buffer>>> handler) {
    withRequest(HttpMethod.PUT, requestUri, headers, queryParameters, event -> {
      if (Objects.nonNull(data)) {
        handler.handle(event.sendBuffer(data));
      } else {
        handler.handle(event.send());
      }
    });
  }

  public void delete(final String requestUri, MultiMap headers, MultiMap queryParameters,
                     Handler<Future<HttpResponse<Buffer>>> handler) {
    withRequest(HttpMethod.DELETE, requestUri, headers, queryParameters, event -> handler.handle(event.send()));
  }

  public void patch(final String requestUri, MultiMap headers, MultiMap queryParameters,
                    Buffer data, Handler<Future<HttpResponse<Buffer>>> handler) {
    withRequest(HttpMethod.PATCH, requestUri, headers, queryParameters, event -> {
      if (Objects.nonNull(data)) {
        handler.handle(event.sendBuffer(data));
      } else {
        handler.handle(event.send());
      }
    });
  }

  public void options(final String requestUri, MultiMap headers, MultiMap queryParameters,
                      Handler<Future<HttpResponse<Buffer>>> handler) {
    withRequest(HttpMethod.OPTIONS, requestUri, headers, queryParameters, event -> handler.handle(event.send()));
  }

  public void trace(final String requestUri, MultiMap headers, MultiMap queryParameters,
                    Handler<Future<HttpResponse<Buffer>>> handler) {
    withRequest(HttpMethod.TRACE, requestUri, headers, queryParameters, event -> handler.handle(event.send()));
  }

  private void withRequest(HttpMethod method, final String requestUri, MultiMap headers, MultiMap queryParameters, Handler<HttpRequest<Buffer>> handler) {
    vertx.runOnContext(event -> {
      ServiceNode<ShardInfo> node = serviceFinder.get(criteria);
      if (Objects.isNull(node)) {
        throw new IllegalStateException("No service node available");
      }
      var request = client.request(method, node.getPort(), node.getHost(), requestUri);
      if(Objects.nonNull(authentication)) {
        request.authentication(authentication);
      }
      if (Objects.nonNull(headers)) {
        request.putHeaders(headers);
      }
      if (Objects.nonNull(queryParameters)) {
        queryParameters.forEach(e -> request.addQueryParam(e.getKey(), e.getValue()));
      }
      handler.handle(request);
    });
  }
}