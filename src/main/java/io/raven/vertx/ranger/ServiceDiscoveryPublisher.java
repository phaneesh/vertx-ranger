package io.raven.vertx.ranger;

import com.flipkart.ranger.ServiceProviderBuilders;
import com.flipkart.ranger.healthcheck.Healthcheck;
import com.flipkart.ranger.serviceprovider.ServiceProvider;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.raven.vertx.ranger.common.Constants;
import io.raven.vertx.ranger.common.RotationStatus;
import io.raven.vertx.ranger.common.ShardInfo;
import io.raven.vertx.ranger.handlers.BIRHandler;
import io.raven.vertx.ranger.handlers.InfoHandler;
import io.raven.vertx.ranger.handlers.OORHandler;
import io.raven.vertx.ranger.healthchecks.InitialDelayChecker;
import io.raven.vertx.ranger.healthchecks.InternalHealthChecker;
import io.raven.vertx.ranger.healthchecks.RotationCheck;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.ext.web.Router;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import lombok.val;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

@Log4j2
public class ServiceDiscoveryPublisher {

  private JsonObject config;
  private Router router;
  private List<Healthcheck> healthchecks = Lists.newArrayList();
  private ServiceProvider<ShardInfo> serviceProvider;
  private ServiceDiscoveryClient serviceDiscoveryClient;
  @Getter
  private CuratorFramework curator;
  @Getter
  private String namespace;
  @Getter
  private String serviceName;
  private RotationStatus rotationStatus;

  @Builder
  public ServiceDiscoveryPublisher(final JsonObject config, final Router router) {
    this.config = config;
    this.router = router;
  }

  public void start() throws Exception {
    this.namespace = config.getString("namespace");
    this.serviceName = config.getString("serviceName");
    var hostname = getHost();
    var port = getPort();
    rotationStatus = new RotationStatus(config.getBoolean("inRotationStatus", true));
    curator = CuratorFrameworkFactory.builder()
        .connectString(config.getString("zkUrl"))
        .namespace(namespace)
        .retryPolicy(new RetryForever(config.getInteger("connectionRetryMillis", Constants.DEFAULT_RETRY_CONN_INTERVAL)))
        .build();
    curator.start();
    serviceProvider = buildServiceProvider(
        namespace,
        serviceName,
        hostname,
        port
    );
    serviceProvider.start();

    serviceDiscoveryClient = buildDiscoveryClient(namespace, serviceName);
    serviceDiscoveryClient.start();

    router.get("/instances").handler(InfoHandler.builder()
        .serviceDiscoveryClient(serviceDiscoveryClient)
        .build());

    router.post("/tasks/ranger-oor").handler(OORHandler.builder()
          .rotationStatus(rotationStatus)
        .build());
    router.post("/tasks/ranger-bir").handler(BIRHandler.builder()
        .rotationStatus(rotationStatus)
        .build());

  }


  protected int getPort() {
    Preconditions.checkArgument(
        Constants.DEFAULT_PORT != config.getInteger("publishedPort", Constants.DEFAULT_PORT)
            && 0 != config.getInteger("publishedPort", Constants.DEFAULT_PORT),
        "Looks like publishedPost has not been set and getPort() has not been overridden. This is wrong. \n" +
            "Either set publishedPort in config or override getPort() to return the port on which the service is running");
    return config.getInteger("publishedPort", Constants.DEFAULT_PORT);
  }

  protected String getHost() throws UnknownHostException {
    var host = config.getString("publishedHost", "__DEFAULT_SERVICE_HOST");
    if (Strings.isNullOrEmpty(host) || host.equals(Constants.DEFAULT_HOST)) {
      return InetAddress.getLocalHost()
          .getCanonicalHostName();
    }
    return host;
  }

  public void registerHealthcheck(Healthcheck healthcheck) {
    this.healthchecks.add(healthcheck);
  }

  public ServiceDiscoveryClient buildDiscoveryClient(String namespace, String serviceName) {
    return ServiceDiscoveryClient.fromCurator()
        .curator(curator)
        .namespace(namespace)
        .serviceName(serviceName)
        .environment(config.getString("environment", "stage"))
        .objectMapper(DatabindCodec.mapper())
        .refreshTimeMs(config.getInteger("refreshTimeMs", Constants.MINIMUM_REFRESH_TIME))
        .disableWatchers(config.getBoolean("disableWatchers", true))
        .build();
  }

  private ServiceProvider<ShardInfo> buildServiceProvider(
      String namespace,
      String serviceName,
      String hostname,
      int port) {
    val nodeInfo = ShardInfo.builder()
        .environment(config.getString("environment", "stage"))
        .build();
    val dwMonitoringInterval = config.getInteger("checkInterval", Constants.DEFAULT_CHECK_INTERVAl) == 0
        ? Constants.DEFAULT_CHECK_INTERVAl
        : config.getInteger("checkInterval", Constants.DEFAULT_CHECK_INTERVAl);
    val serviceProviderBuilder = ServiceProviderBuilders.<ShardInfo>shardedServiceProviderBuilder()
        .withCuratorFramework(curator)
        .withNamespace(namespace)
        .withServiceName(serviceName)
        .withHealthUpdateIntervalMs(dwMonitoringInterval * 1000)
        .withSerializer(data -> {
          try {
            return DatabindCodec.mapper().writeValueAsBytes(data);
          } catch (Exception e) {
            log.warn("Could not parse node data", e);
          }
          return null;
        })
        .withHostname(hostname)
        .withPort(port)
        .withNodeData(nodeInfo)
        .withHealthcheck(new InternalHealthChecker(healthchecks))
        .withHealthcheck(new RotationCheck(rotationStatus))
        .withHealthcheck(new InitialDelayChecker(config.getInteger("initialDelayChecker", 30)))
        .withHealthUpdateIntervalMs(config.getInteger("refreshTimeMs", 15000))
        .withStaleUpdateThresholdMs(10000);
    return serviceProviderBuilder.buildServiceDiscovery();
  }

}
