package io.raven.vertx.ranger.healthchecks;

import com.flipkart.ranger.healthcheck.Healthcheck;
import com.flipkart.ranger.healthcheck.HealthcheckStatus;

/**
 * The following will return healthy only after stipulated time
 * This will give other bundles etc to startup properly
 * By the time the node joins the cluster
 */
public class InitialDelayChecker implements Healthcheck {

  private final long validRegistrationTime;


  public InitialDelayChecker(long initialDelaySeconds) {
    validRegistrationTime = System.currentTimeMillis() + initialDelaySeconds * 1000;
  }

  @Override
  public HealthcheckStatus check() {
    return System.currentTimeMillis() > validRegistrationTime
        ? HealthcheckStatus.healthy
        : HealthcheckStatus.unhealthy;
  }
}