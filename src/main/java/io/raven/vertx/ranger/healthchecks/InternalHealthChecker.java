package io.raven.vertx.ranger.healthchecks;

import com.flipkart.ranger.healthcheck.Healthcheck;
import com.flipkart.ranger.healthcheck.HealthcheckStatus;

import java.util.List;

/**
 * Evaluates all registered healthchecks
 */
public class InternalHealthChecker implements Healthcheck {
  private final List<Healthcheck> healthchecks;

  public InternalHealthChecker(List<Healthcheck> healthchecks) {
    this.healthchecks = healthchecks;
  }

  @Override
  public HealthcheckStatus check() {
    return healthchecks.stream()
        .map(Healthcheck::check)
        .filter(healthcheckStatus -> healthcheckStatus == HealthcheckStatus.unhealthy)
        .findFirst()
        .orElse(HealthcheckStatus.healthy);
  }
}