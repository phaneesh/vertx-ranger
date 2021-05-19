package io.raven.vertx.ranger.healthchecks;

import com.flipkart.ranger.healthcheck.Healthcheck;
import com.flipkart.ranger.healthcheck.HealthcheckStatus;
import io.raven.vertx.ranger.common.RotationStatus;


/**
 * This allows the node to be taken offline in the cluster but still keep running
 */
public class RotationCheck implements Healthcheck  {

  private final RotationStatus rotationStatus;

  public RotationCheck(RotationStatus rotationStatus) {
    this.rotationStatus = rotationStatus;
  }

  @Override
  public HealthcheckStatus check() {
    return (rotationStatus.status())
        ? HealthcheckStatus.healthy
        : HealthcheckStatus.unhealthy;
  }
}
