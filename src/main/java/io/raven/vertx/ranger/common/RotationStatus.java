package io.raven.vertx.ranger.common;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Current rotation status
 */
public class RotationStatus {

  private AtomicBoolean inRotation;

  public RotationStatus(boolean initialStatus) {
    inRotation = new AtomicBoolean(initialStatus);
  }

  public void oor() {
    inRotation.set(false);
  }

  public void bir() {
    inRotation.set(true);
  }

  public boolean status() {
    return inRotation.get();
  }
}
