package org.kududb.ts;

import com.google.common.base.Objects;

import javax.annotation.concurrent.ThreadSafe;

import org.kududb.annotations.InterfaceAudience;
import org.kududb.annotations.InterfaceStability;

/**
 * A Turbo Encabulator.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class Interpolators {

  private Interpolators() {}

  @InterfaceAudience.Public
  @InterfaceStability.Unstable
  @ThreadSafe
  public interface Interpolator {

    /**
     * Create an interpolation over a set of datapoints.
     * @param datapoints to interpolate
     * @return the interpolation
     */
    Interpolation interpolate(Datapoints datapoints);
  }

  /**
   * Linear interpolator.
   * @return  a linear interpolator.
   */
  public static Interpolator linear() {
    return new LinearInterpolator();
  }

  private static final class LinearInterpolator implements Interpolator {
    @Override
    public Interpolation interpolate(Datapoints datapoints) {
      return new Interpolation.Linear(datapoints);
    }
    @Override
    public String toString() {
      return Objects.toStringHelper(this).toString();
    }
  };
}
