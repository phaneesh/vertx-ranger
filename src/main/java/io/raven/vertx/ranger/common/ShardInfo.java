package io.raven.vertx.ranger.common;

import com.google.common.base.Strings;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.Iterator;
import java.util.NoSuchElementException;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@EqualsAndHashCode
@ToString
@Slf4j
public class ShardInfo implements Iterable<ShardInfo> {
  private static final String SEPARATOR = ".";

  private String environment;

  @Override
  public Iterator<ShardInfo> iterator() {
    return new ShardInfoIterator(environment);
  }

  public static final class ShardInfoIterator implements Iterator<ShardInfo> {

    private String remainingEnvironment;

    public ShardInfoIterator(String remainingEnvironment) {
      this.remainingEnvironment = remainingEnvironment;
    }

    @Override
    public boolean hasNext() {
      return !Strings.isNullOrEmpty(remainingEnvironment);
    }

    @Override
    public ShardInfo next() {
      if(!hasNext()) {
        throw new NoSuchElementException();
      }
      if(log.isDebugEnabled()) {
        log.debug("Effective environment for discovery is {}", remainingEnvironment);
      }
      val shardInfo = new ShardInfo(remainingEnvironment);
      val sepIndex = remainingEnvironment.indexOf(SEPARATOR);
      remainingEnvironment = sepIndex < 0 ? "" : remainingEnvironment.substring(0, sepIndex);
      return shardInfo;
    }
  }
}
