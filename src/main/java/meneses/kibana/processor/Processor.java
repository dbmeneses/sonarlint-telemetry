package meneses.kibana.processor;

import java.time.OffsetDateTime;

public interface Processor {
  void before();

  void process(OffsetDateTime installTime, OffsetDateTime systemTime, String[] fields);

  void after();
}
