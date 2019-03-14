package meneses.kibana.processor;

import java.text.NumberFormat;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

public class RetentionProcessor implements Processor {
  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance(Locale.US);

  final Map<OffsetDateTime, OffsetDateTime> lastUsePerUser = new HashMap<>();
  final Map<OffsetDateTime, OffsetDateTime> firstUsePerUser = new HashMap<>();

  @Override
  public void before() {

  }

  @Override
  public void process(OffsetDateTime installTime, OffsetDateTime systemTime, String[] fields) {
    if (installTime != null) {
      // Last use per user
      lastUsePerUser.compute(installTime, (k, v) -> {
        if (v == null) {
          return systemTime;
        }
        return systemTime.isAfter(v) ? systemTime : v;
      });

      firstUsePerUser.compute(installTime, (k, v) -> {
        if (v == null) {
          return systemTime;
        }
        return systemTime.isBefore(v) ? systemTime : v;
      });
    }
  }

  Map<OffsetDateTime, Integer> retentionByUser = new HashMap<>();

  @Override
  public void after() {
    System.out.println("\nComputing retention");
    TreeMap<Integer, Integer> retention = new TreeMap<>();
    TreeMap<Integer, Integer> aggregateRetention = new TreeMap<>();
    TreeMap<RETENTION_CATEGORY, Integer> retentionCat = new TreeMap<>();

    for (Map.Entry<OffsetDateTime, OffsetDateTime> e : lastUsePerUser.entrySet()) {
      // too recent installs
      if (e.getKey().isAfter(OffsetDateTime.of(2019, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC))) {
        continue;
      }

      // still using it
      if (e.getValue().isAfter(OffsetDateTime.of(2019, 2, 1, 0, 0, 0, 0, ZoneOffset.UTC))) {
        retention.compute(-1, (k, v) -> v == null ? 1 : v + 1);
        retentionByUser.put(e.getKey(), -1);
        continue;
      }

      // installs before November and stopped using at some point
      int months = (int) e.getKey().until(e.getValue(), ChronoUnit.MONTHS);
      retention.compute(months, (k, v) -> v == null ? 1 : v + 1);
      retentionByUser.put(e.getKey(), months);

      if (months == 0) {
        int days = (int) e.getKey().until(e.getValue(), ChronoUnit.DAYS);
        for (RETENTION_CATEGORY c : RETENTION_CATEGORY.values()) {
          if (days <= c.rangeEnd) {
            retentionCat.compute(c, (k, v) -> v == null ? 1 : v + 1);
            break;
          }
        }
      }

    }

    List<Integer> keys = new ArrayList<>(retention.descendingKeySet());
    int aggregate = 0;
    for (Integer k : keys) {
      aggregate += retention.get(k);
      aggregateRetention.put(k, aggregate);
    }

    for (Map.Entry<Integer, Integer> e : retention.entrySet()) {
      System.out.println(String.format("%3s %5s %s",
        NUMBER_FORMAT.format(e.getKey()),
        NUMBER_FORMAT.format(e.getValue()),
        NUMBER_FORMAT.format(aggregateRetention.get(e.getKey()))));
    }

    System.out.println("\nFor users that used for less than a month, days using it:");
    for (RETENTION_CATEGORY c : RETENTION_CATEGORY.values()) {
      System.out.println(String.format("%10s %8s", c.label, retentionCat.getOrDefault(c, 0)));
    }

  }

  private enum RETENTION_CATEGORY {
    C1(1, "1"),
    C2_5(5, "2-5"),
    C6_15(15, "6-15"),
    C15_PLUS(Integer.MAX_VALUE, "15+");

    private int rangeEnd;
    private String label;

    private RETENTION_CATEGORY(int rangeEnd, String label) {
      this.rangeEnd = rangeEnd;
      this.label = label;
    }
  }

}
