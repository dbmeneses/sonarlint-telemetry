package meneses.kibana.processor;

import java.text.NumberFormat;
import java.time.OffsetDateTime;
import java.time.YearMonth;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class UsersProcessor implements Processor {
  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance(Locale.US);

  private final Map<OffsetDateTime, Integer> users = new TreeMap<>();
  private final Map<YearMonth, Map<String, Integer>> pingsByMonthAndProduct = new TreeMap<>();
  private final Map<YearMonth, Set<OffsetDateTime>> uniqueByMonth = new HashMap<>();
  private final Map<YearMonth, Map<OffsetDateTime, Integer>> countPerUserAndMonth = new HashMap<>();

  private final RetentionProcessor retention;

  public UsersProcessor(RetentionProcessor retention) {
    this.retention = retention;
  }

  @Override public void before() {

  }

  @Override public void process(OffsetDateTime installTime, OffsetDateTime systemTime, String[] fields) {
    YearMonth systemYearMonth = YearMonth.from(systemTime);
    String product = fields[5];
    pingsByMonthAndProduct.computeIfAbsent(systemYearMonth, k -> new HashMap<>()).compute(product, (k, v) -> v == null ? 1 : v + 1);

    if (installTime != null) {
      uniqueByMonth.compute(systemYearMonth, (k, v) -> {
        if (v == null) {
          v = new HashSet<>();
        }
        v.add(installTime);
        return v;
      });

      // Unique users
      users.compute(installTime, (k, v) -> v == null ? 1 : v + 1);
      countPerUserAndMonth.computeIfAbsent(systemYearMonth, k -> new HashMap<>()).compute(installTime, (k, v) -> v == null ? 1 : v + 1);
    }
  }

  @Override public void after() {
    Map<YearMonth, Integer> lastUsePerMonth = new HashMap<>();
    Map<YearMonth, Integer> firstUsePerMonth = new HashMap<>();

    for (Map.Entry<OffsetDateTime, OffsetDateTime> e : retention.lastUsePerUser.entrySet()) {
      lastUsePerMonth.compute(YearMonth.from(e.getValue()), (k, v) -> v == null ? 1 : v + 1);
    }

    for (Map.Entry<OffsetDateTime, OffsetDateTime> e : retention.firstUsePerUser.entrySet()) {
      firstUsePerMonth.compute(YearMonth.from(e.getValue()), (k, v) -> v == null ? 1 : v + 1);
    }

    System.out.println("Number of users: " + users.size());

    System.out.println("Users per month");
    System.out.println(String.format("%12s%12s%12s%12s%12s%12s%12s%12s%12s",
      "Month", "Pings", "Pings SLI", "Pings SLE", "Pings VS", "Pings VSCode", "Unique Users", "New Users", "Users Dropping"));
    for (YearMonth yearMonth : pingsByMonthAndProduct.keySet()) {
      System.out.println(String.format("%12s%12s%12s%12s%12s%12s%12s%12s%12s",
        yearMonth,
        NUMBER_FORMAT.format(pingsByMonthAndProduct.get(yearMonth).values().stream().mapToInt(x -> x).sum()),
        NUMBER_FORMAT.format(pingsByMonthAndProduct.get(yearMonth).getOrDefault("SonarLint IntelliJ", 0)),
        NUMBER_FORMAT.format(pingsByMonthAndProduct.get(yearMonth).getOrDefault("SonarLint Eclipse", 0)),
        NUMBER_FORMAT.format(pingsByMonthAndProduct.get(yearMonth).getOrDefault("SonarLint Visual Studio", 0)),
        NUMBER_FORMAT.format(pingsByMonthAndProduct.get(yearMonth).getOrDefault("SonarLint VSCode", 0)),
        NUMBER_FORMAT.format(uniqueByMonth.getOrDefault(yearMonth, Collections.emptySet()).size()),
        NUMBER_FORMAT.format(firstUsePerMonth.getOrDefault(yearMonth, 0)),
        NUMBER_FORMAT.format(lastUsePerMonth.getOrDefault(yearMonth, 0))));
    }

    Map<YearMonth, Map<USE_CATEGORY, Integer>> countPerCategoryAndMonth = new TreeMap<>();

    for (Map.Entry<YearMonth, Map<OffsetDateTime, Integer>> x : countPerUserAndMonth.entrySet()) {
      YearMonth yearMonth = x.getKey();
      for (Map.Entry<OffsetDateTime, Integer> e : x.getValue().entrySet()) {
        for (USE_CATEGORY c : USE_CATEGORY.values()) {
          if (e.getValue() <= c.rangeEnd) {
            countPerCategoryAndMonth.computeIfAbsent(yearMonth, k -> new TreeMap<>()).compute(c, (k,v) -> v == null ? 1 : v+1);
            break;
          }
        }
      }
    }

    System.out.println("Days using SonarLint per user");
    System.out.print("Months    ");
    for (USE_CATEGORY u : USE_CATEGORY.values()) {
      System.out.print(String.format("%10s", u.label));
    }
    System.out.println("");

    for (Map.Entry<YearMonth, Map<USE_CATEGORY, Integer>> e : countPerCategoryAndMonth.entrySet()) {
      System.out.print(String.format("%10s", e.getKey()));
      for (USE_CATEGORY u : USE_CATEGORY.values()) {
        System.out.print(String.format("%10s", Integer.toString(e.getValue().getOrDefault(u, 0))));
      }
      System.out.println("");
    }

  }

  private enum USE_CATEGORY {
    C1_5(5, "1-5"),
    C6_10(10, "5-10"),
    C11_15(15, "10-15"),
    C16_20(20, "15-20"),
    C21_PLUS(Integer.MAX_VALUE, "20+");

    private int rangeEnd;
    private String label;

    private USE_CATEGORY(int rangeEnd, String label) {
      this.rangeEnd = rangeEnd;
      this.label = label;
    }
  }
}
