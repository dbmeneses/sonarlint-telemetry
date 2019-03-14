package meneses.kibana.processor;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.text.NumberFormat;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

public class PerformanceProcessor implements Processor {
  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance(Locale.US);

  private final Map<UserLang, RatePerDuration> durationsPerUserLang = new HashMap<>();
  private final Map<String, Set<OffsetDateTime>> uniqueUsersPerLang = new HashMap<>();
  private final RetentionProcessor retention;

  public PerformanceProcessor(RetentionProcessor retention) {
    this.retention = retention;
  }

  @Override public void before() {

  }

  @Override public void process(OffsetDateTime installTime, OffsetDateTime systemTime, String[] fields) {
    String json = fields[1];
    if (json.isEmpty()) {
      return;
    }

    List<RatePerDuration> durations = parse(json);

    for (RatePerDuration rpd : durations) {
      uniqueUsersPerLang.computeIfAbsent(rpd.language, k -> new HashSet<>()).add(installTime);
      UserLang userLang = new UserLang(installTime, rpd.language);
      durationsPerUserLang.compute(userLang, (k, v) -> v != null ? sum(v, rpd) : rpd);
    }
  }

  @Override public void after() {
    System.out.println("Unique Users per Language");
    for (Map.Entry<String, Set<OffsetDateTime>> e : uniqueUsersPerLang.entrySet()) {
      System.out.println(String.format("%10s %10s", e.getKey(), NUMBER_FORMAT.format(e.getValue().size())));
    }

    System.out.println("Processing performance stats");

    Map<String, RatePerDuration> sumPerLang = new HashMap();
    Map<String, RatePerDuration> percentil = new HashMap<>();
    Map<Integer, Integer> perfByMonth = new TreeMap<>();
    Map<Integer, Integer> countByMonth = new TreeMap<>();

    for (Map.Entry<UserLang, RatePerDuration> e : durationsPerUserLang.entrySet()) {
      double total = Arrays.stream(e.getValue().levels).sum();
      double sum = 0.0;
      double reference = 0.85 * total;
      int perfLevel = 0;

      sumPerLang.compute(e.getKey().language, (k, v) -> v == null ? e.getValue() : sum(v, e.getValue()));

      for (; perfLevel < e.getValue().levels.length; perfLevel++) {
        sum += e.getValue().levels[perfLevel];
        if (sum >= reference) {
          percentil.computeIfAbsent(e.getKey().language, k -> new RatePerDuration()).levels[perfLevel]++;
          break;
        }
      }

      if ("java".equals(e.getValue().language)) {
        int l = perfLevel;
        Integer months = retention.retentionByUser.get(e.getKey().user);
        if (months != null) {
          countByMonth.compute(months, (k, v) -> v == null ? 1 : v + 1);
          perfByMonth.compute(months, (k, v) -> v == null ? l : v + l);
        }
      }
    }

    System.out.println("JAVA performance vs months of usage");
    for (Map.Entry<Integer, Integer> f : countByMonth.entrySet()) {
      System.out.println(String.format("%8s %8s %3.2f", f.getKey(), f.getValue(), ((double) perfByMonth.get(f.getKey())) / f.getValue()));
    }

    for (Map.Entry<String, RatePerDuration> e : percentil.entrySet()) {
      System.out.println("LANG: " + e.getKey());
      double totalPerc = Arrays.stream(e.getValue().levels).sum();
      double totalSum = Arrays.stream(sumPerLang.get(e.getKey()).levels).sum();

      for (PERF_LEVEL level : PERF_LEVEL.values()) {
        System.out.println(String.format("   %10s %10s %5s%% %10s %5s%%", level.label,
          NUMBER_FORMAT.format(e.getValue().levels[level.level]),
          NUMBER_FORMAT.format(Math.round(100.0 * e.getValue().levels[level.level] / totalPerc)),
          NUMBER_FORMAT.format(sumPerLang.get(e.getKey()).levels[level.level]),
          NUMBER_FORMAT.format(Math.round(100.0 * sumPerLang.get(e.getKey()).levels[level.level] / totalSum))));
      }
    }
  }

  private List<RatePerDuration> parse(String field) {
    List<RatePerDuration> list = new ArrayList<>();
    JsonArray root = new JsonParser().parse(field).getAsJsonArray();
    for (JsonElement el : root) {
      RatePerDuration rpd = new RatePerDuration();
      JsonObject obj = el.getAsJsonObject();
      rpd.language = obj.get("language").getAsString();
      JsonObject rates = obj.getAsJsonObject("rate_per_duration");
      for (PERF_LEVEL level : PERF_LEVEL.values()) {
        rpd.levels[level.level] = getAsDouble(rates, level.label);
      }
      list.add(rpd);
    }

    return list;
  }

  private double getAsDouble(JsonObject obj, String field) {
    JsonElement el = obj.get(field);
    return el == null ? 0.0 : el.getAsDouble();
  }

  private RatePerDuration sum(RatePerDuration r1, RatePerDuration r2) {
    for (int i = 0; i < r1.levels.length; i++) {
      r1.levels[i] += r2.levels[i];
    }
    return r1;
  }

  private class RatePerDuration {
    String language;
    double[] levels = new double[PERF_LEVEL.values().length];
  }

  private static class UserLang {
    private OffsetDateTime user;
    private String language;

    public UserLang(OffsetDateTime user, String language) {
      this.user = user;
      this.language = language;
    }

    @Override public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;
      UserLang userLang = (UserLang) o;
      return Objects.equals(user, userLang.user) &&
        Objects.equals(language, userLang.language);
    }

    @Override public int hashCode() {
      return Objects.hash(user, language);
    }

    @Override
    public String toString() {
      return user + "-" + language;
    }
  }

  private enum PERF_LEVEL {
    FAST(0, "0-300"),
    GOOD(1, "300-500"),
    ACCEPTABLE(2, "500-1000"),
    POOR(3, "1000-2000"),
    SLOW(4, "2000-4000"),
    TERRIBLE(5, "4000+");

    private int level;
    private String label;

    private PERF_LEVEL(int level, String label) {
      this.level = level;
      this.label = label;
    }
  }

}
