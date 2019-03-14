package meneses.kibana;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.NumberFormat;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import meneses.kibana.processor.PerformanceProcessor;
import meneses.kibana.processor.Processor;
import meneses.kibana.processor.RetentionProcessor;
import meneses.kibana.processor.UsersProcessor;

import static meneses.kibana.Save.SEPARATOR;

public class Load {
  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance(Locale.US);
  private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_DATE_TIME;
  private static final String[] FILE_PATHS = {"/home/meneses/inactive-sonarlint-telemetry-2019.csv", "/home/meneses/telemetry-sonarlint.csv"};

  public static void main(String[] args) throws InterruptedException {
    long start = System.currentTimeMillis();
    List<Path> paths = Arrays.stream(FILE_PATHS).map(Paths::get).collect(Collectors.toList());
    new Load().work(createProcessors(), paths);
    System.out.println(System.currentTimeMillis() - start + " ms");
  }

  private static List<Processor> createProcessors() {
    RetentionProcessor retention = new RetentionProcessor();
    return Arrays.asList(retention, new UsersProcessor(retention), new PerformanceProcessor(retention));
  }

  private Queue queue = new Queue();

  public void work(List<Processor> processors, List<Path> filePaths) throws InterruptedException {
    RunnableExceptionWrapper producer = new RunnableExceptionWrapper(() -> load(filePaths));
    Thread t1 = new Thread(producer, "file-reader");
    t1.start();
    consume(processors);
    t1.join();
  }

  public void load(List<Path> filePaths) throws IOException, InterruptedException {

    for (Path filePath : filePaths) {
      System.out.println("Reading " + filePath);

      try (BufferedReader reader = Files.newBufferedReader(filePath, StandardCharsets.UTF_8)) {
        // skip header
        reader.readLine();

        String line = reader.readLine();
        while (line != null) {
          queue.put(line);
          line = reader.readLine();
        }
      }
    }
    queue.close();
  }

  private void consume(List<Processor> processors) throws InterruptedException {
    int count = 0;
    int installMissing = 0;
    int invalid = 0;
    String invalidLine = null;

    processors.forEach(Processor::before);

    while (true) {
      String line = queue.take();
      if (line == null) {
        break;
      }

      boolean process = true;

      String[] fields = line.split(SEPARATOR);
      if (fields.length == 0) {
        continue;
      }

      OffsetDateTime installTime;
      OffsetDateTime systemTime = OffsetDateTime.parse(fields[6], FORMATTER);

      if (fields[0].isEmpty()) {
        installMissing++;
        installTime = null;
      } else {
        installTime = OffsetDateTime.parse(fields[0], FORMATTER);

        if (installTime.getYear() < 2017 || installTime.getYear() > 2019 || systemTime.getYear() < 2017 || systemTime.getYear() > 2019) {
          invalid++;
          invalidLine = line;
          process = false;
        }

        if (installTime.until(systemTime, ChronoUnit.DAYS) < -3) {
          invalid++;
          invalidLine = line;
          process = false;
        }
      }

      if (process) {
        processors.forEach(p -> p.process(installTime, systemTime, fields));
      }

      count++;
      if (count % 100000 == 0) {
        System.out.println("  Processed " + NUMBER_FORMAT.format(count)
          + ", Skipped: " + NUMBER_FORMAT.format(installMissing)
          + ", Invalid: " + NUMBER_FORMAT.format(invalid)
          + ", Example invalid: " + invalidLine);
      }
    }

    for (Processor p : processors) {
      System.out.println("\n############################");
      p.after();
    }
  }

  public interface RunnableException {
    void run() throws Exception;
  }

  public class RunnableExceptionWrapper implements Runnable {
    private final RunnableException r;

    public RunnableExceptionWrapper(RunnableException r) {
      this.r = r;
    }

    @Override
    public void run() {
      try {
        r.run();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
