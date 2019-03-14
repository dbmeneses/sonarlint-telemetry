package meneses.kibana;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.elasticsearch.search.SearchHit;

public class Save {
  private static final String[] FIELDS = {"install_time", "analyses", "connected_mode_used", "system_time", "sonarlint_version",
    "sonarlint_product", "timestamp", "connected_mode_used", "connected_mode_sonarcloud", "type", "days_of_use", "days_since_installation"};

  static final String FILE_PATH = "/home/meneses/inactive-sonarlint-telemetry-2019.csv";
  static final String SEPARATOR = ";";

  private Writer writer;

  private int linesProcessed = 0;
  private int linesSkipped = 0;

  public void before() throws IOException {
    writer = new BufferedWriter(Files.newBufferedWriter(Paths.get(FILE_PATH), StandardCharsets.UTF_8));
    writeHeader(writer);
  }

  public void processLine(SearchHit hit) throws IOException {
    linesProcessed++;
    Map<String, Object> fields = hit.getSourceAsMap();

    if (!writeLine(writer, fields)) {
      linesSkipped++;
    }

    if (linesProcessed % 100000 == 0) {
      System.out.println("Lines processed: " + linesProcessed + ", skipped: " + linesSkipped);
    }
  }

  public void after() throws IOException {
    writer.close();
  }

  private static void writeHeader(Writer writer) throws IOException {
    writer.write(String.join(SEPARATOR, FIELDS));
    writer.write('\n');
  }

  private static boolean writeLine(Writer writer, Map<String, Object> fields) throws IOException {
    List<String> values = new ArrayList<>(FIELDS.length);
    for (String f : FIELDS) {
      Object v = fields.get(f);
      if (v == null) {
        values.add("");
      } else {
        values.add(v.toString());
      }
    }

    writer.write(String.join(SEPARATOR, values));
    writer.write('\n');
    return true;
  }
}
