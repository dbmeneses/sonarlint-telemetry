package meneses.kibana;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * Improve performance reducing contention by using a BlockingQueue by putting and taking batches of items
 */
public class Queue {
  private static final int SIZE = 1_000;
  private LinkedBlockingQueue<String[]> queue = new LinkedBlockingQueue<>(100);
  private String[] writeBuffer = new String[SIZE];
  private String[] readBuffer;

  private int readIdx = 0;
  private int writeIdx = 0;

  public String take() throws InterruptedException {
    if (readBuffer == null || readIdx == readBuffer.length) {
      readBuffer = queue.take();
      readIdx = 0;
    }
    return readBuffer[readIdx++];
  }

  public void put(String line) throws InterruptedException {
    writeBuffer[writeIdx] = line;
    writeIdx++;

    if (writeIdx == SIZE) {
      queue.put(writeBuffer);
      writeBuffer = new String[SIZE];
      writeIdx = 0;
    }
  }

  public void close() throws InterruptedException {
    queue.put(writeBuffer);
    queue.put(new String[1]);
  }
}
