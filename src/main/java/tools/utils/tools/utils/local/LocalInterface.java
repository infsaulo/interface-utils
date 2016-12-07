package tools.utils.tools.utils.local;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class LocalInterface {

  private static final Logger LOGGER = Logger.getLogger(LocalInterface.class.getName());

  private LocalInterface() {

  }

  public static void appendToFile(String filePath, String content) throws IOException {

    Writer writer = null;

    try {

      writer = new OutputStreamWriter(new FileOutputStream(filePath, true), StandardCharsets.UTF_8);
      writer.write(content);
    } catch (IOException e) {

      LOGGER.log(Level.SEVERE, e.toString(), e);
    } finally {
      if (writer != null) {
        writer.close();
      }
    }
  }

  public static void writeToFile(String filePath, String content) throws IOException {

    Writer writer = null;
    try {

      writer = new OutputStreamWriter(new FileOutputStream(filePath, false), StandardCharsets
          .UTF_8);
      writer.write(content);

    } catch (IOException e) {

      LOGGER.log(Level.SEVERE, e.toString(), e);
    } finally {
      if (writer != null) {
        writer.close();
      }
    }
  }

}
