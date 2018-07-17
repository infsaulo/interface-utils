package tools.utils.local;

import com.google.common.base.Charsets;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class LocalInterface {

    private static final Logger LOGGER = Logger.getLogger(LocalInterface.class.getName());

    private Writer writer;

    private BufferedReader reader;

    private boolean fileExists;

    public LocalInterface(final String filePath) {

        try {

            reader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), Charsets.UTF_8));
            fileExists = true;
        } catch (FileNotFoundException ex) {

            LOGGER.log(Level.SEVERE, ex.toString(), ex);
        }

    }

    public LocalInterface(final String filePath, final boolean append) throws IOException {

        try {

            writer = new OutputStreamWriter(new FileOutputStream(filePath, append), StandardCharsets
                    .UTF_8);
            fileExists = true;
        } catch (IOException e) {

            LOGGER.log(Level.SEVERE, e.toString(), e);
        }
    }

    public String readLineFile() {

        synchronized (reader) {

            try {

                final String line = reader.readLine();

                return line;
            } catch (IOException ex) {

                LOGGER.log(Level.SEVERE, ex.toString(), ex);

                return null;
            }
        }
    }

    public void writeToFile(String content) {

        synchronized (writer) {
            try {

                writer.write(content);
                writer.flush();
            } catch (IOException e) {

                LOGGER.log(Level.SEVERE, e.toString(), e);
            }
        }
    }

    public void closeReaderInterface() {

        try {

            reader.close();
        } catch (IOException e) {

            LOGGER.log(Level.SEVERE, e.toString(), e);
        }
    }

    public void closeInterface() {

        try {

            writer.close();
        } catch (IOException e) {

            LOGGER.log(Level.SEVERE, e.toString(), e);
        }
    }

    public boolean isFileExists() {

        return fileExists;
    }
}
