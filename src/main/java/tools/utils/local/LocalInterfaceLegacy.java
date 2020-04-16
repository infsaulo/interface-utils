package tools.utils.local;

import com.google.common.base.Charsets;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

@Deprecated
public final class LocalInterfaceLegacy {

    private static final Logger LOGGER = Logger.getLogger(LocalInterfaceLegacy.class.getName());

    private Writer writer;

    private BufferedReader reader;

    private boolean fileExists;

    public LocalInterfaceLegacy(final String filePath) {

        while (!fileExists) {

            try {

                reader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), Charsets.UTF_8));
                fileExists = true;
            } catch (FileNotFoundException ex) {

                LOGGER.log(Level.SEVERE, ex.toString(), ex);
                continue;
            }
        }
    }

    public LocalInterfaceLegacy(final String filePath, final boolean append) throws IOException {

        while (!fileExists) {

            try {

                writer = new OutputStreamWriter(new FileOutputStream(filePath, append), StandardCharsets
                        .UTF_8);
                fileExists = true;
            } catch (IOException e) {

                LOGGER.log(Level.SEVERE, e.toString(), e);
                continue;
            }
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