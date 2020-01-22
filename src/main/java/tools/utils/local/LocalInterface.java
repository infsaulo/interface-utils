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

    private ObjectOutputStream objectWriter;

    private ObjectInputStream objectReader;

    private boolean fileExists;

    private boolean isObject;

    public LocalInterface(final String filePath, final boolean object) {

        try {

            if (object) {

                objectReader = new ObjectInputStream(new FileInputStream(filePath));
                isObject = true;
            } else {

                reader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), Charsets.UTF_8));
                isObject = false;

            }

            fileExists = true;
        } catch (IOException ex) {

            LOGGER.log(Level.SEVERE, ex.toString(), ex);
        }

    }

    public LocalInterface(final String filePath, final boolean append, final boolean object) throws IOException {


        try {

            if (object) {

                objectWriter = new ObjectOutputStream(new FileOutputStream(filePath));
                isObject = true;
            } else {

                writer = new OutputStreamWriter(new FileOutputStream(filePath, append), StandardCharsets
                        .UTF_8);
                isObject = false;

            }
            fileExists = true;
        } catch (IOException e) {

            LOGGER.log(Level.SEVERE, e.toString(), e);
        }
    }

    public Object readLineFile() {

        if (isObject) {

            try {
                final Object obj = objectReader.readObject();

                return obj;
            } catch (IOException ex) {

                LOGGER.log(Level.SEVERE, ex.toString(), ex);

                return null;
            } catch (ClassNotFoundException ex) {

                LOGGER.log(Level.SEVERE, ex.toString(), ex);

                return null;
            }
        } else {

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
    }

    public void writeToFile(Object content) {

        if (isObject) {

            synchronized (objectWriter) {
                try {

                    objectWriter.writeObject(content);
                    objectWriter.flush();
                } catch (IOException e) {

                    LOGGER.log(Level.SEVERE, e.toString(), e);
                }
            }
        } else {

            synchronized (writer) {
                try {

                    writer.write((String) content);
                    writer.flush();
                } catch (IOException e) {

                    LOGGER.log(Level.SEVERE, e.toString(), e);
                }
            }
        }
    }

    public void closeReaderInterface() {

        try {

            if (isObject) {

                objectReader.close();
            } else {
                reader.close();
            }
        } catch (IOException e) {

            LOGGER.log(Level.SEVERE, e.toString(), e);
        }
    }

    public void closeInterface() {

        try {

            if (isObject) {

                objectWriter.close();
            } else {

                writer.close();
            }
        } catch (IOException e) {

            LOGGER.log(Level.SEVERE, e.toString(), e);
        }
    }

    public boolean isFileExists() {

        return fileExists;
    }
}
