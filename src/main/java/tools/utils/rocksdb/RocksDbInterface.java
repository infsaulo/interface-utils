package tools.utils.rocksdb;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.logging.Level;
import java.util.logging.Logger;

public class RocksDbInterface {

    private static final Logger LOGGER = Logger.getLogger(RocksDbInterface.class.getName());

    static {

        RocksDB.loadLibrary();
    }

    private RocksDB rocksDB;
    private final Options opts;

    public RocksDbInterface(final String dbPath) {

        opts = new Options();
        opts.setCreateIfMissing(true);

        try {

            rocksDB = RocksDB.open(opts, dbPath);
        } catch (RocksDBException ex) {

            rocksDB = null;

            LOGGER.log(Level.SEVERE, ex.toString(), ex);
        }
    }

    public void close() {

        if (rocksDB != null) {

            rocksDB.close();
        }

        opts.close();
    }

    public byte[] getValue(final byte[] key) {

        byte[] value;

        try {

            value = rocksDB.get(key);

            if (value == null) {

                value = new byte[0];
            }
        } catch (RocksDBException ex) {

            LOGGER.log(Level.SEVERE, ex.toString(), ex);
            value = new byte[0];
        }

        return value;
    }

    // return true if put with success, otherwise false
    public boolean setKeyValue(final byte[] key, final byte[] value) {

        boolean status = false;
        try {

            rocksDB.put(key, value);
            status = true;
        } catch (RocksDBException ex) {

            LOGGER.log(Level.SEVERE, ex.toString(), ex);

        }

        return status;
    }

    public boolean deleteKey(final byte[] key) {

        boolean status = false;
        try {

            rocksDB.delete(key);
            status = true;
        } catch (RocksDBException ex) {

            LOGGER.log(Level.SEVERE, ex.toString(), ex);
        }

        return status;
    }
}
