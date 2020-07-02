package tools.utils.rocksdb;

import org.apache.commons.io.Charsets;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;

public class RocksDbInterfaceTest {

    private static RocksDbInterface rocksInterface;

    private static final String DB_PATH = "./rocksDBInterfaceTestDir";

    @BeforeClass
    public static void setUp() {

        rocksInterface = new RocksDbInterface(DB_PATH);
    }

    @AfterClass
    public static void cleanUp() {

        rocksInterface.close();

        deleteDir(new File(DB_PATH));
    }

    private static void deleteDir(File file) {

        final File[] contents = file.listFiles();

        if (contents != null) {

            for (final File f : contents) {

                if (!Files.isSymbolicLink(f.toPath())) {

                    deleteDir(f);
                }
            }
        }

        boolean status = file.delete();
    }

    @Test
    public void testGetAndSetKeyValue() {

        final boolean result =
                rocksInterface.setKeyValue("testKey".getBytes(Charsets.UTF_8), "testValue".getBytes(Charsets.UTF_8));
        Assert.assertTrue(result);

        final byte[] value = rocksInterface.getValue("testKey".getBytes(Charsets.UTF_8));

        Assert.assertEquals("testValue", new String(value, Charsets.UTF_8));
    }

    @Test
    public void testDeleteKey() {

        boolean result =
                rocksInterface.setKeyValue("testKey2".getBytes(Charsets.UTF_8), "testValue2".getBytes(Charsets.UTF_8));
        Assert.assertTrue(result);

        byte[] value = rocksInterface.getValue("testKey2".getBytes(Charsets.UTF_8));

        Assert.assertEquals("testValue2", new String(value, Charsets.UTF_8));

        result = rocksInterface.deleteKey("testKey2".getBytes(Charsets.UTF_8));
        Assert.assertTrue(result);

        value = rocksInterface.getValue("testKey2".getBytes(Charsets.UTF_8));
        Assert.assertEquals(0, value.length);
    }
}
