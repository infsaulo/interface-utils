package tools.utils.gcs;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import com.google.api.client.util.Charsets;

public class GcsInterfaceTest {

  static final String bucketname = System.getenv("GCS_BUCKETNAME");
  static final String keyFilename = System.getenv("GCS_KEYFILE");
  static final String keyPass = System.getenv("GCS_KEYPASS");
  static final String accountId = System.getenv("GCS_ACCOUNTID");
  static final String filename = "test.csv";
  static final GcsInterface writter = new GcsInterface(accountId, keyFilename, keyPass);

  @Test
  public void testWriteGcsFile() {

    writter.writeFile(bucketname, filename,
        "col1row1,col2row1,col3row1\ncol1row2,col2row2,col3row2".getBytes(Charsets.UTF_8),
        "text/csv");

    final String fileContent = new String(writter.readFile(bucketname, filename), Charsets.UTF_8);
    final String[] lines = fileContent.split("\n");

    Assert.assertTrue(lines.length == 2);
  }

  @AfterClass
  public static void teardDownClass() {

    writter.deleteFile(bucketname, filename);
  }

}
