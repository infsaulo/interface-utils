package tools.utils.gcs;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageScopes;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.collect.Lists;

public class GcsInterface {

  private static final Logger LOGGER = Logger.getLogger(GcsInterface.class.getName());

  private Storage storage;

  public GcsInterface(final String accountId, final String keyFilename, final String keyPass) {

    try {

      storage = setupStorage(accountId, keyFilename, keyPass);
    } catch (final GeneralSecurityException | IOException e) {

      LOGGER.log(Level.SEVERE, e.toString(), e);
    }
  }

  public void writeFile(final String bucketName, final String objName, final byte[] content,
      final String contentType) {

    try {

      final StorageObject objStorage = new StorageObject();
      objStorage.setBucket(bucketName);
      objStorage.setContentEncoding("UTF-8");
      objStorage.setContentType(contentType);
      objStorage.setName(objName);

      final InputStreamContent contentStream =
          new InputStreamContent(contentType, new ByteArrayInputStream(content));
      storage.objects().insert(bucketName, objStorage, contentStream).execute();

    } catch (final IOException e) {

      LOGGER.log(Level.SEVERE, e.toString(), e);
    }
  }

  public void deleteFile(final String bucketName, final String objName) {

    try {

      storage.objects().delete(bucketName, objName).execute();
    } catch (final IOException e) {

      LOGGER.log(Level.SEVERE, e.toString(), e);
    }
  }

  public byte[] readFile(final String bucketName, final String objName) {

    final ByteArrayOutputStream output = new ByteArrayOutputStream();

    try {

      storage.objects().get(bucketName, objName).executeMediaAndDownloadTo(output);

      final byte[] fileContent = output.toByteArray();

      return fileContent;
    } catch (final IOException e) {

      LOGGER.log(Level.SEVERE, e.toString(), e);
    }

    return new byte[0];
  }

  private Storage setupStorage(final String gcsAccountId, final String keyFilename,
      final String keyPass) throws GeneralSecurityException, IOException {

    final KeyStore keyStore = KeyStore.getInstance("PKCS12");
    keyStore.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(keyFilename),
        keyPass.toCharArray());
    final PrivateKey key = (PrivateKey) keyStore.getKey("privatekey", keyPass.toCharArray());
    final GoogleCredential credential =
        new GoogleCredential.Builder().setTransport(new NetHttpTransport())
            .setJsonFactory(new JacksonFactory()).setServiceAccountId(gcsAccountId)
            .setServiceAccountScopes(Lists.newArrayList(StorageScopes.DEVSTORAGE_FULL_CONTROL))
            .setServiceAccountPrivateKey(key).build();

    return new Storage.Builder(new NetHttpTransport(), new JacksonFactory(), credential)
        .setApplicationName("Generic Pipeline").build();
  }
}
