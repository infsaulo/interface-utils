package tools.utils.azure.storage;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.file.CloudFile;
import com.microsoft.azure.storage.file.CloudFileClient;
import com.microsoft.azure.storage.file.CloudFileDirectory;
import com.microsoft.azure.storage.file.CloudFileShare;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class AzureStorageInterface {

  private static final Logger LOGGER = Logger.getLogger(AzureStorageInterface.class.getName());

  final String connectionUrl;

  CloudStorageAccount storageAccount = null;

  CloudFileClient client = null;

  CloudFileShare share = null;

  CloudFileDirectory rootDir = null;

  public AzureStorageInterface(String accountName, String accountKey, String shareName) {

    connectionUrl = String.format("DefaultEndpointsProtocol=http;AccountName=%s;"
                                            + "AccountKey=%s", accountName, accountKey);

    try {

      storageAccount = CloudStorageAccount.parse(connectionUrl);
      client = storageAccount.createCloudFileClient();
      share = client.getShareReference(shareName);
      rootDir = share.getRootDirectoryReference();
    } catch (InvalidKeyException | URISyntaxException | StorageException e) {

      LOGGER.log(Level.SEVERE, e.toString(), e);
    }
  }

  public String readFile(String dirname, String filename) {

    try {
      final CloudFileDirectory dir = rootDir.getDirectoryReference(dirname);

      final CloudFile file = dir.getFileReference(filename);

      final String content = file.downloadText();

      return content;
    } catch (URISyntaxException | StorageException | IOException e) {

      LOGGER.log(Level.SEVERE, e.toString(), e);
    }

    return null;
  }

  public void writeFile(String dirname, String filename, String content) {

    try {

      final CloudFileDirectory dir = rootDir.getDirectoryReference(dirname);
      final CloudFile file = dir.getFileReference(filename);
      file.uploadText(content);
    } catch (StorageException | URISyntaxException | IOException e) {

      LOGGER.log(Level.SEVERE, e.toString(), e);
    }
  }
}
