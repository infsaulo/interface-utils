package tools.utils.azure.storage;

import com.google.api.client.util.Charsets;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudAppendBlob;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.file.CloudFile;
import com.microsoft.azure.storage.file.CloudFileClient;
import com.microsoft.azure.storage.file.CloudFileDirectory;
import com.microsoft.azure.storage.file.CloudFileShare;

import java.io.ByteArrayInputStream;
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

  CloudBlobClient blobClient = null;

  CloudBlobContainer container = null;

  CloudFileShare share = null;

  CloudFileDirectory rootDir = null;

  public AzureStorageInterface(String accountName, String accountKey, String shareName, String
      containerName) {

    connectionUrl = String.format("DefaultEndpointsProtocol=http;AccountName=%s;"
                                  + "AccountKey=%s", accountName, accountKey);

    try {

      storageAccount = CloudStorageAccount.parse(connectionUrl);
      if (shareName != null) {

        client = storageAccount.createCloudFileClient();
        share = client.getShareReference(shareName);
        rootDir = share.getRootDirectoryReference();
      }
      if (containerName != null) {

        blobClient = storageAccount.createCloudBlobClient();
        container = blobClient.getContainerReference(containerName);
      }
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

  public String readBlob(String blobName) {

    try {

      CloudBlockBlob blob = container.getBlockBlobReference(blobName);
      String content = blob.downloadText();

      return content;

    } catch (IOException | URISyntaxException | StorageException e) {

      LOGGER.log(Level.SEVERE, e.toString(), e);
    }
    return null;
  }

  public void writeBlob(String blobName, String content) {

    try {

      CloudBlob blob = container.getBlockBlobReference(blobName);
      byte[] contentBytes = content.getBytes(Charsets.UTF_8);
      blob.upload(new ByteArrayInputStream(contentBytes), contentBytes.length);
    } catch (IOException | URISyntaxException | StorageException e) {

      LOGGER.log(Level.SEVERE, e.toString(), e);
    }
  }

  public void writeBlobFromFile(String blobName, String filePath) {

    try {

      CloudBlob blob = container.getBlockBlobReference(blobName);
      blob.uploadFromFile(filePath);
    } catch (IOException | URISyntaxException | StorageException e) {

      LOGGER.log(Level.SEVERE, e.toString(), e);
    }
  }

  public void appendBlob(String blobName, String content) {

    try {

      CloudAppendBlob blob = container.getAppendBlobReference(blobName);
      blob.appendText(content);
    } catch (URISyntaxException | StorageException | IOException e) {

      LOGGER.log(Level.SEVERE, e.toString(), e);
    }
  }

  public void createAppendBlob(String blobName) {

    try {

      CloudAppendBlob blob = container.getAppendBlobReference(blobName);
      blob.createOrReplace();
    } catch (URISyntaxException | StorageException e) {

      LOGGER.log(Level.SEVERE, e.toString(), e);
    }
  }

  public boolean blobExists(String blobName) {

    try {

      CloudAppendBlob blob = container.getAppendBlobReference(blobName);
      return blob.exists();
    } catch (URISyntaxException | StorageException e) {

      LOGGER.log(Level.SEVERE, e.toString(), e);
    }

    return false;
  }
}
