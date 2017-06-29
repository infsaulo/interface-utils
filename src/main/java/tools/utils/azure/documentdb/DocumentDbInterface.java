package tools.utils.azure.documentdb;

import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.FeedOptions;
import com.microsoft.azure.documentdb.PartitionKey;
import com.microsoft.azure.documentdb.RequestOptions;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DocumentDbInterface {

  private static final Logger LOGGER = Logger.getLogger(DocumentDbInterface.class.getName());

  private final DocumentClient documentClient;

  private final Database database;

  private final DocumentCollection collection;

  public DocumentDbInterface(final String endpoint, final String key, final String databaseId,
                             final String collectionId) {

    documentClient = new DocumentClient(endpoint, key, ConnectionPolicy.GetDefault(),
                                        ConsistencyLevel.Session);

    database = documentClient.queryDatabases("SELECT * FROM root r WHERE r.id ='" + databaseId
                                             + "'", null).getQueryIterable().toList().get(0);

    collection = documentClient.queryCollections(database.getSelfLink(),
                                                 "SELECT * FROM root r " + "WHERE r.id='"
                                                 + collectionId + "'", null).getQueryIterable()
        .toList().get(0);
  }

  public List<Document> queryCollection(final String query) {

    FeedOptions options = new FeedOptions();
    options.setEnableCrossPartitionQuery(true);
    List<Document> retrievedDocs = documentClient.queryDocuments(collection.getSelfLink(), query,
                                                                 options).getQueryIterable()
        .toList();

    return retrievedDocs;
  }

  public void replaceDocument(final Document document, final Object key) {

    try {

      PartitionKey partitionKey = new PartitionKey(key);
      RequestOptions options = new RequestOptions();
      options.setPartitionKey(partitionKey);
      documentClient.replaceDocument(document, options);

    } catch (DocumentClientException e) {

      LOGGER.log(Level.SEVERE, e.toString(), e);
    }
  }
}
