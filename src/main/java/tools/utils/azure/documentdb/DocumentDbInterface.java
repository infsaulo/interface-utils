package tools.utils.azure.documentdb;

import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.FeedOptions;
import com.wizzardo.tools.json.JsonObject;
import com.wizzardo.tools.json.JsonTools;

import java.util.List;
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

  public JsonObject[] queryCollection(final String query) {

    FeedOptions options = new FeedOptions();
    options.setEnableCrossPartitionQuery(true);
    List<Document> retrievedDocs = documentClient.queryDocuments(collection.getSelfLink(), query,
                                                                 options).getQueryIterable()
        .toList();

    JsonObject[] retrievedObjs = new JsonObject[retrievedDocs.size()];

    int index = 0;
    for (Document doc : retrievedDocs) {

      retrievedObjs[index++] = JsonTools.parse(doc.toString()).asJsonObject();
    }

    return retrievedObjs;
  }
}
