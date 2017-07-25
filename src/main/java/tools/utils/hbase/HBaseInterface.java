package tools.utils.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HBaseInterface {

  private static final Logger LOGGER = Logger.getLogger(HBaseInterface.class.getName());


  public static ResultScanner searchByRegex(Table table, String regex) {

    final RegexStringComparator regexComp = new RegexStringComparator(regex);

    final RowFilter filter = new RowFilter(CompareOp.EQUAL, regexComp);

    final Scan rowScan = new Scan();

    rowScan.setFilter(filter);

    final ResultScanner results;

    try {

      results = table.getScanner(rowScan);

      return results;
    } catch (IOException exception) {

      LOGGER.log(Level.SEVERE, exception.toString(), exception);
      return null;
    }
  }

  public static ResultScanner searchByPrefix(Table table, String prefix) {

    final PrefixFilter prefixFilter = new PrefixFilter(Bytes.toBytes(prefix));

    final Scan prefixScan = new Scan();

    prefixScan.setFilter(prefixFilter);

    final ResultScanner results;

    try {

      results = table.getScanner(prefixScan);

      return results;
    } catch (IOException exception) {

      LOGGER.log(Level.SEVERE, exception.toString(), exception);
      return null;
    }
  }

  public static void closeResultScanner(ResultScanner results) {

    results.close();
  }

  public static Connection getConnection(final Map<String, String> configParams) {

    final Configuration conf = HBaseConfiguration.create();

    for (Map.Entry<String, String> param : configParams.entrySet()) {

      conf.set(param.getKey(), param.getValue());
    }

    try {

      final Connection conn = ConnectionFactory.createConnection(conf);

      return conn;
    } catch (IOException exception) {

      LOGGER.log(Level.SEVERE, exception.toString(), exception);

      return null;
    }
  }

  public static Table getTable(final Connection conn, final String tableName) {

    try {

      final Table table = conn.getTable(TableName.valueOf(tableName));

      return table;
    } catch (IOException exception) {

      LOGGER.log(Level.SEVERE, exception.toString(), exception);

      return null;
    }
  }

  public static void closeTable(final Table table) {

    try {

      table.close();
    } catch (IOException exception) {

      LOGGER.log(Level.SEVERE, exception.toString(), exception);
    }
  }

  public static void closeConnection(final Connection conn) {

    try {

      conn.close();
    } catch (IOException exception) {

      LOGGER.log(Level.SEVERE, exception.toString(), exception);
    }
  }
}
