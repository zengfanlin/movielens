import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseTools {

    public static Configuration conf = new Configuration();
    private static Connection conn = null;
    private static ExecutorService pool = Executors.newFixedThreadPool(200);

    static {
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "node21,node22,node23");
        conf.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 120000);
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Table openTable(String tableName) throws IOException {
        if (conn == null) {
            conn = ConnectionFactory.createConnection(conf);
        }
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName), pool);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }

    public static void closeTable(Table table) {
        if (table != null) {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void closeConn() {
        if (conn != null) {
            try {
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void putColumnDatas(Table table, String rowKey,
                                      String familyName, Map<String, String> columnDatas) throws IOException {
        if (conn == null) {
            conn = ConnectionFactory.createConnection(conf);
        }
        Put put = new Put(rowKey.getBytes());
        for (Map.Entry<String, String> columnData : columnDatas.entrySet()) {
            put.addColumn(familyName.getBytes(),
                    columnData.getKey().getBytes(), Bytes.toBytes(columnData.getValue()));
        }
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void putFamilyDatas(Table table, String rowKey,
                                      Map<String, Map<String, String>> familyDatas) throws IOException {
        if (conn == null) {
            conn = ConnectionFactory.createConnection(conf);
        }
        Put put = new Put(rowKey.getBytes());
        for (Map.Entry<String, Map<String, String>> familyData : familyDatas
                .entrySet()) {
            String familyName = familyData.getKey();
            Map<String, String> columnDatas = familyData.getValue();
            for (Map.Entry<String, String> columnData : columnDatas.entrySet()) {
                put.addColumn(familyName.getBytes(), columnData.getKey()
                        .getBytes(), Bytes.toBytes(columnData.getValue()));
            }
        }
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void putRowDatas(Table table,
                                   Map<String, Map<String, Map<String, String>>> rowDatas) throws IOException {
        if (conn == null) {
            conn = ConnectionFactory.createConnection(conf);
        }
        List<Put> puts = new ArrayList<Put>();
        for (Map.Entry<String, Map<String, Map<String, String>>> rowData : rowDatas
                .entrySet()) {
            String rowKey = rowData.getKey();
            if (rowKey != null) {
                Map<String, Map<String, String>> familyDatas = rowData
                        .getValue();
                Put put = new Put(rowKey.getBytes());
                for (Map.Entry<String, Map<String, String>> familyData : familyDatas
                        .entrySet()) {
                    String familyName = familyData.getKey();
                    Map<String, String> columnDatas = familyData.getValue();
                    for (Map.Entry<String, String> columnData : columnDatas
                            .entrySet()) {
                        put.addColumn(familyName.getBytes(), columnData
                                .getKey().getBytes(), Bytes.toBytes(columnData.getValue()));
                    }
                }
                puts.add(put);
            }
        }
        try {
            table.put(puts);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Map<String, String> getFamilyDatas(Table table, String rowkey, String family) throws IOException {
        if (conn == null) {
            conn = ConnectionFactory.createConnection(conf);
        }
        Map<String, String> datas = new HashMap<String, String>();
        Get get = new Get(rowkey.getBytes());
        get.addFamily(family.getBytes());
        try {
            Result result = table.get(get);
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                String key = new String(CellUtil.cloneQualifier(cell));
                String value = new String(CellUtil.cloneValue(cell), "UTF-8");
                datas.put(key, value);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return datas;
    }

    public static String getValueData(Table table, String rowkey, String family, String key) throws IOException {
        if (conn == null) {
            conn = ConnectionFactory.createConnection(conf);
        }
        String value = "";
        Get get = new Get(rowkey.getBytes());
        get.addFamily(family.getBytes());
        try {
            Result result = table.get(get);
            value = Bytes.toString(result.getValue(family.getBytes(), key.getBytes()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return value;
    }

    public static List<String> scanValueDatas(Table table, String family, String key, int limit) throws IOException {
        if (conn == null) {
            conn = ConnectionFactory.createConnection(conf);
        }
        List<String> values = new ArrayList<String>();
        Scan scan = new Scan();
        Filter filter = new PageFilter(limit);
        scan.setFilter(filter);
        try {
            ResultScanner results = table.getScanner(scan);
            for (Result res : results) {
                values.add(Bytes.toString(res.getValue(family.getBytes(), key.getBytes())));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return values;
    }

}