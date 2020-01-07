

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Hbase 操作工具类
 *
 * @author Logan
 * @version 1.0.0
 * @createDate 2019-05-03
 */
public class HbaseUtils {

    public static Configuration conf = new Configuration();
    //    private static Connection conn = null;
    private static ExecutorService pool = Executors.newFixedThreadPool(200);

    static {
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "node21,node22,node23");
        conf.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 120000);
//        try {
//            conn = ConnectionFactory.createConnection(conf);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    // ===============Common=====================================
    public static Connection getHbaseConn() throws IOException {
        Connection conn = ConnectionFactory.createConnection(conf);
        System.out.println("connectting open:" + conn.hashCode());
        return conn;
    }

    /**
     * 根据表名获取Table对象
     *
     * @param name 表名，必要时可指定命名空间，比如：“default:user”
     * @return Hbase Table 对象
     * @throws IOException 有异常抛出，由调用者捕获处理
     */
    public static Table getTable(Connection connection, String name) throws IOException {
        TableName tableName = TableName.valueOf(name);
        return connection.getTable(tableName);
    }

    // =============== Put =====================================

    /**
     * 根据rowKey生成一个Put对象
     *
     * @param rowKey rowKey
     * @return Put对象
     */
    public static Put createPut(String rowKey) {
        return new Put(Bytes.toBytes(rowKey));
    }

    /**
     * 在Put对象上增加Cell
     *
     * @param put  Put对象
     * @param cell cell对象
     * @throws IOException 有异常抛出，由调用者捕获处理
     */
    public static void addCellOnPut(Put put, Cell cell) throws IOException {
        put.add(cell);
    }

    /**
     * 在Put对象上增加值
     *
     * @param put       Put对象
     * @param family    列簇
     * @param qualifier 列
     * @param value     字符串类型的值
     */
    public static void addValueOnPut(Put put, String family, String qualifier, String value) {
        addValueOnPut(put, family, qualifier, Bytes.toBytes(value));
    }

    /**
     * 在Put对象上增加值
     *
     * @param put       Put对象
     * @param family    列簇
     * @param qualifier 列
     * @param value     字节数组类型的值，可以是任意对象序列化而成
     */
    public static void addValueOnPut(Put put, String family, String qualifier, byte[] value) {
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), value);
    }

    /**
     * 在Put对象上增加值
     *
     * @param put       Put对象
     * @param family    列簇
     * @param qualifier 列
     * @param ts        Timestamp时间戳
     * @param value     字符串类型的值
     */
    public static void addValueOnPut(Put put, String family, String qualifier, long ts, String value) {
        addValueOnPut(put, family, qualifier, ts, Bytes.toBytes(value));
    }

    /**
     * 在Put对象上增加值
     *
     * @param put       Put对象
     * @param family    列簇
     * @param qualifier 列
     * @param ts        Timestamp时间戳
     * @param value     字节数组类型的值，可以是任意对象序列化而成
     */
    public static void addValueOnPut(Put put, String family, String qualifier, long ts, byte[] value) {
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), ts, value);
    }

    /**
     * 按表名插入一个Put对象包含的数据
     *
     * @param tableName 表名，必要时可指定命名空间，比如：“default:user”
     * @param put       要插入的数据对象
     * @throws IOException 有异常抛出，由调用者捕获处理
     */
    public static void put(Connection connection, String tableName, Put put) throws IOException {
        try (
                Table table = getTable(connection, tableName);
        ) {

            table.put(put);
            table.close();
        }
    }

    /**
     * 按表名批量插入Put对象包含的数据
     *
     * @param tableName 表名，必要时可指定命名空间，比如：“default:user”
     * @param puts      要插入的数据对象集合
     * @throws IOException 有异常抛出，由调用者捕获处理
     */
    public static void put(Connection connection, String tableName, List<Put> puts) throws IOException {
        try (
                Table table = getTable(connection, tableName);
        ) {

            table.put(puts);
            table.close();
        }
    }

    // =============== Get =====================================

    /**
     * 根据rowKey生成一个查询的Get对象
     *
     * @param rowKey rowKey
     * @return Get 对象
     */
    public static Get createGet(String rowKey) {
        return new Get(Bytes.toBytes(rowKey));
    }

    /**
     * 对查询的Get对象增加指定列簇
     *
     * @param get
     * @param family
     */
    public static void addFamilyOnGet(Get get, String family) {
        get.addFamily(Bytes.toBytes(family));
    }

    /**
     * 对查询的Get对象增加指定列簇和列
     *
     * @param get
     * @param family
     * @param qualifier
     */
    public static void addColumnOnGet(Get get, String family, String qualifier) {
        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
    }

    /**
     * 根据表名和rowKey查询结果（包含全部列簇和列）
     *
     * @param tableName 表名，必要时可指定命名空间，比如：“default:user”
     * @param rowKey    查询rowKey
     * @return 查询结果Result
     * @throws IOException 有异常抛出，由调用者捕获处理
     */
    public static Result get(Connection connection, String tableName, String rowKey) throws IOException {
        Get get = createGet(rowKey);
        return get(connection, tableName, get);
    }

    /**
     * 根据表名和rowKey数组批量查询结果（包含全部列簇和列）
     *
     * @param tableName 表名，必要时可指定命名空间，比如：“default:user”
     * @param rowKeys   查询rowKey数组
     * @return 查询结果Result数组
     * @throws IOException 有异常抛出，由调用者捕获处理
     */
    public static Result[] get(Connection connection, String tableName, String[] rowKeys) throws IOException {
        List<Get> gets = new ArrayList<Get>();
        for (String rowKey : rowKeys) {
            gets.add(createGet(rowKey));
        }
        return get(connection, tableName, gets);
    }

    /**
     * 根据表名和Get对象查询结果
     *
     * @param tableName 表名，必要时可指定命名空间，比如：“default:user”
     * @param get       Hbase查询对象
     * @return 查询结果Result
     * @throws IOException 有异常抛出，由调用者捕获处理
     */
    public static Result get(Connection connection, String tableName, Get get) throws IOException {
        try (
                Table table = getTable(connection, tableName);
        ) {
            Result results = table.get(get);
            table.close();
            return results;
        }
    }

    /**
     * 根据表名和Get对象数组查询结果
     *
     * @param tableName 表名，必要时可指定命名空间，比如：“default:user”
     * @param gets      多个Hbase查询对象组成的数组
     * @return 查询结果Result数组
     * @throws IOException 有异常抛出，由调用者捕获处理
     */
    public static Result[] get(Connection connection, String tableName, List<Get> gets) throws IOException {
        try (
                Table table = getTable(connection, tableName);
        ) {
            Result[] results = table.get(gets);
            table.close();
            return results;

        }

    }

    // =============== Scan =====================================

    /**
     * 根据startRow和stopRow创建扫描对象
     *
     * @param startRow 扫描开始行，结果包含该行
     * @param stopRow  扫描结束行，结果不包含该行
     * @return Scan对象
     */
    public static Scan createScan(String startRow, String stopRow) {
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(startRow));
        scan.withStopRow(Bytes.toBytes(stopRow));
        return scan;
    }

    /**
     * 对扫描对象设置列簇
     *
     * @param scan   扫描对象
     * @param family 列簇
     */
    public static void addFamilyOnScan(Scan scan, String family) {
        scan.addFamily(Bytes.toBytes(family));
    }

    /**
     * 对扫描对象设置列
     *
     * @param scan      扫描对象
     * @param family    列簇
     * @param qualifier 列簇下对应的列
     */
    public static void addColumnOnScan(Scan scan, String family, String qualifier) {
        scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
    }

    /**
     * 根据表名和扫描对象扫描数据
     *
     * @param tableName 表名，必要时可指定命名空间，比如：“default:user”
     * @param scan      扫描对象
     * @return 扫描结果集对象ResultScanner
     * @throws IOException 有异常抛出，由调用者捕获处理
     */
    public static ResultScanner scan(Connection connection, String tableName, Scan scan) throws IOException {
        try (
                Table table = getTable(connection, tableName);
        ) {
            ResultScanner results = table.getScanner(scan);
            table.close();
            return results;
//            return table.getScanner(scan);
        }
    }

    /**
     * 根据表名、开始行和结束行扫描数据（结果包含开始行，不包含结束行，半开半闭区间[startRow, stopRow)）
     *
     * @param tableName 表名，必要时可指定命名空间，比如：“default:user”
     * @param startRow  扫描开始行
     * @param stopRow   扫描结束行
     * @return 扫描结果集对象ResultScanner
     * @throws IOException 有异常抛出，由调用者捕获处理
     */
    public static ResultScanner scan(Connection connection, String tableName, String startRow, String stopRow) throws IOException {
        return scan(connection, tableName, createScan(startRow, stopRow));
    }

    // =============== Delete =====================================

    /**
     * 根据rowKey生成一个查询的Delete对象
     *
     * @param rowKey rowKey
     * @return Delete对象
     */
    public static Delete createDelete(String rowKey) {
        return new Delete(Bytes.toBytes(rowKey));
    }

//    /**
//     * 在Delete对象上增加Cell
//     *
//     * @param delete Delete对象
//     * @param cell cell对象
//     * @throws IOException 有异常抛出，由调用者捕获处理
//     */
//    public static void addCellOnDelete(Delete delete, Cell cell) throws IOException {
//        delete.add(cell);
//    }

    /**
     * 对删除对象增加指定列簇
     *
     * @param delete Delete对象
     * @param family 列簇
     */
    public static void addFamilyOnDelete(Delete delete, String family) {
        delete.addFamily(Bytes.toBytes(family));
    }

    /**
     * 对删除对象增加指定列簇和列
     *
     * @param delete    Delete对象
     * @param family    列簇
     * @param qualifier 列
     */
    public static void addColumnOnDelete(Delete delete, String family, String qualifier) {
        delete.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
    }

    /**
     * 按表名删除一个Delete对象指定的数据
     *
     * @param tableName 表名，必要时可指定命名空间，比如：“default:user”
     * @param delete    Delete对象
     * @throws IOException 有异常抛出，由调用者捕获处理
     */
    public static void delete(Connection connection, String tableName, Delete delete) throws IOException {
        try (
                Table table = getTable(connection, tableName);
        ) {
            table.delete(delete);
            table.close();
        }
    }

    /**
     * 按表名批量删除Delete对象集合包含的指定数据
     *
     * @param tableName 表名，必要时可指定命名空间，比如：“default:user”
     * @param deletes   Delete对象集合
     * @throws IOException 有异常抛出，由调用者捕获处理
     */
    public static void delete(Connection connection, String tableName, List<Delete> deletes) throws IOException {
        try (
                Table table = getTable(connection, tableName);
        ) {
            table.delete(deletes);
            table.close();
        }
    }

}