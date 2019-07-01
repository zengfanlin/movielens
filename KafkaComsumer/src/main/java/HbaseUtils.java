import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

/**
 * 使用方法：通过getInstance获得HbaseUtils实例对象，通过对象获得连接
 */
public class HbaseUtils {

    private static final HbaseUtils eg = new HbaseUtils();
    private static Configuration configuration = null;
    private static Connection connection = null;
    private static Admin admin = null;

    /**
     * 单例模式
     */
    private static HbaseUtils instance = null;

    public static synchronized HbaseUtils getInstance() {
        if (instance == null) {
            instance = new HbaseUtils();
        }
        return instance;
    }

    private HbaseUtils() {
        getConnection();
        getAdmin();
    }

    public Connection GetConn() {
        return connection;
    }

    public static Connection getConnection() {
        try {
            if (configuration == null) {
                Properties prop = new Properties();
                InputStream file = HbaseUtils.class.getClassLoader().getResourceAsStream("hbase_login_initial.properties");
                prop.load(file);
                String ZK_CONNECT_KEY = prop.getProperty("ZK_CONNECT_KEY");
                String ZK_CONNECT_VALUE = prop.getProperty("ZK_CONNECT_VALUE");
                configuration = HBaseConfiguration.create();
                configuration.set(ZK_CONNECT_KEY, ZK_CONNECT_VALUE);
            }
            if (connection == null) {
                connection = ConnectionFactory.createConnection(configuration);
            } else return connection;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connection;
    }

    // 获取管理员对象
    public static Admin getAdmin() {
        try {
            admin = connection.getAdmin();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return admin;
    }

    // 查询所有表
    public void getAllTables() throws Exception {
        //获取列簇的描述信息
        HTableDescriptor[] listTables = admin.listTables();
        for (HTableDescriptor listTable : listTables) {
            //转化为表名
            String tbName = listTable.getNameAsString();
            //获取列的描述信息
            HColumnDescriptor[] columnFamilies = listTable.getColumnFamilies();
            System.out.println("tableName:" + tbName);
            for (HColumnDescriptor columnFamilie : columnFamilies) {
                //获取列簇的名字
                String columnFamilyName = columnFamilie.getNameAsString();
                System.out.print("\t" + "columnFamilyName:" + columnFamilyName);
            }
            System.out.println();
        }
    }

    public Table getTable(String tableName) throws Exception {
        Table table;
        TableName name = TableName.valueOf(tableName);
        if (admin.tableExists(name)) {
            table = connection.getTable(name);
        } else {
            table = null;
        }
//        table = connection.getTable(name);
        return table;
    }

    public Result getResult(String tableName, String rowKey) throws Exception {
        Result result;
        TableName name = TableName.valueOf(tableName);
//        if(admin.tableExists(name)) {
//            Table table = connection.getTable(name);
//            Get get = new Get(rowKey.getBytes());
//            result = table.get(get);
//        }else {
//            result = null;
//        }
        Table table = connection.getTable(name);
        Get get = new Get(rowKey.getBytes());
        result = table.get(get);
        return result;
    }


    public void createTable(String tableName, String[] family) throws Exception {
        TableName name = TableName.valueOf(tableName);
        //判断表是否存在
        if (admin.tableExists(name)) {
            System.out.println("table已经存在！");
        } else {
            //表的列簇示例
            HTableDescriptor htd = new HTableDescriptor(name);
            //向列簇中添加列的信息
            for (String str : family) {
                HColumnDescriptor hcd = new HColumnDescriptor(str);
                hcd.setMaxVersions(1);
                htd.addFamily(hcd);
            }
            //创建表
            admin.createTable(htd);
            //判断表是否创建成功
            if (admin.tableExists(name)) {
                System.out.println("table创建成功");
            } else {
                System.out.println("table创建失败");
            }
        }
    }

    public void createTable(HTableDescriptor htds) throws Exception {
        //获得表的名字
        String tbName = htds.getNameAsString();
        admin.createTable(htds);
    }

    /**
     * // 创建表，传参，表名和封装好的多个列簇
     *
     * @param tableName 表名
     * @param htds      封装好的多个列簇
     * @throws Exception
     */
    public void createTable(String tableName, HTableDescriptor htds) throws Exception {

        TableName name = TableName.valueOf(tableName);

        if (admin.tableExists(name)) {
            System.out.println("table已经存在！");
        } else {
            admin.createTable(htds);
            boolean flag = admin.tableExists(name);
            System.out.println(flag ? "创建成功" : "创建失败");
        }

    }

    /**
     * // 查看表的列簇属性
     *
     * @param tableName 表名
     * @throws Exception
     */
    public void descTable(String tableName) throws Exception {
        //转化为表名
        TableName name = TableName.valueOf(tableName);
        //判断表是否存在
        if (admin.tableExists(name)) {
            //获取表中列簇的描述信息
            HTableDescriptor tableDescriptor = admin.getTableDescriptor(name);
            //获取列簇中列的信息
            HColumnDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();
            for (HColumnDescriptor columnFamily : columnFamilies) {
                System.out.println(columnFamily);
            }

        } else {
            System.out.println("table不存在");
        }

    }

    /**
     * 判断表存在不存在
     *
     * @param tableName
     * @return
     * @throws Exception
     */
    public boolean existTable(String tableName) throws Exception {
        TableName name = TableName.valueOf(tableName);
        return admin.tableExists(name);
    }

    /**
     * disable表
     *
     * @param tableName
     * @throws Exception
     */
    public void disableTable(String tableName) throws Exception {

        TableName name = TableName.valueOf(tableName);

        if (admin.tableExists(name)) {
            if (admin.isTableEnabled(name)) {
                admin.disableTable(name);
            } else {
                System.out.println("table不是活动状态");
            }
        } else {
            System.out.println("table不存在");
        }

    }

    /**
     * drop表
     *
     * @param tableName
     * @throws Exception
     */
    public void dropTable(String tableName) throws Exception {
        //转化为表名
        TableName name = TableName.valueOf(tableName);
        //判断表是否存在
        if (admin.tableExists(name)) {
            //判断表是否处于可用状态
            boolean tableEnabled = admin.isTableEnabled(name);

            if (tableEnabled) {
                //使表变成不可用状态
                admin.disableTable(name);
            }
            //删除表
            admin.deleteTable(name);
            //判断表是否存在
            if (admin.tableExists(name)) {
                System.out.println("删除失败");
            } else {
                System.out.println("删除成功");
            }

        } else {
            System.out.println("table不存在");
        }
    }

    /**
     * 修改表(增加
     *
     * @param tableName
     * @throws Exception
     */
    public void modifyTable_add(String tableName, String[] addColumn) throws Exception {
        //转化为表名
        TableName name = TableName.valueOf(tableName);
        //判断表是否存在
        if (admin.tableExists(name)) {
            //判断表是否可用状态
            boolean tableEnabled = admin.isTableEnabled(name);

            if (tableEnabled) {
                //使表变成不可用
                admin.disableTable(name);
            }
            //根据表名得到表
            HTableDescriptor tableDescriptor = admin.getTableDescriptor(name);
            for (String add : addColumn) {
                HColumnDescriptor addColumnDescriptor = new HColumnDescriptor(add);
                tableDescriptor.addFamily(addColumnDescriptor);
            }
            //替换该表所有的列簇
            admin.modifyTable(name, tableDescriptor);

        } else {
            System.out.println("table不存在");
        }
    }

    /**
     * 修改表(删除
     *
     * @param tableName
     * @throws Exception
     */
    public void modifyTable_remove(String tableName, String[] removeColumn) throws Exception {
        //转化为表名
        TableName name = TableName.valueOf(tableName);
        //判断表是否存在
        if (admin.tableExists(name)) {
            //判断表是否可用状态
            boolean tableEnabled = admin.isTableEnabled(name);

            if (tableEnabled) {
                //使表变成不可用
                admin.disableTable(name);
            }
            //根据表名得到表
            HTableDescriptor tableDescriptor = admin.getTableDescriptor(name);
            for (String remove : removeColumn) {
                HColumnDescriptor removeColumnDescriptor = new HColumnDescriptor(remove);
                tableDescriptor.removeFamily(removeColumnDescriptor.getName());
            }
            //替换该表所有的列簇
            admin.modifyTable(name, tableDescriptor);

        } else {
            System.out.println("table不存在");
        }
    }

    /**
     * 修改表(增加和删除
     *
     * @param tableName
     * @param addColumn
     * @param removeColumn
     * @throws Exception
     */
    public void modifyTable(String tableName, String[] addColumn, String[] removeColumn) throws Exception {
        //转化为表名
        TableName name = TableName.valueOf(tableName);
        //判断表是否存在
        if (admin.tableExists(name)) {
            //判断表是否可用状态
            boolean tableEnabled = admin.isTableEnabled(name);

            if (tableEnabled) {
                //使表变成不可用
                admin.disableTable(name);
            }
            //根据表名得到表
            HTableDescriptor tableDescriptor = admin.getTableDescriptor(name);
            //创建列簇结构对象，添加列
            for (String add : addColumn) {
                HColumnDescriptor addColumnDescriptor = new HColumnDescriptor(add);
                tableDescriptor.addFamily(addColumnDescriptor);
            }
            //创建列簇结构对象，删除列
            for (String remove : removeColumn) {
                HColumnDescriptor removeColumnDescriptor = new HColumnDescriptor(remove);
                tableDescriptor.removeFamily(removeColumnDescriptor.getName());
            }

            admin.modifyTable(name, tableDescriptor);


        } else {
            System.out.println("table不存在");
        }

    }

    /**
     * 添加已封装好的列
     *
     * @param tableName
     * @param hcds
     * @throws Exception
     */
    public void modifyTable(String tableName, HColumnDescriptor hcds) throws Exception {
        //转化为表名
        TableName name = TableName.valueOf(tableName);
        //根据表名得到表
        HTableDescriptor tableDescriptor = admin.getTableDescriptor(name);
        //获取表中所有的列簇信息
        HColumnDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();

        boolean flag = false;
        //判断参数中传入的列簇是否已经在表中存在
        for (HColumnDescriptor columnFamily : columnFamilies) {
            if (columnFamily.equals(hcds)) {
                flag = true;
            }
        }
        //存在提示，不存在直接添加该列簇信息
        if (flag) {
            System.out.println("该列簇已经存在");
        } else {
            tableDescriptor.addFamily(hcds);
            admin.modifyTable(name, tableDescriptor);
        }

    }

    /**
     * 添加数据的定义前提
     *
     * @param familyName
     * @param name
     * @throws IOException
     */
    private void putData_Temp(String familyName, TableName name) throws IOException {
        if (admin.tableExists(name)) {
        } else {
            //根据表明创建表结构
            HTableDescriptor tableDescriptor = new HTableDescriptor(name);
            //定义列簇的名字
            HColumnDescriptor columnFamilyName = new HColumnDescriptor(familyName);
            tableDescriptor.addFamily(columnFamilyName);
            admin.createTable(tableDescriptor);
        }
    }


    /**
     * 添加数据
     * tableName:    表明
     * rowKey:    行键
     * familyName:列簇
     * columnName:列名
     * value:        值
     */
    public boolean putData(String tableName, String rowKey, String familyName, String columnName, String value)
            throws Exception {
        try {
            //转化为表名
            TableName name = TableName.valueOf(tableName);
            //添加数据之前先判断表是否存在，不存在的话先创建表
            putData_Temp(familyName, name);

            Table table = connection.getTable(name);
            Put put = new Put(rowKey.getBytes());

            put.addColumn(familyName.getBytes(), columnName.getBytes(), value.getBytes());
            table.put(put);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }


    /**
     * 添加数据包括（时间戳）
     *
     * @param tableName
     * @param rowKey
     * @param familyName
     * @param columnName
     * @param value
     * @param timestamp
     * @throws Exception
     */
    public void putData(String tableName, String rowKey, String familyName, String columnName, String value,
                        long timestamp) throws Exception {

        // 转化为表名
        TableName name = TableName.valueOf(tableName);
        // 添加数据之前先判断表是否存在，不存在的话先创建表
        putData_Temp(familyName, name);

        Table table = connection.getTable(name);
        Put put = new Put(rowKey.getBytes());

        //put.addColumn(familyName.getBytes(), columnName.getBytes(), value.getBytes());
        put.addImmutable(familyName.getBytes(), columnName.getBytes(), timestamp, value.getBytes());
        table.put(put);

    }

    /**
     * 根据rowkey查询数据
     *
     * @param tableName
     * @param rowKey
     * @param familyName
     * @return
     * @throws Exception
     */
    public Result getResult(String tableName, String rowKey, String familyName) throws Exception {
        Result result;
        TableName name = TableName.valueOf(tableName);
        if (admin.tableExists(name)) {
            Table table = connection.getTable(name);
            Get get = new Get(rowKey.getBytes());
            get.addFamily(familyName.getBytes());
            result = table.get(get);

        } else {
            result = null;
        }

        return result;
    }

    /**
     * 根据rowkey查询数据
     *
     * @param tableName
     * @param rowKey
     * @param familyName
     * @param columnName
     * @return
     * @throws Exception
     */
    public Result getResult(String tableName, String rowKey, String familyName, String columnName) throws Exception {

        Result result;
        TableName name = TableName.valueOf(tableName);
        if (admin.tableExists(name)) {
            Table table = connection.getTable(name);
            Get get = new Get(rowKey.getBytes());
            get.addColumn(familyName.getBytes(), columnName.getBytes());
            result = table.get(get);

        } else {
            result = null;
        }

        return result;
    }

    /**
     * 查询指定version
     *
     * @param tableName
     * @param rowKey
     * @param familyName
     * @param columnName
     * @param versions
     * @return
     * @throws Exception
     */
    public Result getResultByVersion(String tableName, String rowKey, String familyName, String columnName,
                                     int versions) throws Exception {

        Result result;
        TableName name = TableName.valueOf(tableName);
        if (admin.tableExists(name)) {
            Table table = connection.getTable(name);
            Get get = new Get(rowKey.getBytes());
            get.addColumn(familyName.getBytes(), columnName.getBytes());
            get.setMaxVersions(versions);
            result = table.get(get);

        } else {
            result = null;
        }

        return result;
    }

    /**
     * scan全表数据
     *
     * @param tableName
     * @return
     * @throws Exception
     */
    public ResultScanner getResultScann(String tableName) throws Exception {

        ResultScanner result;
        TableName name = TableName.valueOf(tableName);
        if (admin.tableExists(name)) {
            Table table = connection.getTable(name);
            Scan scan = new Scan();
            result = table.getScanner(scan);

        } else {
            result = null;
        }

        return result;
    }

    /**
     * scan全表数据
     *
     * @param tableName
     * @param scan
     * @return
     * @throws Exception
     */
    public ResultScanner getResultScann(String tableName, Scan scan) throws Exception {

        ResultScanner result;
        TableName name = TableName.valueOf(tableName);
        if (admin.tableExists(name)) {
            Table table = connection.getTable(name);
            result = table.getScanner(scan);

        } else {
            result = null;
        }

        return result;
    }

    /**
     * 删除数据（指定的lie
     *
     * @param tableName
     * @param rowKey
     * @throws Exception
     */
    public void deleteColumn(String tableName, String rowKey) throws Exception {

        TableName name = TableName.valueOf(tableName);
        if (admin.tableExists(name)) {
            Table table = connection.getTable(name);
            Delete delete = new Delete(rowKey.getBytes());
            table.delete(delete);

        } else {
            System.out.println("table不存在");
        }
    }

    /**
     * 删除数据（指定的lie）
     *
     * @param tableName
     * @param rowKey
     * @param falilyName
     * @throws Exception
     */
    public void deleteColumn(String tableName, String rowKey, String falilyName) throws Exception {

        TableName name = TableName.valueOf(tableName);
        if (admin.tableExists(name)) {
            Table table = connection.getTable(name);
            Delete delete = new Delete(rowKey.getBytes());
            delete.addFamily(falilyName.getBytes());
            table.delete(delete);

        } else {
            System.out.println("table不存在");
        }
    }

    /**
     * 删除数据（指定的行lie
     *
     * @param tableName
     * @param rowKey
     * @param falilyName
     * @param columnName
     * @throws Exception
     */
    public void deleteColumn(String tableName, String rowKey, String falilyName, String columnName) throws Exception {
        TableName name = TableName.valueOf(tableName);
        if (admin.tableExists(name)) {
            Table table = connection.getTable(name);
            Delete delete = new Delete(rowKey.getBytes());
            delete.addColumn(falilyName.getBytes(), columnName.getBytes());
            table.delete(delete);

        } else {
            System.out.println("table不存在");
        }
    }

    public static void close() {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Result getResult(String tableName, String rowKey, FilterList filterList) throws IOException {
        Result result;
        TableName name = TableName.valueOf(tableName);
        if (admin.tableExists(name)) {
            Table table = connection.getTable(name);
            Get get = new Get(rowKey.getBytes());
            get.setFilter(filterList);
            result = table.get(get);
        } else {
            result = null;
        }
        return result;
    }

    public ResultScanner getResult(String tableName, FilterList filterList) throws IOException {
        ResultScanner resultScanner;
        TableName name = TableName.valueOf(tableName);
        if (admin.tableExists(name)) {
            Table table = connection.getTable(name);
            Scan scan = new Scan();
            scan.setFilter(filterList);
            resultScanner = table.getScanner(scan);
        } else {
            resultScanner = null;
        }
        return resultScanner;
    }

    public ResultScanner getScanner(String tableName) {
        try {
            TableName name = TableName.valueOf(tableName);
            Table table = connection.getTable(name);
            Scan scan = new Scan();
            scan.setCaching(1000);
            return table.getScanner(scan);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public boolean putDatas(String tableName, List<Put> puts) {
        try {
            //转化为表名
            TableName name = TableName.valueOf(tableName);
            Table table = connection.getTable(name);
            table.put(puts);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    public ResultScanner getScanner(String tableName, String startRowkey, String endRowkey) {
        try {
            TableName name = TableName.valueOf(tableName);
            Table table = connection.getTable(name);
            Scan scan = new Scan();
            scan.setStartRow(startRowkey.getBytes());
            scan.setStopRow(endRowkey.getBytes());
            scan.setCaching(1000);
            return table.getScanner(scan);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public ResultScanner getScanner(String tableName, String startRowkey, String endRowkey, FilterList filterList) {
        try {
            TableName name = TableName.valueOf(tableName);
            Table table = connection.getTable(name);
            Scan scan = new Scan();
            scan.setStartRow(startRowkey.getBytes());
            scan.setStopRow(endRowkey.getBytes());
            scan.setFilter(filterList);
            scan.setCaching(1000);
            return table.getScanner(scan);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public boolean deleteColumnFamily(String tableName, String cfName) {
        try {
            admin.deleteColumn(TableName.valueOf(tableName), cfName.getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    public boolean deleteQualifier(String tableName, String rowkey, String cfName, String qualifier) {
        try {
            TableName name = TableName.valueOf(tableName);
            Table table = connection.getTable(name);
            Delete delete = new Delete(rowkey.getBytes());
            delete.addColumn(cfName.getBytes(), qualifier.getBytes());
            table.delete(delete);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }


}
