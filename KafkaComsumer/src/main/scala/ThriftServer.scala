import java.sql.DriverManager

object ThriftServer {
  def main(args: Array[String]): Unit = {
    //1.创建驱动
    val driver = "org.apache.hive.jdbc.HiveDriver"
    Class.forName(driver)

    //2.创建连接
    val (url, username, userpasswd) = ("jdbc:hive2://node22:10000", "root", "12345")
    val conn = DriverManager.getConnection(url, username, userpasswd)

    //3.执行sql
    //      val sql="select * from default.emp a join default.dept b on a.deptno = b.deptno"
    val sql = "select * from movielensdb.udata"
    val pstmt = conn.prepareStatement(sql)
    val rs = pstmt.executeQuery()

    while (rs.next()) {
      //        println(rs.getInt("default.empno")+":"+rs.getString("default.ename"))
      //        println(rs.getString("emp.ename"))
      println(rs.getString("users") + ":" + rs.getString("rating"))
    }
    //4.关闭
    rs.close()
    pstmt.close()
    conn.close()
  }
}
