package cn.econta.ruban;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.FileSystem;
import io.vertx.core.file.OpenOptions;
import io.vertx.mssqlclient.MSSQLConnectOptions;
import io.vertx.mysqlclient.MySQLConnectOptions;
import io.vertx.sqlclient.*;
import io.vertx.sqlclient.templates.SqlTemplate;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;


public class MainVerticle extends AbstractVerticle {

  private final String mainId;

  public MainVerticle(String mainId) {
    this.mainId = mainId;
  }

  @Override
  public void start(Promise<Void> startPromise) {
    SqlConnectOptions connectOptions;
    String dbType = config().getString("db.type", "mysql");
    if ("mssql".equals(dbType)) {
      connectOptions = new MSSQLConnectOptions();
    } else {
      connectOptions = new MySQLConnectOptions();
    }
    connectOptions.setPort(config().getInteger("db.port", 3000))
      .setHost(config().getString("db.host", "139.196.105.80"))
      .setDatabase(config().getString("db.name", "e_health_v1"))
      .setUser(config().getString("db.username", "econta"))
      .setPassword(config().getString("db.password", "@Econta.!0"));
    PoolOptions poolOptions = new PoolOptions().setMaxSize(5);
    Pool pool = Pool.pool(vertx, connectOptions, poolOptions);
    FileSystem fs = vertx.fileSystem();
    String path = config().getString("gen.path", "D:" + File.separator) + mainId + "_" + System.currentTimeMillis() + ".sql";
    fs.open(path, new OpenOptions().setAppend(true)).onSuccess(af -> work(af, pool, "e_patient_record", mainId)
      .compose(v -> work(af, pool, "e_patient_medical", mainId))
      .compose(v -> work(af, pool, "e_patient_advice", mainId))
      .compose(v -> work(af, pool, "e_patient_archive", mainId))
      .compose(v -> work(af, pool, "e_patient_assay", mainId))
      .compose(v -> work(af, pool, "e_patient_examine", mainId))
      .compose(v -> work(af, pool, "e_patient_items", mainId))
      .compose(v -> work(af, pool, "e_patient_surgery", mainId))
      .compose(v -> work(af, pool, "e_patient_vitals", mainId))
      .onFailure(System.out::println)
      .onComplete(event -> startPromise.complete()));
  }


  static String SEP = ", ";
  static String QUOTE = "'";
  static String STRING_PREFIX = "";
  static String NEWLINE = System.getProperty("line.separator");

  static String INSERT_INTO = "insert into ";
  static String VALUES = ") values (";
  static String NULL = "null";

  /**
   * 获取表列名
   *
   * @param client    客户端
   * @param tableName 表名
   * @return 列名
   */
  private Future<RowSet<Row>> getTableColumns(Pool client, String tableName) {
    Map<String, Object> parameters = Collections.singletonMap("tableName", tableName);
    if("mssql".equals(config().getString("db.type"))) {
      return SqlTemplate.forQuery(client,"select syscolumns.name as column_name,systypes.name as data_type from syscolumns,systypes where syscolumns.xusertype=systypes.xusertype  and syscolumns.id=object_id(#{tableName})").execute(parameters);
    } else {
      return SqlTemplate.forQuery(client, "select  column_name,data_type  from information_schema.columns where  table_schema='e_health_v1' and table_name = #{tableName}").execute(parameters);
    }
  }

  /**
   * 获取表列名
   *
   * @param client 客户端
   * @param mainId 索引号
   * @return 列名
   */
  private static Future<RowSet<Row>> getRecord(Pool client, String tableName, String mainId) {
    Map<String, Object> parameters = Collections.singletonMap("mainId", mainId);
    return SqlTemplate.forQuery(client, "select * from " + tableName + " where main_id = #{mainId}")
      .execute(parameters);
  }

  private CompositeFuture work(AsyncFile asyncFile, Pool pool, String tableName, String mainId) {
    return CompositeFuture.all(getTableColumns(pool, tableName), getRecord(pool, tableName, mainId))
      .onSuccess(ar -> {
        CompositeFuture future = ar.result();
        RowSet<Row> columnsRowSet = future.resultAt(0);
        RowSet<Row> rowSet = future.resultAt(1);
        rowSet.forEach(row -> {
          StringBuilder builder = new StringBuilder(INSERT_INTO);
          builder.append(tableName).append(" (");
          StringBuilder builderValue = new StringBuilder(VALUES);
          Iterator<Row> iterator = columnsRowSet.iterator();
          for (int i = 0; i < columnsRowSet.size(); i++) {
            Row columnsRow = iterator.next();
            String columnsName = columnsRow.getString(0);
            String columnsType = columnsRow.getString(1);
            Object value = row.getValue(columnsName);
            String stringValue = value == null ? NULL : String.valueOf(value);
            if ("org_id".equals(columnsName)) {
              stringValue = config().getString("org_id");
            }

            boolean isStringLiteral = value != null && Arrays.asList("varchar", "text","nvarchar").contains(columnsType);

            builder.append(columnsName).append(i != columnsRowSet.size() - 1 ? SEP : "");
            builderValue.append(isStringLiteral ? (STRING_PREFIX + QUOTE) : "")
              .append(isStringLiteral ? stringValue.replace(QUOTE, QUOTE + QUOTE) : stringValue)
              .append(isStringLiteral ? QUOTE : "")
              .append(i != columnsRowSet.size() - 1 ? SEP : "");
          }
          builder.append(builderValue).append(");").append(NEWLINE);
          asyncFile.write(Buffer.buffer(builder.toString()));
        });

      });
  }
}
