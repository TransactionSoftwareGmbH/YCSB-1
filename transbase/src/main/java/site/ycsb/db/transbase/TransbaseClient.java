package site.ycsb.db.transbase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

/**
 * <a href="https://www.tranbase.de/">Transbase</a> binding for YCSB.
 */
public class TransbaseClient extends DB {
  // public HashMap<Set<String>,StatementCache> StatementsDelete;
  private HashMap<Set<String>, CachedStatement> statementsInsert;
  private HashMap<Set<String>, CachedStatement> statementsUpdate;
  private PreparedStatement deleteStatement = null;
  private PreparedStatement readStatement = null;
  private PreparedStatement scanStatement = null;

  // Properties:
  //  Connection Properties:
  public static final String CONNECTION_URL = "db.url";
  public static final String TRANSBASE_PREFIX = "jdbc:transbase:";
  public static final String CONNECTION_USER = "db.user";
  public static final String CONNECTION_PASSWD = "db.passwd";
  public static final String JDBC_AUTO_COMMIT = "jdbc.autocommit";
  public static final String JDBC_AUTO_COMMIT_DEFAULT = "true";
  
  //  YCSB Client Properties:
  public static final String DB_BATCH_SIZE = "db.batchsize";
  public static final String DB_BATCH_SIZE_DEFAULT = "10";
  public static final String JDBC_BATCH_UPDATES = "jdbc.batchupdateapi";
  public static final String JDBC_BATCH_UPDATES_DEFAULT = "false";

  //  YCSB Configuration Properties (necessary for creation of the table used):
  public static final String TABLE_NAME_PROPERTY = "table";
  public static final String TABLE_NAME_PROPERTY_DEFAULT = "usertable";
  public static final String FIELD_COUNT_PROPERTY = "fieldcount";
  public static final String FIELD_COUNT_PROPERTY_DEFAULT = "10";
  public static final String FIELD_NAME_PREFIX = "fieldnameprefix";
  public static final String FIELD_NAME_PREFIX_DEFAULT = "field";

  private String dbDriver = "jdbc.transbase.Driver";
  private String dbUrl = "";
  private String dbUser = "tbadmin";
  private String dbPasswd = "";
  private String dbBatchsize = "0";
  private String tablename = "";
  private static String keyname = "YCSB_KEY";
  private String fieldnamePrefix = "";
  private int nrfields = 10;

  private static boolean useGlobCon = false;
  private static Connection globCon = null;
  private Connection con = null;
  private volatile boolean initialized = false;
  private static volatile boolean tableInitialized = false;
  private Properties props;

  private boolean autoCommit;
  private boolean batchUpdates;
  private int batchsize;

  @Override
  public void init() throws DBException {
    if(initialized) {
      return;
    }
    initialized = true;

    props = getProperties();
    dbUrl = props.getProperty(CONNECTION_URL, dbUrl);
    dbUser = props.getProperty(CONNECTION_USER, dbUser);
    dbPasswd = props.getProperty(CONNECTION_PASSWD, dbPasswd);
    dbDriver = "transbase.jdbc.Driver";

    autoCommit = Boolean.parseBoolean(props.getProperty(JDBC_AUTO_COMMIT, JDBC_AUTO_COMMIT_DEFAULT));

    tablename = props.getProperty(TABLE_NAME_PROPERTY, TABLE_NAME_PROPERTY_DEFAULT);
    fieldnamePrefix = props.getProperty(FIELD_NAME_PREFIX, FIELD_NAME_PREFIX_DEFAULT);
    nrfields = Integer.parseInt(props.getProperty(FIELD_COUNT_PROPERTY, FIELD_COUNT_PROPERTY_DEFAULT));

    dbBatchsize = props.getProperty(DB_BATCH_SIZE, DB_BATCH_SIZE_DEFAULT);
    batchsize = Integer.parseUnsignedInt(dbBatchsize);
    batchUpdates = Boolean.parseBoolean(props.getProperty(JDBC_BATCH_UPDATES, JDBC_BATCH_UPDATES_DEFAULT));

    statementsInsert = new HashMap<Set<String>, CachedStatement>();
    statementsUpdate = new HashMap<Set<String>, CachedStatement>();

    if(!dbUrl.startsWith(TRANSBASE_PREFIX)) {
      dbUrl = TRANSBASE_PREFIX + dbUrl;
    }

    try {
      Class.forName(dbDriver);
      if(useGlobCon) {
        synchronized (TransbaseClient.class) {
          if(null == globCon) {
            globCon = DriverManager.getConnection(dbUrl, dbUser, dbPasswd);
          }
        }
        con = globCon;
      } else {
        con = DriverManager.getConnection(dbUrl, dbUser, dbPasswd);
      }
      if(!autoCommit) {
        con.setAutoCommit(false);
      }
      con.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);

      // create usertable if it does not exist
      if(!tableInitialized) {
        synchronized (TransbaseClient.class) {
          if(!tableInitialized) {
            StringBuffer sql = new StringBuffer("create table if not exists ");
            sql.append(toIdentifier(tablename)).append(" without ikaccess(").append(toIdentifier(keyname));
            sql.append(" string primary key");
            for(int ii=0; ii<nrfields; ii++) {
              sql.append(",").append(fieldnamePrefix).append(ii).append(" string");
            }
            sql.append(")");
            Statement stat = con.createStatement();
            stat.executeUpdate(sql.toString());
            tableInitialized = true;
          }
        }
      }

    } catch (SQLException e) {
      e.printStackTrace();
      throw new DBException(e.getMessage());
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      throw new DBException(e.getMessage());
    }
  }
  
  @Override
  public void cleanup() throws DBException {
    synchronized(con) {
      try {
        completePendingBatches(null);
        if(!autoCommit) {
          con.commit();
        }
      } catch(SQLException e) {
        throw new DBException(e.getMessage());
      }
    }
  }

  @Override
  public Status delete(String table, String key) {
    synchronized(con) {
      try {
        PreparedStatement stmt = getdeleteStatement(table);
        completePendingBatches(null);

        stmt.setString(1, key);
        int recs = stmt.executeUpdate();
        if(0== recs) {
          return Status.NOT_FOUND;
        }
      } catch (SQLException e) {
        // log exception
        return Status.ERROR;
      }
    }
    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) 
  {
    synchronized(con) {
      try {
        CachedStatement cs = getInsertStatement(table, values);
        completePendingBatches(cs);

        PreparedStatement ps = cs.ps;
        ps.setString(1, key);
        int pos = 1;
        for(String f : cs.fields) {
          cs.ps.setString(++pos, values.get(f).toString());
        }

        if(batchUpdates) {
          cs.ps.addBatch();
          cs.batched++;

          if(0< batchsize && batchsize <= cs.batched) {
            cs.batched = 0;
            cs.ps.executeBatch();
          }
        } else {
          cs.ps.executeUpdate();
        }
        return Status.OK;
      } catch (SQLException e) {
        e.printStackTrace();
        return Status.ERROR;
      }
    }
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values)
  {
    synchronized(con) {
      try {
        CachedStatement cs = getUpdateStatement(table,  values);
        completePendingBatches(cs);

        int ii = 0;
        for(String field: cs.fields) {  // ensure stable order of fields
          cs.ps.setString(++ii, values.get(field).toString()); // <field> = ?
        }
        cs.ps.setString(++ii, key);  // where <key> = ?

        if(batchUpdates) {
          cs.ps.addBatch();
          cs.batched++;

          if(0 < batchsize && batchsize <= cs.batched) {
            cs.batched = 0;
            cs.ps.executeBatch();
          }
        } else {
          cs.ps.executeUpdate();
        }
        return Status.OK;
      } catch (SQLException e) {
        e.printStackTrace();
        return Status.ERROR;
      }
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) 
  {
    synchronized(con) {
      try {
        PreparedStatement stmt = getreadStatement(table, fields);
        completePendingBatches(null);

        stmt.setString(1, key);
        ResultSet res = stmt.executeQuery();
        if (!res.next()) {
          res.close();
          return Status.NOT_FOUND;
        }
        if (result != null && fields != null) {
          for (String field : fields) {
            String value = res.getString(field);
            result.put(field, new StringByteIterator(value));
          }
        }
        res.close();
        return Status.OK;
      } catch (SQLException e) {
        System.err.println("Error in read of table " + table + ": " + e);
        return Status.ERROR;
      }
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    if((null== result) || (null== fields)) { 
      return Status.BAD_REQUEST; 
    }
    synchronized(con) {
      try {
        PreparedStatement stmt = getscanStatement(table, fields);
        completePendingBatches(null);

        stmt.setString(1, startkey);
        ResultSet res = stmt.executeQuery();
      
        int cc = 0;
        while(res.next() && (cc < recordcount)) {
          HashMap<String, ByteIterator> rec = new HashMap<String, ByteIterator>();
          for (String field : fields) {
            String value = res.getString(field);
            rec.put(field, (ByteIterator)new StringByteIterator(value));
          }
          result.add(rec);
          ++cc;
        }
        res.close();
        return Status.OK;
      } catch (SQLException e) {
        System.err.println("Error in scan of table " + table + ": " + e);
        return Status.ERROR;
      }
    }
  }

  PreparedStatement getdeleteStatement(String table) throws SQLException {
    if(null== deleteStatement) {
      String sql = String.format("delete from %s where %s = ?", toIdentifier(table), toIdentifier(keyname));
      deleteStatement = con.prepareStatement(sql);
    }

    return deleteStatement;
  }

  CachedStatement getInsertStatement(String table, Map<String, ByteIterator> values) throws SQLException
  {
    CachedStatement sc = statementsInsert.get(values.keySet());
    if(null== sc) {
      StringBuilder flist = new StringBuilder();
      StringBuilder plist = new StringBuilder();
      ArrayList<String> fields = new ArrayList<>();
      flist.append(toIdentifier(keyname));
      plist.append("?");
      for(String key : values.keySet()) {
        fields.add(key);
        flist.append(",");
        flist.append(toIdentifier(key));
        plist.append(",?");
      }
      String sql = String.format("insert into %s (%s) values (%s)", toIdentifier(table), flist, plist);
      PreparedStatement stmt;
      stmt = con.prepareStatement(sql);

      sc = new CachedStatement();
      sc.fields = fields;
      sc.ps = stmt;
      sc.batched = 0;

      statementsInsert.put(values.keySet(), sc);
    }
    return sc;
  }

  CachedStatement getUpdateStatement(String table, Map<String, ByteIterator> values) throws SQLException
  {
    CachedStatement sc = statementsUpdate.get(values.keySet());
    if(null== sc) {
      StringBuilder sql = new StringBuilder();
      ArrayList<String> fields = new ArrayList<>();
      sql.append("update ").append(toIdentifier(tablename)).append(" set ");
      boolean first = true;
      for(String key : values.keySet()) {
        fields.add(key);

        if(!first) {
          sql.append(", ");
        }
        sql.append(key).append(" = ?");
        first = false;
      }
      sql.append(" where ").append(toIdentifier(keyname)).append(" = ?");
      PreparedStatement stmt = con.prepareStatement(sql.toString());

      sc = new CachedStatement();
      sc.fields = fields;
      sc.ps = stmt;
      sc.batched = 0;

      statementsUpdate.put(values.keySet(), sc);
    }
    return sc;
  }

  PreparedStatement getreadStatement(String table, Set<String> fields) throws SQLException
  {
    if(null== readStatement) {
      String sql = String.format("select * from %s where %s = ?", toIdentifier(table), toIdentifier(keyname));
      readStatement = con.prepareStatement(sql);
    }
    return readStatement;
  }

  PreparedStatement getscanStatement(String table, Set<String> fields) throws SQLException
  {
    if(null== scanStatement) {
      String sql = String.format("select * from %s where %s >= ? order by %s", 
          toIdentifier(table), toIdentifier(keyname), toIdentifier(keyname));
      scanStatement = con.prepareStatement(sql);
    }
    return scanStatement;
  }

  private CachedStatement lastStatement = null;
  private void completePendingBatches(CachedStatement cs) {
    if(null!= lastStatement) {
      if(null== cs || lastStatement != cs) {
        if(0< lastStatement.batched) {
          try {
            lastStatement.ps.executeBatch();
            lastStatement.batched = 0; 
          } catch (SQLException e) {
            e.printStackTrace();
          }
        }
      }
    }
    if(null!= cs) {
      lastStatement = cs;
    }
  }

  private static String toIdentifier(String name) {
    // for the time being we assume that all names can be used as simple identifiers!!!
    return name;
    //return "\"" + name.replace("\"", "\"\"") + "\"";
  }

  private static String toLiteral(String name) {
    return "'" + name.replace("'", "''") + "'";
  }

  private class CachedStatement {
    private ArrayList<String> fields;
    private PreparedStatement ps;
    private int batched;
  }
}
