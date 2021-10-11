package site.ycsb.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

public class TransbaseClient extends DB
{
	// public HashMap<Set<String>,StatementCache> StatementsDelete;
	HashMap<Set<String>,CachedStatement> StatementsInsert;
	HashMap<Set<String>,CachedStatement> StatementsUpdate;
	PreparedStatement DeleteStatement = null;
	PreparedStatement ReadStatement = null;
	PreparedStatement ScanStatement = null;

	// Properties:
  //  Connection Properties:
  public static final String CONNECTION_URL = "db.url";
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
	
	String db_driver = "jdbc.transbase.Driver";
	String db_url = "jdbc:transbase:file://YCSBdb";
	String db_user = "tbadmin";
	String db_passwd = "";
	String db_batchsize = "0";
	String jdbc_batchupdateapi = "true";
	String jdbc_batchsize = "10";
	String jdbc_fetchsize = "10";
	String tablename = "";
	static String keyname = "YCSB_KEY";
	String fieldname_prefix = "";
	int nrfields = 10;

	/*
	int iBatchSize = 0;
	int uBatchSize = 0;
	int dBatchSize = 0;
	*/
	
	private Connection con = null;
  private boolean initialized = false;
  private Properties props;

  private int batchsize;
  private boolean autoCommit;
  private boolean batchUpdates;
	
	
	
	//@Override
	public void init() throws DBException
	{
		if( initialized ) return;
		initialized = true;
	
		props = getProperties();
    db_url = props.getProperty(CONNECTION_URL, db_url);
    db_user = props.getProperty(CONNECTION_USER, db_user);
    db_passwd = props.getProperty(CONNECTION_PASSWD, db_passwd);
    String db_driver = "transbase.jdbc.Driver";
    
    autoCommit = Boolean.parseBoolean(props.getProperty(JDBC_AUTO_COMMIT, JDBC_AUTO_COMMIT_DEFAULT));
		
    tablename = props.getProperty(TABLE_NAME_PROPERTY, TABLE_NAME_PROPERTY_DEFAULT);
    fieldname_prefix = props.getProperty(FIELD_NAME_PREFIX, FIELD_NAME_PREFIX_DEFAULT);
    nrfields = Integer.parseInt(props.getProperty(FIELD_COUNT_PROPERTY, FIELD_COUNT_PROPERTY_DEFAULT));

    db_batchsize = props.getProperty(DB_BATCH_SIZE, DB_BATCH_SIZE_DEFAULT);
    batchsize = Integer.parseUnsignedInt(db_batchsize);
    batchUpdates = Boolean.parseBoolean(props.getProperty(JDBC_BATCH_UPDATES, JDBC_BATCH_UPDATES_DEFAULT));
    
    StatementsInsert = new HashMap<Set<String>,CachedStatement>();
    StatementsUpdate = new HashMap<Set<String>,CachedStatement>();

		// create usertable if it does not exist
		try
		{
			Class.forName(db_driver);
			con = DriverManager.getConnection(db_url, db_user, db_passwd);
			Statement stat = con.createStatement();
			ResultSet rs = stat.executeQuery(String.format("select tname from systable where tname = %s", toLiteral(tablename)));
			boolean exist = rs.next();
			rs.close();
			if( !exist )
			{
				StringBuffer sql = new StringBuffer("create table ");
				sql.append(toIdentifier(tablename)).append("(").append(toIdentifier(keyname)).append(" string primary key");
				for( int ii=0; ii<nrfields ; ii++ )
					sql.append(",").append(fieldname_prefix).append(ii).append(" string");
				sql.append(")");

				stat.executeUpdate(sql.toString());
			}
			if( !autoCommit )
				con.setAutoCommit(false);
		} 
		catch (SQLException e)
		{
			e.printStackTrace();
			throw new DBException(e.getMessage());
		} 
		catch (ClassNotFoundException e)
		{
			e.printStackTrace();
			throw new DBException(e.getMessage());
		}
	}
	//MSsNnmjeGpmY5G3
	
	@Override
  public void cleanup() throws DBException 
	{
		completePendingBatches(null);
	}
	
	@Override
  public Status delete(String table, String key) 
	{
		try
		{
			PreparedStatement stmt = getDeleteStatement(table);
  		completePendingBatches(null);

  		stmt.setString(1, key);
			int recs = stmt.executeUpdate();
			if( 0== recs )
				return Status.NOT_FOUND;
		} 
		catch (SQLException e)
		{
			// log exception
			return Status.ERROR;
		}
		return Status.OK;
	}

	@Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) 
	{
		try
		{
			CachedStatement cs = getInsertStatement(table, values);
  		completePendingBatches(cs);

  		PreparedStatement ps = cs.ps;
			ps.setString(1,key);
			int pos = 1;
			for( String f : cs.fields )
				cs.ps.setString(++pos,values.get(f).toString());

			if( 0< batchsize )
			{
				cs.ps.addBatch();
				cs.batched++;
				
				if(cs.batched >= batchsize)
				{
					cs.batched = 0;
					cs.ps.executeBatch();
				}
			}
			else
				cs.ps.executeUpdate();
			return Status.OK;
		} 
		catch (SQLException e)
		{
			e.printStackTrace();
			return Status.ERROR;
		}
	}
		
	@Override
	public Status update(String table, String key, Map<String, ByteIterator> values)
	{
		try
		{
			CachedStatement cs = getUpdateStatement( table,  values);
  		completePendingBatches(cs);

  		int ii = 0;
			for( String field: cs.fields)		// ensure stable order of fields
				cs.ps.setString(++ii, values.get(field).toString()); // <field> = ?
			cs.ps.setString(++ii, key);		// where <key> = ?
			
			if( batchUpdates && 0< batchsize )
			{
				cs.ps.addBatch();
				cs.batched++;
				
				if(cs.batched >= batchsize)
				{
					cs.batched = 0;
					cs.ps.executeBatch();
				}
			}
			else
			{
				cs.ps.executeUpdate();
			}
			return Status.OK;
		} 
		catch (SQLException e)
		{
			e.printStackTrace();
			return Status.ERROR;
		}
	}

	@Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) 
	{
    try 
    {
      PreparedStatement stmt = getReadStatement(table, fields);
  		completePendingBatches(null);

  		stmt.setString(1, key);
      ResultSet res = stmt.executeQuery();
      if (!res.next()) 
      {
        res.close();
        return Status.NOT_FOUND;
      }
      if (result != null && fields != null) 
      {
        for (String field : fields) 
        {
          String value = res.getString(field);
          result.put(field, new StringByteIterator(value));
        }
      }
      res.close();
      return Status.OK;
    } 
    catch (SQLException e) 
    {
      System.err.println("Error in read of table " + table + ": " + e);
      return Status.ERROR;
    }
  }

	@Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) 
	{
		if( (null== result) || (null== fields) ) return Status.BAD_REQUEST; 
    try 
    {
      PreparedStatement stmt = getScanStatement(table, fields);
  		completePendingBatches(null);


      stmt.setString(1, startkey);
      ResultSet res = stmt.executeQuery();
      
      int cc = 0;
      while( res.next() && (cc < recordcount) ) 
      {
      	HashMap<String, ByteIterator> rec = new HashMap<String, ByteIterator>();
        for (String field : fields) 
        {
          String value = res.getString(field);
          rec.put(field, (ByteIterator)new StringByteIterator(value));
        }
        result.add(rec);
        ++cc;
      }
      res.close();
      return Status.OK;
    } 
    catch (SQLException e) 
    {
      System.err.println("Error in scan of table " + table + ": " + e);
      return Status.ERROR;
    }
	}
	
	PreparedStatement getDeleteStatement(String table) throws SQLException
	{
		if( null== DeleteStatement )
		{
			String sql = String.format("delete from %s where %s = ?", toIdentifier(table), toIdentifier(keyname));
			DeleteStatement = con.prepareStatement(sql);
		}
		
		return DeleteStatement;
	}
	
	CachedStatement getInsertStatement(String table, Map<String, ByteIterator> values) throws SQLException
	{
		CachedStatement sc = StatementsInsert.get(values.keySet());
		if( null== sc )
		{
			StringBuilder flist = new StringBuilder();
			StringBuilder plist = new StringBuilder();
			ArrayList<String> fields = new ArrayList<>();
			flist.append(toIdentifier(keyname));
			plist.append("?");
			for( String key : values.keySet() )
			{
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
			
			StatementsInsert.put(values.keySet(), sc);
		}
		return sc;
	}
	
	CachedStatement getUpdateStatement(String table, Map<String, ByteIterator> values) throws SQLException
	{
		CachedStatement sc = StatementsUpdate.get(values.keySet());
		if( null== sc )
		{
			StringBuilder sql = new StringBuilder();
			ArrayList<String> fields = new ArrayList<>();
			sql.append("update ").append(toIdentifier(tablename)).append(" set ");
			boolean first = true;
			for( String key : values.keySet() )
			{
				fields.add(key);
				
				if( !first )
					sql.append(", ");
				sql.append(key).append(" = ?");
				first = false;
			}
			sql.append(" where ").append(toIdentifier(keyname)).append(" = ?");
			PreparedStatement stmt = con.prepareStatement(sql.toString());

			sc = new CachedStatement();
			sc.fields = fields;
			sc.ps = stmt;
			sc.batched = 0;
			
			StatementsUpdate.put(values.keySet(), sc);
		}
		return sc;
	}
	
	PreparedStatement getReadStatement(String table, Set<String> fields) throws SQLException
	{
		if( null== ReadStatement )
		{
			String sql = String.format("select * from %s where %s = ?", toIdentifier(table), toIdentifier(keyname));
			ReadStatement = con.prepareStatement(sql);
		}
		return ReadStatement;
	}
	
	PreparedStatement getScanStatement(String table, Set<String> fields) throws SQLException
	{
		if( null== ScanStatement )
		{
			String sql = String.format("select * from %s where %s >= ? order by %s", 
					toIdentifier(table), toIdentifier(keyname), toIdentifier(keyname));
			ScanStatement = con.prepareStatement(sql);
		}
		return ScanStatement;
	}

	private CachedStatement lastStatement = null;
	private void completePendingBatches(CachedStatement cs)
	{
		if( null!= lastStatement )
		{
			if( null== cs || lastStatement != cs )
			{
				if( 0< lastStatement.batched )
				{
					try
					{
						lastStatement.ps.executeBatch();
						lastStatement.batched = 0;
					} 
					catch (SQLException e)
					{
						e.printStackTrace();
					}
				}
			}
		}
		if( null!= cs )
			lastStatement = cs;
	}
	
	private static String toIdentifier(String name)
	{
		// for the time being we assume that all names can be used as simple identifiers!!!
		return name;
		//return "\"" + name.replace("\"", "\"\"") + "\"";
	}
	
	private static String toLiteral(String name)
	{
		return "'" + name.replace("'", "''") + "'";
	}
	
	private class CachedStatement 
	{
		public ArrayList<String> fields;
		public PreparedStatement ps;
		public int batched;
	}
}
