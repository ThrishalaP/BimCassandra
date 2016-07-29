package org.bimserver.database.berkeley;

/******************************************************************************
 * Copyright (C) 2009-2016  BIMserver.org
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see {@literal<http://www.gnu.org/licenses/>}.
 *****************************************************************************/

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.lang.Object;

import org.bimserver.BimserverDatabaseException;
import org.bimserver.database.BimTransaction;
import org.bimserver.database.BimserverLockConflictException;
import org.bimserver.database.DatabaseSession;
import org.bimserver.database.KeyValueStore;
import org.bimserver.database.Record;
import org.bimserver.database.RecordIterator;
import org.bimserver.database.SearchingRecordIterator;
import org.bimserver.utils.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.exceptions.ReadTimeoutException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.utils.Bytes;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.DriverException;

//import com.sleepycat.je.Cursor;
//import com.sleepycat.je.CursorConfig;
//import com.sleepycat.je.Database;
//import com.sleepycat.je.DatabaseConfig;
//import com.sleepycat.je.DatabaseEntry;
//import com.sleepycat.je.DatabaseException;
//import com.sleepycat.je.Environment;
//import com.sleepycat.je.EnvironmentConfig;
//import com.sleepycat.je.EnvironmentLockedException;
//import com.sleepycat.je.JEVersion;
//import com.sleepycat.je.LockConflictException;
//import com.sleepycat.je.LockMode;
//import com.sleepycat.je.OperationStatus;
//import com.sleepycat.je.Transaction;
//import com.sleepycat.je.TransactionConfig;


public class BerkeleyKeyValueStore implements KeyValueStore {

	private static final Logger LOGGER = LoggerFactory.getLogger(BerkeleyKeyValueStore.class);
	private static Cluster cluster;
	private static Session session;
	public static final String mykeyspace="keyspace";
	private long committedWrites;
	private long reads;
	private final Map<String, TableWrapper> tables = new HashMap<>();
	private boolean isNew;
	//private Environment environment;
	//private TransactionConfig transactionConfig;
	//private CursorConfig safeCursorConfig;
	private long lastPrintedReads = 0;
	private long lastPrintedCommittedWrites = 0;
	private static final boolean MONITOR_CURSOR_STACK_TRACES = false;
	//private final AtomicLong cursorCounter = new AtomicLong();
	//private final Map<Long, StackTraceElement[]> openCursors = new ConcurrentHashMap<>();
	private boolean useTransactions = true;
	//private CursorConfig unsafeCursorConfig;
	
	public BerkeleyKeyValueStore(Path dataDir) throws DatabaseInitException, NoHostAvailableException {
		
		
		if (Files.isDirectory(dataDir)) {
			try {
				if (PathUtils.list(dataDir).size() > 0) {
					LOGGER.info("Non-empty database directory found \"" + dataDir.toString() + "\"");
					isNew = false;
				} else {
					LOGGER.info("Empty database directory found \"" + dataDir.toString() + "\"");
					isNew = true;
				}
			} catch (IOException e) {
				LOGGER.error("", e);
			}
		} else {
			isNew = true;
			LOGGER.info("No database directory found, creating \"" + dataDir.toString() + "\"");
			try {
				Files.createDirectory(dataDir);
				LOGGER.info("Successfully created database dir \"" + dataDir.toString() + "\"");
			} catch (Exception e) {
				LOGGER.error("Error creating database dir \"" + dataDir.toString() + "\"");
			}
		}
		
		PoolingOptions poolingOptions = new PoolingOptions();
		// customize options...
		poolingOptions
	    .setCoreConnectionsPerHost(HostDistance.LOCAL,  4)
	    .setMaxConnectionsPerHost( HostDistance.LOCAL, 10)
	    .setCoreConnectionsPerHost(HostDistance.REMOTE, 2)
	    .setMaxConnectionsPerHost( HostDistance.REMOTE, 4);

		poolingOptions
	    .setConnectionsPerHost(HostDistance.LOCAL,  4, 10)
	    .setConnectionsPerHost(HostDistance.REMOTE, 2, 4);

		
		//when a connection has been idle for a given amount of time,
		//the driver will simulate activity by writing a dummy request to it.
		//This feature is enabled by default. The default heart beat interval is 30 seconds
		poolingOptions.setHeartbeatIntervalSeconds(60);

//		//comment it once the whole file is changed to cassandra implemention
//		EnvironmentConfig envConfig = new EnvironmentConfig();
//		envConfig.setCachePercent(50);
//		envConfig.setAllowCreate(true);
//		envConfig.setTransactional(useTransactions);
//		envConfig.setTxnTimeout(10, TimeUnit.SECONDS);
//		envConfig.setLockTimeout(2000, TimeUnit.MILLISECONDS);
//		envConfig.setConfigParam(EnvironmentConfig.CHECKPOINTER_HIGH_PRIORITY, "true");
//		envConfig.setConfigParam(EnvironmentConfig.CLEANER_THREADS, "5");
		
		try {

			//change the IP address to the Ip address where docker is running
			Cluster cluster = Cluster.builder().addContactPoint("172.17.0.2").withPoolingOptions(poolingOptions)
			    .build();
			
			
//			environment = new Environment(dataDir.toFile(), envConfig);
		} catch (NoHostAvailableException h){
			String message = "check host connection (" + h.getMessage() + ")" ;
		}
		
//		catch (EnvironmentLockedException e) {
//			String message = "Environment locked exception. Another process is using the same database, or the current user has no write access (database location: \""
//					+ dataDir.toString() + "\")";
//			throw new DatabaseInitException(message);
//		} catch (DatabaseException e) {
//			String message = "A database initialisation error has occured (" + e.getMessage() + ")";
//			throw new DatabaseInitException(message);
//		}
//		
//		transactionConfig = new TransactionConfig();
//		transactionConfig.setReadCommitted(true);
//
//		safeCursorConfig = new CursorConfig();
//		safeCursorConfig.setReadCommitted(true);
//
//		unsafeCursorConfig = new CursorConfig();
//		unsafeCursorConfig.setReadUncommitted(true);
	}

	public boolean isNew() {
		return isNew;
	}

	public BimTransaction startTransaction() {
		if (useTransactions) {
			try {
				return new BerkeleyTransaction(session.init());
			} catch (DriverException e) {
				LOGGER.error("", e);
			}
		}
		return null;
	}

	public boolean createTable(String tableName, DatabaseSession databaseSession, boolean transactional) throws BimserverDatabaseException {
		if (tables.containsKey(tableName)) {
			throw new BimserverDatabaseException("Table " + tableName + " already created");
		}
//		DatabaseConfig databaseConfig = new DatabaseConfig();
//		databaseConfig.setAllowCreate(true);
		boolean finalTransactional = session.isClosed();
//		databaseConfig.setDeferredWrite(!finalTransactional);
//		databaseConfig.setTransactional(finalTransactional);
//		databaseConfig.setSortedDuplicates(false);
//		Database database = environment.openDatabase(null, tableName, databaseConfig);
		//select a keyspace created
		Metadata metadata =cluster.getMetadata();
		System.out.printf("Connected to cluster : %s \n",metadata.getClusterName());
		KeyspaceMetadata database = metadata.getKeyspace(mykeyspace);
	
		
		session = cluster.connect(); 
		
		
		if (database == null) {
			return false;
		}
		tables.put(tableName, new TableWrapper(database, finalTransactional));
		
		return true;
	}

	public boolean createIndexTable(String tableName, DatabaseSession databaseSession, boolean transactional) throws BimserverDatabaseException {
		if (tables.containsKey(tableName)) {
			throw new BimserverDatabaseException("Table " + tableName + " already created");
		}
//		DatabaseConfig databaseConfig = new DatabaseConfig();
//		databaseConfig.setAllowCreate(true);
		boolean finalTransactional = session.isClosed();
//		databaseConfig.setDeferredWrite(!finalTransactional);
//		databaseConfig.setTransactional(finalTransactional);
//		databaseConfig.setSortedDuplicates(true);
//		Database database = environment.openDatabase(null, tableName, databaseConfig);
		Metadata metadata =cluster.getMetadata();
//		System.out.printf("Connected to cluster : %s \n",metadata.getClusterName());
		session = cluster.connect();
		KeyspaceMetadata database = metadata.getKeyspace(mykeyspace);
		if (database == null) {
			return false;
		}
		tables.put(tableName, new TableWrapper(database, finalTransactional));
		
		return true;
	}
	
	public boolean openTable(String tableName, boolean transactional) throws BimserverDatabaseException {
		if (tables.containsKey(tableName)) {
			throw new BimserverDatabaseException("Table " + tableName + " already opened");
		}
//		DatabaseConfig databaseConfig = new DatabaseConfig();
//		databaseConfig.setAllowCreate(false);
		boolean finalTransactional = session.isClosed();
//		databaseConfig.setDeferredWrite(!finalTransactional);
//		databaseConfig.setTransactional(finalTransactional);
//		databaseConfig.setSortedDuplicates(false);
//		Database database = environment.openDatabase(null, tableName, databaseConfig);
		Metadata metadata =cluster.getMetadata();
//		System.out.printf("Connected to cluster : %s \n",metadata.getClusterName());
		session = cluster.connect();
		KeyspaceMetadata database = metadata.getKeyspace(mykeyspace);
		if (database == null) {
			throw new BimserverDatabaseException("Table " + tableName + " not found in database");
		}
		tables.put(tableName, new TableWrapper(database, finalTransactional));
		return true;
	}

	public void openIndexTable(String tableName, boolean transactional) throws BimserverDatabaseException {
		if (tables.containsKey(tableName)) {
			throw new BimserverDatabaseException("Table " + tableName + " already opened");
		}
//		DatabaseConfig databaseConfig = new DatabaseConfig();
//		databaseConfig.setAllowCreate(false);
		boolean finalTransactional = session.isClosed();
//		databaseConfig.setDeferredWrite(!finalTransactional);
//		databaseConfig.setTransactional(finalTransactional);
//		databaseConfig.setSortedDuplicates(true);
//		Database database = environment.openDatabase(null, tableName, databaseConfig);
		Metadata metadata =cluster.getMetadata();
//		System.out.printf("Connected to cluster : %s \n",metadata.getClusterName());
		session = cluster.connect();
		KeyspaceMetadata database = metadata.getKeyspace(mykeyspace);
		if (database == null) {
			throw new BimserverDatabaseException("Table " + tableName + " not found in database");
		}
		tables.put(tableName, new TableWrapper(database, finalTransactional));
	}
	
	private KeyspaceMetadata getDatabase(String tableName) throws BimserverDatabaseException {
		return getTableWrapper(tableName).getDatabase();
	}

	private TableWrapper getTableWrapper(String tableName) throws BimserverDatabaseException {
		TableWrapper tableWrapper = tables.get(tableName);
		if (tableWrapper == null) {
			throw new BimserverDatabaseException("Table " + tableName + " not found");
		}
		return tableWrapper;
	}

	private Session getTransaction(DatabaseSession databaseSession) {
		if (databaseSession != null) {
			BerkeleyTransaction BerkeleyTransaction = (BerkeleyTransaction) databaseSession.getBimTransaction();
			if (BerkeleyTransaction != null) {
				return BerkeleyTransaction.getSession();
			}
		}
		return null;
	}

	public void close() {
		for (TableWrapper tableWrapper : tables.values()) {
			try {
				tableWrapper.getDatabase().equals(session.closeAsync());
			} catch (DriverException e) {
				LOGGER.error("", e);
			}
		}
		if ( tables.isEmpty()) {
			try {
				session.close();
			} catch (DriverException e) {
				LOGGER.error("", e);
			}
		}
	}

//	public LockMode getLockMode(TableWrapper tableWrapper) {
//		if (tableWrapper.isTransactional()) {
//			return LockMode.READ_COMMITTED;
//		} else {
//			return LockMode.READ_UNCOMMITTED;
//		}
//	}
	
	public Session getTransaction(DatabaseSession databaseSession, TableWrapper tableWrapper) {
		return tableWrapper.isTransactional() ? getTransaction(databaseSession) : null;
	}
	
//	public CursorConfig getCursorConfig(TableWrapper tableWrapper) {
//		if (tableWrapper.isTransactional()) {
//			return safeCursorConfig;
//		} else {
//			return unsafeCursorConfig;
//		}
//	}
	
	@Override
	public byte[] get(String tableName, byte[] keybytes, DatabaseSession databaseSession) throws BimserverDatabaseException {
				
		String key =  Bytes.toHexString(keybytes);
//		DatabaseEntry key = new DatabaseEntry(keyBytes);
//		DatabaseEntry value = new DatabaseEntry();
		try {
				TableWrapper value = getTableWrapper(tableName);
				ResultSet r = session.execute(key, value);
				List<Row> rs =	r.all();
				byte[] rs1= Bytes.getArray((ByteBuffer) rs);
				
				return rs1;
//			OperationStatus operationStatus = tableWrapper.getDatabase().get(getTransaction(databaseSession, tableWrapper), key, value, getLockMode(tableWrapper));
//			if (operationStatus == OperationStatus.SUCCESS) {
//				return value.getData();
//			}
		} catch (DriverException e) {
			LOGGER.error("", e);
		}
		return null;
	}

	@Override
	public List<byte[]> getDuplicates(String tableName,byte[] keybytes, DatabaseSession databaseSession) throws BimserverDatabaseException {
//      DatabaseEntry key = new DatabaseEntry(keyBytes);
//      DatabaseEntry value = new DatabaseEntry();
      try {
          TableWrapper value = getTableWrapper(tableName);
          String key =  Bytes.toHexString(keybytes);
          
          
          ResultSet r = session.execute(key, value);
          //Cursor cursor = tableWrapper.getDatabase().openCursor(getTransaction(databaseSession, tableWrapper), getCursorConfig(tableWrapper));
          List<Row> rs= r.all();
          
          
          byte[] rs1= Bytes.getArray((ByteBuffer) rs);
          try {
              //OperationStatus operationStatus = cursor.getSearchKey(key, value, LockMode.DEFAULT);
              
              List<Row> result = new ArrayList<Row>();
              //while (operationStatus == OperationStatus.SUCCESS) {
              while(result.isEmpty())
              {
                  result.addAll(rs);
              }
              //result is converted from List<Row> to byte[]
              byte[] res1 = Bytes.getArray((ByteBuffer) result);
              //haveto convert byte[] to list<byte[]>
              List<byte[]> res = Arrays.asList(res1); 
              
              //operationStatus = cursor.getNextDup(key, value, LockMode.DEFAULT);
              //}
              return res; //this is returning byte[] but it shld return list bytearray
          } finally {
              session.close();
          }
      } catch (DriverException e) {
          LOGGER.error("", e);
      }
      return null;
  }

	public long getTotalWrites() {
		return committedWrites;
	}

	public void sync() {
		try {
			session.closeAsync();
//			environment.sync();
//			environment.flushLog(true);
//			environment.evictMemory();
		} catch (DriverException e) {
			LOGGER.error("", e);
		}
	}

	@Override
	public boolean containsTable(String tableName) {
		try {
				return tables.containsKey(tableName);
			//return environment.getDatabaseNames().contains(tableName);
		} catch (DriverException e) {
			LOGGER.error("", e);
		}
		return false;
	}

	@Override
	public RecordIterator getRecordIterator(String tableName, DatabaseSession databaseSession) throws BimserverDatabaseException {
		//Cursor cursor = null;
		try {
			TableWrapper tableWrapper = getTableWrapper(tableName);
			
			//cursor = tableWrapper.getDatabase().openCursor(getTransaction(databaseSession, tableWrapper), getCursorConfig(tableWrapper));
//			BerkeleyRecordIterator BerkeleyRecordIterator = new BerkeleyRecordIterator(cursor, this, cursorCounter.incrementAndGet());
//			if (MONITOR_CURSOR_STACK_TRACES) {
//				openCursors.put(BerkeleyRecordIterator.getCursorId(), new Exception().getStackTrace());
//			}
//			return BerkeleyRecordIterator;
		} catch (DriverException e) {
			LOGGER.error("", e);
		}
		return null;
	}

	@Override
	public SearchingRecordIterator getRecordIterator(String tableName, byte[] mustStartWith, byte[] startSearchingAt, DatabaseSession databaseSession) throws BimserverLockConflictException, BimserverDatabaseException {
//		Cursor cursor = null;
		try {
			TableWrapper tableWrapper = getTableWrapper(tableName);
//			cursor = tableWrapper.getDatabase().openCursor(getTransaction(databaseSession, tableWrapper), getCursorConfig(tableWrapper));
//			BerkeleySearchingRecordIterator BerkeleySearchingRecordIterator = new BerkeleySearchingRecordIterator(cursor, this, cursorCounter.incrementAndGet(), mustStartWith, startSearchingAt);
//			if (MONITOR_CURSOR_STACK_TRACES) {
//				openCursors.put(BerkeleySearchingRecordIterator.getCursorId(), new Exception().getStackTrace());
//			}
//			return BerkeleySearchingRecordIterator;
		} catch (BimserverLockConflictException e) {
			if (session != null) {
				try {
					session.close();
					throw e;
				} catch (DriverException e1) {
					LOGGER.error("", e1);
				}
			}
		} catch (DriverException e1) {
			LOGGER.error("", e1);
		}
		return null;
	}

	@Override
	public long count(String tableName) {
		try {
			return getDatabase(tableName).getTables().size();
		} catch (DriverException e) {
			LOGGER.error("", e);
		} catch (BimserverDatabaseException e) {
			LOGGER.error("", e);
		}
		return -1;
	}

	@Override
	public byte[] getFirstStartingWith(String tableName, byte[] key, DatabaseSession databaseSession) throws BimserverLockConflictException, BimserverDatabaseException {
		SearchingRecordIterator recordIterator = getRecordIterator(tableName, key, key, databaseSession);
		try {
			Record record = recordIterator.next(key);
			if (record == null) {
				return null;
			}
			return record.getValue();
		} finally {
			recordIterator.close();
		}
	}

	public void delete(String tableName, byte[] key, DatabaseSession databaseSession) throws BimserverLockConflictException {
		//DatabaseEntry entry = new DatabaseEntry(key);
		try {
			//TableWrapper tableWrapper = getTableWrapper(tableName);
			//tableWrapper.getDatabase().delete(getTransaction(databaseSession, tableWrapper), entry);
			if(containsTable(tableName)){
			delete(tableName, key, databaseSession);
			}else
			{
				System.out.println("Table not found \n");
				
			}
		}catch (ReadTimeoutException rt) {
		    throw new BimserverLockConflictException(rt);
	    }catch (WriteTimeoutException wt) {
	    throw new BimserverLockConflictException(wt);
	    } catch (DriverException e) {
			LOGGER.error("", e);
		} catch (UnsupportedOperationException e) {
			LOGGER.error("", e);
		} catch (IllegalArgumentException e) {
			LOGGER.error("", e);
		} catch (BimserverDatabaseException e) {
			LOGGER.error("", e);
		}
	}
	
	@Override
	public void delete(String indexTableName, byte[] featureBytesOldIndex, byte[] array, DatabaseSession databaseSession) throws BimserverLockConflictException {
		try {
			//continue from here
			TableWrapper tableWrapper = getTableWrapper(indexTableName);
			String dbkey =  Bytes.toHexString(featureBytesOldIndex);
			String v =  Bytes.toHexString(array);
			TableWrapper value = getTableWrapper(v);
			//Cursor cursor = tableWrapper.getDatabase().openCursor(getTransaction(databaseSession, tableWrapper), getCursorConfig(tableWrapper));
			ResultSet r = session.execute(dbkey, value);
			try {
//				if (cursor.getSearchBoth(new DatabaseEntry(featureBytesOldIndex), new DatabaseEntry(array), LockMode.DEFAULT) == OperationStatus.SUCCESS) {
//					cursor.delete();
//				}
			} finally {
				session.close();
			}
		} catch (ReadTimeoutException rt) {
		    throw new BimserverLockConflictException(rt);
	    }catch (WriteTimeoutException wt) {
	    	throw new BimserverLockConflictException(wt);
	    }catch (DriverException e) {
			LOGGER.error("", e);
		} catch (UnsupportedOperationException e) {
			LOGGER.error("", e);
		} catch (IllegalArgumentException e) {
			LOGGER.error("", e);
		} catch (BimserverDatabaseException e) {
			LOGGER.error("", e);
		}
	}

	@Override
	public String getLocation() {
		final Metadata metadata = cluster.getMetadata();
		try{
			for (final Host host : metadata.getAllHosts())
			{
				return host.getAddress().toString();
				
			}
			//return environment.getHome().getAbsolutePath();
		} catch (DriverException e) {
			LOGGER.error("", e);
		}
		return "unknown";
	}

//	@Override
//	public String getStats() {
//		try {
//			return environment.getStats(null).toString();
//		} catch (DatabaseException e) {
//			LOGGER.error("", e);
//		}
//		return null;
//	}

	@Override
	public void commit(DatabaseSession databaseSession) throws BimserverLockConflictException, BimserverDatabaseException {
		Session bdbTransaction = getTransaction(databaseSession);
		try {
			bdbTransaction.close();
		} catch (ReadTimeoutException rt) {
		    throw new BimserverLockConflictException(rt);
	    }catch (WriteTimeoutException wt) {
	    throw new BimserverLockConflictException(wt);
	    }catch (DriverException e) {
			throw new BimserverDatabaseException("", e);
		}
	}

	@Override
	public void store(String tableName, byte[] key, byte[] value, DatabaseSession databaseSession) throws BimserverDatabaseException, BimserverLockConflictException {
		store(tableName, key, value, 0, value.length, databaseSession);
	}
	
	@Override
	public void store(String tableName, byte[] key, byte[] value, int offset, int length, DatabaseSession databaseSession) throws BimserverDatabaseException, BimserverLockConflictException {
//		DatabaseEntry dbKey = new DatabaseEntry(key);
//		DatabaseEntry dbValue = new DatabaseEntry(value, offset, length);
		
		try {
			TableWrapper tableWrapper = getTableWrapper(tableName);
			String dbkey =  Bytes.toHexString(key);
			//tableWrapper.getDatabase().put(getTransaction(databaseSession, tableWrapper), dbKey, dbValue);
			ResultSet r = session.execute(dbkey, value);
		} catch (ReadTimeoutException rt) {
		    throw new BimserverLockConflictException(rt);
	    }catch (WriteTimeoutException wt) {
	    throw new BimserverLockConflictException(wt);
	    }catch (DriverException e) {
			throw new BimserverDatabaseException("", e);
		}
	}

	@Override
	public void storeNoOverwrite(String tableName, byte[] key, byte[] value, DatabaseSession databaseSession) throws BimserverDatabaseException, BimserverLockConflictException, BimserverConcurrentModificationDatabaseException {
		storeNoOverwrite(tableName, key, value, 0, value.length, databaseSession);
	}
	
	@Override
	public void storeNoOverwrite(String tableName, byte[] key, byte[] value, int index, int length, DatabaseSession databaseSession) throws BimserverDatabaseException, BimserverLockConflictException, BimserverConcurrentModificationDatabaseException {
//		DatabaseEntry dbKey = new DatabaseEntry(key);
//		DatabaseEntry dbValue = new DatabaseEntry(value, index, length);
		try {
			TableWrapper tableWrapper = getTableWrapper(tableName);
			String dbkey =  Bytes.toHexString(key);
			//OperationStatus putNoOverwrite = tableWrapper.getDatabase().putNoOverwrite(getTransaction(databaseSession, tableWrapper), dbKey, dbValue);
			ResultSet r = session.execute(dbkey, value);
		
			if (r.isFullyFetched()) {
				ByteBuffer keyBuffer = ByteBuffer.wrap(key);
				if (dbkey.length() == 16) {
					int pid = keyBuffer.getInt();
					long oid = keyBuffer.getLong();
					int rid = -keyBuffer.getInt();
					throw new BimserverConcurrentModificationDatabaseException("Key exists: pid: " + pid + ", oid: " + oid + ", rid: " + rid);
				} else {
					throw new BimserverConcurrentModificationDatabaseException("Key exists: " );
				}
			}
		}catch (ReadTimeoutException rt) {
		    throw new BimserverLockConflictException(rt);
	    }catch (WriteTimeoutException wt) {
	    throw new BimserverLockConflictException(wt);
	    } catch (DriverException e) {
			throw new BimserverDatabaseException("", e);
		}
	}
	
	@Override
	public String getType() {
		 
		return "apache casssandra DB Java Edition " + session.getCluster().getDriverVersion().toString();
	}

	@Override
	public long getDatabaseSizeInBytes() {
		long sizedatabase = 0;
		try {
			sizedatabase = tables.size();
			
		} catch (DriverException e) {
			LOGGER.error("", e);
		}
		return sizedatabase;
	}
	
	public Set<String> getAllTableNames() {
		return new HashSet<String>(tables.keySet());
	}
	
	public synchronized void incrementReads(long reads) {
		this.reads += reads;
		if (this.reads / 100000 != lastPrintedReads) {
			LOGGER.info("reads: " + this.reads);
			lastPrintedReads = this.reads / 100000;
		}
	}
	
	@Override
	public synchronized void incrementCommittedWrites(long committedWrites) {
		this.committedWrites += committedWrites;
		if (this.committedWrites / 100000 != lastPrintedCommittedWrites) {
			LOGGER.info("writes: " + this.committedWrites);
			lastPrintedCommittedWrites = this.committedWrites / 100000;
		}
	}

	@Override
	public String getStats() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void dumpOpenCursors() {
		// TODO Auto-generated method stub
		
	}

	/*@Override
	public List<byte[]> getDuplicates1(String tableName, byte[] keyBytes, DatabaseSession databaseSession)
			throws BimserverDatabaseException {
		return null;
	}*/

//	public void removeOpenCursor(long cursorId) {
//		if (MONITOR_CURSOR_STACK_TRACES) {
//			openCursors.remove(cursorId);
//		}
//	}

	//@Override
//	public void dumpOpenCursors() {
//		for (StackTraceElement[] ste : openCursors.values()) {
//			System.out.println("Open cursor");
//			for (StackTraceElement stackTraceElement : ste) {
//				LOGGER.info("\t" + stackTraceElement.getClassName() + ":" + stackTraceElement.getLineNumber() + "."
//						+ stackTraceElement.getMethodName());
//			}
//		}
//	}
}
