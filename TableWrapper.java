package org.bimserver.database.berkeley;

import com.datastax.driver.core.KeyspaceMetadata;

public class TableWrapper {
	private KeyspaceMetadata database;
	private boolean transactional;

	public TableWrapper(KeyspaceMetadata database, boolean transactional) {
		this.database = database;
		this.transactional = transactional;
	}
	
	public boolean isTransactional() {
		return transactional;
	}
	
	public KeyspaceMetadata getDatabase() {
		return database;
	}
}
