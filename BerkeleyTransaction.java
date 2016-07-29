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

import org.bimserver.BimserverDatabaseException;
import org.bimserver.database.BimTransaction;
import org.bimserver.database.BimserverLockConflictException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.ReadFailureException;
import com.datastax.driver.core.exceptions.WriteFailureException;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Cluster;
//import com.sleepycat.je.Transaction;

public class BerkeleyTransaction implements BimTransaction {

	private static final Logger LOGGER = LoggerFactory.getLogger(BerkeleyTransaction.class);
	private final Session transaction;
	private boolean transactionAlive = true;

	public BerkeleyTransaction(Session transaction) {
		this.transaction = transaction;
	}

	public Session getTransaction() {
		return transaction;
	}

/*	@Override
	//public void setName(String name) {
		//this.transaction = name;
	//}*/

	@Override
	public void close() {
		if (transactionAlive) {
			rollback();
		}
	}

	@Override
	public void rollback() {
		try {
			transaction.close();
			transactionAlive = false;
		} catch (DriverException e) {
			LOGGER.error("", e);
		}
	}

	@Override
	public void commit() throws BimserverLockConflictException, BimserverDatabaseException {
		Cluster cluster =  transaction.getCluster();
		try {
		    cluster.builder();
			transaction.close();
			transactionAlive = false;
		} catch (ReadFailureException e) {
			throw new BimserverLockConflictException(e);
		} catch (WriteFailureException e) {
			throw new BimserverLockConflictException(e);
		} catch (DriverException e) {
			throw new BimserverDatabaseException(e);
		}
	}

	@Override
	public String getLoggedKeyspace() {
		return transaction.getLoggedKeyspace();
	}

	public Session getSession() {
		// TODO Auto-generated method stub
		return transaction;
	}
}