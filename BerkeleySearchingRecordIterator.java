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

import java.util.Arrays;

import org.bimserver.database.BimserverLockConflictException;
import org.bimserver.database.Record;
import org.bimserver.database.SearchingRecordIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.datastax.driver.core.exceptions.ReadFailureException;
import com.datastax.driver.core.exceptions.WriteFailureException;

import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

public class BerkeleySearchingRecordIterator implements SearchingRecordIterator {

	private static final Logger LOGGER = LoggerFactory.getLogger(BerkeleySearchingRecordIterator.class);
	private final Cursor cursor;
	private final byte[] mustStartWith;
	private byte[] nextStartSearchingAt;
	private long cursorId;
	private BerkeleyKeyValueStore berkeleyKeyValueStore;

	public BerkeleySearchingRecordIterator(Cursor cursor, BerkeleyKeyValueStore berkeleyKeyValueStore, long cursorId, byte[] mustStartWith, byte[] startSearchingAt) throws BimserverLockConflictException {
		this.cursor = cursor;
		this.berkeleyKeyValueStore = berkeleyKeyValueStore;
		this.cursorId = cursorId;
		this.mustStartWith = mustStartWith;
		this.nextStartSearchingAt = startSearchingAt;
	}

	public long getCursorId() {
		return cursorId;
	}
	
	private Record getFirstNext(byte[] startSearchingAt) throws BimserverLockConflictException {
		this.nextStartSearchingAt = null;
		DatabaseEntry key = new DatabaseEntry(startSearchingAt);
		DatabaseEntry value = new DatabaseEntry();
		try {
			OperationStatus next = cursor.getSearchKeyRange(key, value, LockMode.DEFAULT);
			if (next == OperationStatus.SUCCESS) {
				byte[] firstBytes = new byte[mustStartWith.length];
				System.arraycopy(key.getData(), 0, firstBytes, 0, mustStartWith.length);
				if (Arrays.equals(firstBytes, mustStartWith)) {
					return new BerkeleyRecord(key, value);
				}
			}
		} catch (ReadFailureException e) {
			throw new BimserverLockConflictException(e);
		}
			catch (WriteFailureException e) {
				throw new BimserverLockConflictException(e);
		} catch (DatabaseException e) {
			LOGGER.error("", e);
		}
		return null;
	}

	@Override
	public Record next() throws BimserverLockConflictException {
		if (nextStartSearchingAt != null) {
			return getFirstNext(nextStartSearchingAt);
		}
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry value = new DatabaseEntry();
		try {
			OperationStatus next = cursor.getNext(key, value, LockMode.DEFAULT);
			if (next == OperationStatus.SUCCESS) {
				byte[] firstBytes = new byte[mustStartWith.length];
				System.arraycopy(key.getData(), 0, firstBytes, 0, mustStartWith.length);
				if (Arrays.equals(firstBytes, mustStartWith)) {
					return new BerkeleyRecord(key, value);
				}
			}
		} catch (ReadFailureException e) {
			throw new BimserverLockConflictException(e);
		}
		 catch (WriteFailureException e) {
				throw new BimserverLockConflictException(e);
		} catch (DatabaseException e) {
			LOGGER.error("", e);
		}
		return null;
	}

	@Override
	public void close() {
		try {
			cursor.close();
			//berkeleyKeyValueStore.removeOpenCursor(cursorId);
		} catch (DatabaseException e) {
			LOGGER.error("", e);
		}
	}

	@Override
	public Record next(byte[] startSearchingAt) throws BimserverLockConflictException {
		return getFirstNext(startSearchingAt);
	}

	@Override
	public Record last() throws BimserverLockConflictException {
		if (nextStartSearchingAt != null) {
			return getFirstNext(nextStartSearchingAt);
		}
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry value = new DatabaseEntry();
		try {
			OperationStatus next = cursor.getLast(key, value, LockMode.DEFAULT);
			if (next == OperationStatus.SUCCESS) {
				byte[] firstBytes = new byte[mustStartWith.length];
				System.arraycopy(key.getData(), 0, firstBytes, 0, mustStartWith.length);
				if (Arrays.equals(firstBytes, mustStartWith)) {
					return new BerkeleyRecord(key, value);
				}
			}
		} catch (ReadFailureException e) {
			throw new BimserverLockConflictException(e);
		}
		catch (WriteFailureException e) {
			throw new BimserverLockConflictException(e);
		} catch (DatabaseException e) {
			LOGGER.error("", e);
		}
		return null;
	}
}