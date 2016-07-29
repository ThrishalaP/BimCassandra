package org.bimserver.database;

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
import com.datastax.driver.core.exceptions.ReadFailureException; 
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import com.datastax.driver.core.exceptions.WriteFailureException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
//import com.datastax.driver.core.exceptions.WriteTimeoutException;


public class BimserverLockConflictException extends BimserverDatabaseException {
	private static final long serialVersionUID = 9043339658520339789L;

	//A non-timeout error during a read query.This happens when some of the replicas that were contacted by the coordinator replied with an error.
	public BimserverLockConflictException(ReadFailureException rf) {
		super(rf);
	}
	public ReadFailureException getReadFailureException() {
		return (ReadFailureException) getCause();
	}
	
	//A Cassandra timeout during a read query.
	public BimserverLockConflictException(ReadTimeoutException rt) {
                super(rt);
        }
	public ReadTimeoutException getReadTimeoutException() {
		 return (ReadTimeoutException) getCause();
	}
	
	//A non-timeout error during a write query.This happens when some of the replicas that were contacted by the coordinator replied with an error.
	public BimserverLockConflictException(WriteFailureException wf) {
                super(wf);
        }
	public WriteFailureException getWriteFailureException() {
                return (WriteFailureException) getCause();
        }
	
	//A Cassandra timeout during a write query.
	public BimserverLockConflictException(WriteTimeoutException wt) {
                super(wt);
        }
	public WriteTimeoutException getWriteTimeoutException() {
                return (WriteTimeoutException) getCause();
        }

	

}