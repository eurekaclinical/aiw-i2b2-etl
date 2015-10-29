package edu.emory.cci.aiw.neo4jetl;

/*
 * #%L
 * AIW Neo4j ETL
 * %%
 * Copyright (C) 2015 Emory University
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/gpl-3.0.html>.
 * #L%
 */
import edu.emory.cci.aiw.neo4jetl.config.Configuration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.SynchronousQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.protempa.DataSource;
import org.protempa.PropositionDefinition;
import org.protempa.dest.QueryResultsHandler;
import org.protempa.dest.QueryResultsHandlerCloseException;
import org.protempa.dest.QueryResultsHandlerInitException;
import org.protempa.dest.QueryResultsHandlerProcessingException;
import org.protempa.dest.QueryResultsHandlerValidationFailedException;
import org.protempa.proposition.Proposition;
import org.protempa.proposition.UniqueId;
import org.protempa.query.Query;

/**
 * Because Neo4j's Java API binds transactions to a thread, and Protempa's APIs
 * may call start, handleQueryResult and finish from different threads, we wrap
 * the actual query results handler in this class, which calls start,
 * handleQueryResult and finish from a dedicated single thread.
 *
 * @author Andrew Post
 */
public class Neo4jQueryResultsHandler implements QueryResultsHandler {

	private static final Logger LOGGER = Logger.getLogger(Neo4jQueryResultsHandler.class.getName());
	private final Neo4jQueryResultsHandlerWrapped wrapped;
	private final SynchronousQueue startSynchronousQueue;
	private final SynchronousQueue endSynchronousQueue;
	private final Object handoffObject;
	private volatile Collection<PropositionDefinition> cache;
	private volatile boolean processing;
	private volatile String keyId;
	private volatile List<Proposition> propositions;
	private volatile Map<Proposition, List<Proposition>> forwardDerivations;
	private volatile Map<Proposition, List<Proposition>> backwardDerivations;
	private volatile Map<UniqueId, Proposition> references;
	private volatile boolean hasPassedAfterStart;

	private final Thread wrappedThread;
	private volatile QueryResultsHandlerProcessingException exception;
	private volatile boolean atEnd;
	private final Query query;

	Neo4jQueryResultsHandler(Query inQuery, DataSource dataSource, Configuration configuration) throws QueryResultsHandlerInitException {
		this.wrapped = new Neo4jQueryResultsHandlerWrapped(inQuery, dataSource, configuration);
		this.startSynchronousQueue = new SynchronousQueue();
		this.endSynchronousQueue = new SynchronousQueue();
		this.handoffObject = new Object();

		this.wrappedThread = new Thread() {

			@Override
			public void run() {
				try {
					//Start step
					startSynchronousQueue.take();
					doStart();
					endSynchronousQueue.put(handoffObject);
					//After-start step
					startSynchronousQueue.take();
					endSynchronousQueue.put(handoffObject);
					//Handle query result step
					while (true) {
						startSynchronousQueue.take();
						if (!processing) {
							break;
						}
						doHandleQueryResult();
						endSynchronousQueue.put(handoffObject);
					}

					//Finish step
					doFinish();
					atEnd = true;
					endSynchronousQueue.put(handoffObject);
				} catch (QueryResultsHandlerProcessingException ex) {
					exception = ex;
					atEnd = true;
					try {
						endSynchronousQueue.put(handoffObject);
					} catch (InterruptedException ignore) {
					}
				} catch (InterruptedException ex) {
					LOGGER.log(Level.FINE, "Result processing interrupted", ex);
					atEnd = true;
					try {
						endSynchronousQueue.put(handoffObject);
					} catch (InterruptedException ignore) {
					}
				}
			}

			private void doStart() throws QueryResultsHandlerProcessingException {
				wrapped.start(cache);
			}

			private void doHandleQueryResult() throws QueryResultsHandlerProcessingException {
				wrapped.handleQueryResult(keyId, propositions, forwardDerivations, backwardDerivations, references);
			}

			private void doFinish() throws QueryResultsHandlerProcessingException {
				wrapped.finish();
			}

		};
		this.wrappedThread.start();
		this.query = inQuery;
	}

	@Override
	public void validate() throws QueryResultsHandlerValidationFailedException {
		this.wrapped.validate();
	}

	@Override
	public void start(final Collection<PropositionDefinition> cache) throws QueryResultsHandlerProcessingException {
		this.cache = cache;
		this.processing = true;
		this.hasPassedAfterStart = false;

		try {
			doExecuteStep();
			doCheckForException();
		} catch (InterruptedException ex) {
			LOGGER.log(Level.FINE, "Data processing interrupted", ex);
		}
	}

	@Override
	public void handleQueryResult(String keyId, List<Proposition> propositions, Map<Proposition, List<Proposition>> forwardDerivations, Map<Proposition, List<Proposition>> backwardDerivations, Map<UniqueId, Proposition> references) throws QueryResultsHandlerProcessingException {
		try {
			doPassAfterStartIfNeeded();
			this.keyId = keyId;
			this.propositions = propositions;
			this.forwardDerivations = forwardDerivations;
			this.backwardDerivations = backwardDerivations;
			this.references = references;
			doExecuteStep();
			doCheckForException();
		} catch (InterruptedException ex) {
			LOGGER.log(Level.FINE, "Data processing interrupted", ex);
		}
	}

	@Override
	public void finish() throws QueryResultsHandlerProcessingException {
		LOGGER.log(Level.FINE, "Neo4jQueryResultsHandler finishing up for query {0}...", this.query.getName());
		try {
			this.processing = false;
			doGoToEnd();
			doCheckForException();
		} catch (InterruptedException ex) {
			LOGGER.log(Level.FINE, "Data processing interrupted", ex);
		}
		LOGGER.log(Level.FINE, "Neo4jQueryResultsHandler finished up for query {0}...", this.query.getName());
	}

	@Override
	public void close() throws QueryResultsHandlerCloseException {
		LOGGER.log(Level.FINE, "Neo4jQueryResultsHandler closing for query {0}...", this.query.getName());
		this.processing = false;
		if (!this.atEnd) {
			try {
				doGoToEnd();
			} catch (InterruptedException ex) {
				LOGGER.log(Level.FINE, "Data processing interrupted", ex);
			}
		}
		try {
			LOGGER.log(Level.FINE, "Neo4jQueryResultsHandler waiting for results handling thread to end for query {0}", this.query.getName());
			this.wrappedThread.join();
			LOGGER.log(Level.FINE, "Neo4jQueryResultsHandler results handling thread for query {0} is finished", this.query.getName());
		} catch (InterruptedException ex) {
			LOGGER.log(Level.FINE, "Data processing interrupted", ex);
		}
		boolean thrown = true;
		try {
			doCheckForException();
			thrown = false;
		} catch (QueryResultsHandlerProcessingException ex) {
			throw new QueryResultsHandlerCloseException(ex);
		} finally {
			try {
				this.wrapped.close();
			} catch (QueryResultsHandlerCloseException ex) {
				if (!thrown) {
					throw ex;
				}
			}
		}
		LOGGER.log(Level.FINE, "Neo4jQueryResultsHandler closed for query {0}", this.query.getName());
	}

	@Override
	public void cancel() {
		this.wrappedThread.interrupt();
	}
	
	private void doGoToEnd() throws InterruptedException {
		doPassAfterStartIfNeeded(); //Needed if there are no results to handle.
		doExecuteStep();
	}

	private void doExecuteStep() throws InterruptedException {
		this.startSynchronousQueue.put(this.handoffObject);
		this.endSynchronousQueue.take();

	}

	private void doPassAfterStartIfNeeded() throws InterruptedException {
		if (!this.hasPassedAfterStart) {
			doExecuteStep();
			this.hasPassedAfterStart = true;
		}
	}

	private void doCheckForException() throws QueryResultsHandlerProcessingException {
		if (this.exception != null) {
			QueryResultsHandlerProcessingException throwable = this.exception;
			this.exception = null;
			throw throwable;
		}
	}

}
