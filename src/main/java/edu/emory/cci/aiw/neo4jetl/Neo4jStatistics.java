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
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;
import org.protempa.dest.Statistics;
import org.protempa.dest.StatisticsException;

/**
 * @author hrathod
 */
public class Neo4jStatistics implements Statistics {

	public static final Label NODE_LABEL = DynamicLabel.label("Statistics");
	public static final String TOTAL_KEYS = "numKeys";
	private final Configuration configuration;
	private final File dbPath;

	public Neo4jStatistics(Configuration configuration) throws StatisticsException {
		this.configuration = configuration;
		try {
			Neo4jHome home = new Neo4jHome(this.configuration.getNeo4jHome());
			this.dbPath = home.getDbPath();
		} catch (IOException ex) {
			throw new StatisticsException(ex);
		}
	}

	@Override
	public int getNumberOfKeys() {
		GraphDatabaseFactory factory = new GraphDatabaseFactory();
		GraphDatabaseService database = factory.newEmbeddedDatabase(this.dbPath.getAbsolutePath());
		int numberOfKeys;
		try (Transaction transaction = database.beginTx()) {
			try {
				try (ResourceIterator<Node> nodeIter = GlobalGraphOperations.at(database).getAllNodesWithLabel(NODE_LABEL).iterator()) {
					Node node = nodeIter.next();
					numberOfKeys = (Integer) node.getProperty(TOTAL_KEYS);
				}
				transaction.success();
			} catch (Exception ex) {
				transaction.failure();
				throw ex;
			}
		} finally {
			database.shutdown();
		}
		return numberOfKeys;
	}

	@Override
	public Map<String, Integer> getCounts() {
		return Collections.emptyMap();
	}

	@Override
	public Map<String, Integer> getCounts(String[] propIds) {
		return Collections.emptyMap();
	}

	@Override
	public Map<String, String> getChildrenToParents() {
		return Collections.emptyMap();
	}

	@Override
	public Map<String, String> getChildrenToParents(String[] propIds) {
		return Collections.emptyMap();
	}
	
	
}
