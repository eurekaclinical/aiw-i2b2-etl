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
import edu.emory.cci.aiw.neo4jetl.config.IndexOnProperty;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.neo4j.graphdb.DynamicLabel;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.MultipleFoundException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.helpers.collection.IteratorUtil;
import org.protempa.DataSource;
import org.protempa.DataSourceReadException;
import org.protempa.KnowledgeSourceReadException;
import org.protempa.PropositionDefinition;
import org.protempa.ProtempaException;
import org.protempa.dest.QueryResultsHandler;
import org.protempa.dest.QueryResultsHandlerCloseException;
import org.protempa.dest.QueryResultsHandlerInitException;
import org.protempa.query.QueryMode;
import org.protempa.dest.QueryResultsHandlerProcessingException;
import org.protempa.dest.QueryResultsHandlerValidationFailedException;
import org.protempa.proposition.Proposition;
import org.protempa.proposition.UniqueId;
import org.protempa.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author hrathod
 */
public class Neo4jQueryResultsHandlerWrapped implements QueryResultsHandler {

	private static final Logger LOGGER
			= LoggerFactory.getLogger(Neo4jQueryResultsHandlerWrapped.class);

	public static final Label NODE_LABEL = DynamicLabel.label("Data");

	public static final int DEFAULT_COMMIT_FREQUENCY = 2000;

	private final Map<UniqueId, Node> nodes;
	private final Map<String, RelationshipType> relations;
	private final Map<String, Derivations.Type> deriveType;
	private final Query query;
	private GraphDatabaseService db;
	private Map<String, PropositionDefinition> cache;
	private final PropositionDefinitionRelationForwardVisitor forwardVisitor;
	private final PropositionDefinitionRelationBackwardVisitor backwardVisitor;
	private final Configuration configuration;
	private Neo4jHome home;
	private final String keyType;
	private Transaction transaction;
	private int count;
	private Set<String> missingPropIds;
	private Set<String> keyTypes;

	Neo4jQueryResultsHandlerWrapped(Query inQuery, DataSource dataSource, Configuration configuration) throws QueryResultsHandlerInitException {
		this.nodes = new HashMap<>();
		this.relations = new HashMap<>();
		this.deriveType = new HashMap<>();
		this.query = inQuery;
		try {
			this.forwardVisitor = new PropositionDefinitionRelationForwardVisitor();
			this.backwardVisitor = new PropositionDefinitionRelationBackwardVisitor();
		} catch (KnowledgeSourceReadException ex) {
			throw new QueryResultsHandlerInitException(ex);
		}
		this.configuration = configuration;
		try {
			this.keyType = dataSource.getKeyType();
		} catch (DataSourceReadException ex) {
			throw new QueryResultsHandlerInitException(ex);
		}
		this.missingPropIds = new HashSet<>();
	}

	@Override
	public void start(Collection<PropositionDefinition> cache) throws QueryResultsHandlerProcessingException {
		this.cache = new HashMap<>();
		for (PropositionDefinition pd : cache) {
			this.cache.put(pd.getId(), pd);
		}
		try {
			this.home = new Neo4jHome(this.configuration.getNeo4jHome());
			this.home.stopServer();
			File dbPath = this.home.getDbPath();
			LOGGER.info("Database path is {}", dbPath);
			GraphDatabaseFactory factory = new GraphDatabaseFactory();
			GraphDatabaseBuilder dbBuilder = factory.newEmbeddedDatabaseBuilder(dbPath.getAbsolutePath());
			if (this.query.getQueryMode() == QueryMode.REPLACE) {
				deleteAll();
			}
			this.db = dbBuilder.newGraphDatabase();
			if (this.query.getQueryMode() != QueryMode.REPLACE) {
				LOGGER.error("Grabbing proposition ids already loaded");
				this.keyTypes = new HashSet<>();
				try (Result distinctTypesResult = this.db.execute("MATCH (node:" + NODE_LABEL.name() + ") RETURN DISTINCT node.__type AS node_type");
						ResourceIterator<Object> columnAs = distinctTypesResult.columnAs("node_type")) {
					while (columnAs.hasNext()) {
						Object next = columnAs.next();
						this.keyTypes.add((String) next);
					}
				}
				LOGGER.error("Found proposition ids {}", this.keyTypes);
			}
		} catch (IOException | InterruptedException | CommandFailedException ex) {
			throw new QueryResultsHandlerProcessingException(ex);
		}
		this.transaction = this.db.beginTx();
	}

	@Override
	public void handleQueryResult(String keyId,
			List<Proposition> propositions,
			Map<Proposition, List<Proposition>> forwardDerivations,
			Map<Proposition, List<Proposition>> backwardDerivations,
			Map<UniqueId, Proposition> references)
			throws QueryResultsHandlerProcessingException {

		// clear out the previous patient's data
		this.nodes.clear();

		if (++this.count % DEFAULT_COMMIT_FREQUENCY == 0) {
			if (this.transaction != null) {
				this.transaction.success();
				this.transaction.close();
				this.transaction = null;
			}
			this.transaction = this.db.beginTx();
		}

		try {
			// now create relationships for the references
			handleReferences(propositions, references);

			// now create relationships for the forward derivations
			handleDerivations(forwardDerivations, true);

			// now create relationships for the backward derivations
			handleDerivations(backwardDerivations, false);
		} catch (QueryResultsHandlerProcessingException ex) {
			throw ex;
		} catch (ProtempaException ex) {
			throw new QueryResultsHandlerProcessingException(ex);
		}
	}

	@Override
	public void finish() throws QueryResultsHandlerProcessingException {
		// add/update a statistics node to save the number of patients added
		if (this.db != null) {
			this.transaction.success();
			this.transaction.close();
			this.transaction = null;

			try (Transaction tx = this.db.beginTx()) {
				if (this.query.getQueryMode() == QueryMode.REPLACE) {
					Schema schema = this.db.schema();
					schema.indexFor(NODE_LABEL).on("__uid").create();
					schema.indexFor(NODE_LABEL).on("__type").create();
					for (IndexOnProperty indexOnProperty : this.configuration.getPropertiesToIndex()) {
						schema.indexFor(NODE_LABEL).on(indexOnProperty.getPropertyName()).create();
					}
				}

				tx.success();
			}

			if (this.query.getQueryMode() == QueryMode.REPLACE) {
				try (Transaction tx = this.db.beginTx()) {
					Schema schema = this.db.schema();
					schema.awaitIndexesOnline(4, TimeUnit.HOURS);
					tx.success();
				}
			}

			try (Transaction tx = this.db.beginTx()) {
				Node node;
				try {
					node = this.db.findNode(Neo4jStatistics.NODE_LABEL, null, null);
				} catch (MultipleFoundException ex) {
					throw new QueryResultsHandlerProcessingException("duplicate statistics node");
				}
				if (node == null) {
					node = this.db.createNode(Neo4jStatistics.NODE_LABEL);
				}
				
				ResourceIterator<Node> findNodes = this.db.findNodes(NODE_LABEL, keyType, null);
				int countKeys = IteratorUtil.count(findNodes);
				
				node.setProperty(Neo4jStatistics.TOTAL_KEYS, countKeys);

				tx.success();
			}

		}
	}

	@Override
	public void validate()
			throws QueryResultsHandlerValidationFailedException {
	}

	@Override
	public void close() throws QueryResultsHandlerCloseException {
		this.keyTypes = null;
		if (this.db != null) {
			if (this.transaction != null) {
				this.transaction.close();
			}
			this.db.shutdown();
		}
		try {
			this.home.startServer();
		} catch (IOException | InterruptedException | CommandFailedException ex) {
			throw new QueryResultsHandlerCloseException(ex);
		}
	}

	@Override
	public void cancel() {

	}

	private Node node(Proposition inProposition) throws QueryResultsHandlerProcessingException {
		String uid = inProposition.getUniqueId().getStringRepresentation();
		MapPropositionVisitor visitor = new MapPropositionVisitor(this.configuration, this.cache);
		Node node = null;
		if (this.query.getQueryMode() == QueryMode.REPLACE || !this.keyTypes.contains(inProposition.getId())) {
			node = this.db.createNode(NODE_LABEL);
		} else {
			try {
				node = this.db.findNode(NODE_LABEL, "__uid", uid);
			} catch (MultipleFoundException ex) {
				throw new QueryResultsHandlerProcessingException("duplicate uid " + uid);
			}

			if (node == null) {
				node = this.db.createNode(NODE_LABEL);
			}
		}
		assert node != null : "node was never set";
		String propId = inProposition.getId();
		PropositionDefinition pd = this.cache.get(propId);
		if (pd == null && this.missingPropIds.add(propId)) {
			LOGGER.warn("No proposition definition with id {}", propId);
		}
		node.setProperty("displayName", pd != null ? pd.getDisplayName() : propId);
		node.setProperty("__type", inProposition.getId());
		inProposition.accept(visitor);
		for (Map.Entry<String, Object> entry : visitor.getMap().entrySet()) {
			Object value = entry.getValue();
			try {
				if (value != null) {
					node.setProperty(entry.getKey(), value);
				} else {
					node.setProperty(entry.getKey(), this.configuration.getNullValue());
				}
			} catch (IllegalArgumentException ex) {
				throw new AssertionError(ex);
			}
		}
		node.setProperty("__uid", uid);
		return node;
	}

	private Node getOrCreateNode(Proposition inProposition) throws QueryResultsHandlerProcessingException {
		if (!this.nodes.containsKey(inProposition.getUniqueId())) {
			this.nodes.put(
					inProposition.getUniqueId(), this.node(inProposition));
		}
		return this.nodes.get(inProposition.getUniqueId());
	}

	private void relate(Node source, Node target,
			RelationshipType inRelation) {
		Relationship relationship
				= source.createRelationshipTo(target, inRelation);
		relationship.setProperty("name", relationship.getType().name());
	}

	private RelationshipType getOrCreateRelation(String name) {
		if (!this.relations.containsKey(name)) {
			DynamicRelationshipType relationshipType
					= DynamicRelationshipType.withName(name);
			this.relations.put(name, relationshipType);
		}
		return this.relations.get(name);
	}

	private void handleDerivations(
			Map<Proposition, List<Proposition>> derivations, boolean forward)
			throws QueryResultsHandlerProcessingException, ProtempaException {
		for (Map.Entry<Proposition, List<Proposition>> entry
				: derivations.entrySet()) {
			Proposition sourceProposition = entry.getKey();
			Node source = this.getOrCreateNode(sourceProposition);
			for (Proposition targetProposition : entry.getValue()) {
				Node target = this.getOrCreateNode(targetProposition);
				String derivationType = derivationType(
						sourceProposition, targetProposition, forward);
				RelationshipType relation
						= this.getOrCreateRelation(derivationType);
				this.relate(source, target, relation);
			}
		}
	}

	private void handleReferences(List<Proposition> propositions,
			Map<UniqueId, Proposition> references)
			throws QueryResultsHandlerProcessingException {
		for (Proposition proposition : propositions) {
			Node source = this.getOrCreateNode(proposition);

			String[] names = proposition.getReferenceNames();
			for (String name : names) {
				List<UniqueId> ids = proposition.getReferences(name);
				RelationshipType relation = this.getOrCreateRelation(name);
				if (LOGGER.isTraceEnabled()) {
					LOGGER.trace(
							"Processing {} references with type {} for {}",
							ids.size(), name, proposition.getId());
				}
				for (UniqueId id : ids) {
					Proposition targetProposition = references.get(id);
					if (targetProposition != null) {
						Node target = this.getOrCreateNode(targetProposition);
						this.relate(source, target, relation);
					} else {
						LOGGER.error("No proposition for {}", id);
						throw new QueryResultsHandlerProcessingException(
								"No proposition for id " + id);
					}
				}
			}
		}
	}

	private String derivationType(Proposition source, Proposition target, boolean forward)
			throws ProtempaException {
		Derivations.Type result;
		String key = source.getId() + "->" + target.getId();
		String inverseKey = target.getId() + "->" + source.getId();
		PropositionDefinition definition
				= this.cache.get(source.getId());

		if (this.deriveType.containsKey(key)) {
			result = this.deriveType.get(key);
		} else if (this.deriveType.containsKey(inverseKey)) {
			result = Derivations.inverse(this.deriveType.get(inverseKey));
		} else {
			PropositionDefinitionRelationVisitor visitor
					= forward ? this.forwardVisitor : this.backwardVisitor;
			visitor.setTarget(this.cache.get(target.getId()));
			definition.acceptChecked(visitor);
			result = visitor.getRelation();
			this.deriveType.put(key, result);
			this.deriveType.put(inverseKey, Derivations.inverse(result));
		}
		return result.name();
	}

	private void deleteAll() throws IOException {
		LOGGER.info("Deleting all data from {}", this.home.getDbPath());
		GraphDatabaseFactory factory = new GraphDatabaseFactory();
		//Instantiate a database as a precaution to avoid deleting a directory that isn't a Neo4j database.
		GraphDatabaseService newEmbeddedDatabase = factory.newEmbeddedDatabase(this.home.getDbPath().getAbsolutePath());
		newEmbeddedDatabase.shutdown();
		FileUtils.deleteDirectory(this.home.getDbPath());
		LOGGER.info("Done deleting all data from {}", this.home.getDbPath());
	}

}
