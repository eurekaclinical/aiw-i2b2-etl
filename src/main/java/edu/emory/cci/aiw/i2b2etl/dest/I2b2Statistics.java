package edu.emory.cci.aiw.i2b2etl.dest;

/*
 * #%L
 * AIW i2b2 ETL
 * %%
 * Copyright (C) 2012 - 2015 Emory University
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
import edu.emory.cci.aiw.i2b2etl.dest.config.Configuration;
import edu.emory.cci.aiw.i2b2etl.dest.config.Database;
import edu.emory.cci.aiw.i2b2etl.dest.config.DatabaseSpec;
import edu.emory.cci.aiw.i2b2etl.dest.metadata.MetadataUtil;
import edu.emory.cci.aiw.i2b2etl.ksb.QueryConstructor;
import edu.emory.cci.aiw.i2b2etl.ksb.QueryExecutor;
import edu.emory.cci.aiw.i2b2etl.ksb.ResultSetReader;
import edu.emory.cci.aiw.i2b2etl.ksb.TableAccessReader;
import edu.emory.cci.aiw.i2b2etl.ksb.UniqueIdTempTableHandler;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.arp.javautil.arrays.Arrays;
import org.arp.javautil.sql.ConnectionSpec;
import org.arp.javautil.sql.DatabaseProduct;
import org.protempa.KnowledgeSourceReadException;
import org.protempa.ProtempaUtil;
import org.protempa.dest.Statistics;
import org.protempa.dest.StatisticsException;

/**
 *
 * @author Andrew Post
 */
public class I2b2Statistics implements Statistics {

    private final ConnectionSpec dataConnectionSpec;
    private final ConnectionSpec metaConnectionSpec;
    private final String metaTableName;
    private final Object numberOfKeysMonitor = new Object();
    private final Object populatorMonitor = new Object();
    private Integer numberOfKeys;
    private final Set<String> currentPropIds;
    private Map<String, Integer> counts;
    private Map<String, String> childrenToParents;

    public I2b2Statistics(Configuration config) throws StatisticsException {
        Database databaseSection = config.getDatabase();
        DatabaseSpec dataSchemaSpec = databaseSection.getDataSpec();
        this.dataConnectionSpec = dataSchemaSpec.toConnectionSpec();
        DatabaseSpec metaSchemaSpec = databaseSection.getMetadataSpec();
        this.metaConnectionSpec = metaSchemaSpec.toConnectionSpec();
        this.metaTableName = config.getSettings().getMetaTableName();
        this.currentPropIds = new HashSet<>();
    }

    @Override
    public int getNumberOfKeys() throws StatisticsException {
        synchronized (this.numberOfKeysMonitor) {
            if (this.numberOfKeys == null) {
                try (Connection conn = this.dataConnectionSpec.getOrCreate();
                        Statement stmt = conn.createStatement();
                        ResultSet resultSet = stmt.executeQuery("SELECT COUNT(*) FROM PATIENT_DIMENSION");) {
                    if (!resultSet.next()) {
                        throw new AssertionError("No count retrieved for i2b2 destination");
                    }
                    this.numberOfKeys = resultSet.getInt(1);
                } catch (SQLException ex) {
                    throw new StatisticsException("Could not retrieve statistics from i2b2 destination", ex);
                }
            }
            return this.numberOfKeys;
        }
    }

    @Override
    public Map<String, String> getChildrenToParents() throws StatisticsException {
        return getChildrenToParents(null);
    }

    @Override
    public Map<String, String> getChildrenToParents(String[] propIds) throws StatisticsException {
        synchronized (this.populatorMonitor) {
            updateState(propIds);
            populateCountsAndChildrenToParentsIfNeeded();
            return this.childrenToParents;
        }
    }

    @Override
    public Map<String, Integer> getCounts() throws StatisticsException {
        return getCounts(null);
    }

    @Override
    public Map<String, Integer> getCounts(String[] propIds) throws StatisticsException {
        synchronized (this.populatorMonitor) {
            updateState(propIds);
            populateCountsAndChildrenToParentsIfNeeded();
            return this.counts;
        }
    }

    private void updateState(String[] propIds) {
        if (propIds != null && propIds.length > 0) {
            ProtempaUtil.checkArrayForNullElement(propIds, "propIds");
            for (String propId : propIds) {
                if (!this.currentPropIds.contains(propId)) {
                    this.counts = null;
                    this.childrenToParents = null;
                    this.currentPropIds.clear();
                    Arrays.addAll(this.currentPropIds, propIds);
                    break;
                }
            }

        } else {
            if (!this.currentPropIds.isEmpty()) {
                this.counts = null;
                this.childrenToParents = null;
                this.currentPropIds.clear();
            }
        }
    }

    public void clear() {
        synchronized (this) {
            this.numberOfKeys = null;
            this.counts = null;
            this.childrenToParents = null;
            this.currentPropIds.clear();
        }
    }

    private void populateCountsAndChildrenToParentsIfNeeded() throws StatisticsException {
        if (this.counts == null) {
            try (Connection conn = this.metaConnectionSpec.getOrCreate()) {
                try {
                    if (!this.currentPropIds.isEmpty()) {
                        populateUniqueIdTempTable(conn);
                    }

                    collectParentsAndCountsFromEurekafiedTables(conn);

                    if (this.metaTableName != null) {
                        collectParentsAndCountsFromEurekaOwnedTable(conn);
                    }
                    conn.commit();
                } catch (SQLException | KnowledgeSourceReadException ex) {
                    try {
                        conn.rollback();
                    } catch (SQLException ignore) {
                    } finally {
                        throw ex;
                    }
                }
            } catch (SQLException | KnowledgeSourceReadException ex) {
                throw new StatisticsException("Could not retrieve statistics from i2b2 destination", ex);
            }
            counts = Collections.unmodifiableMap(counts);
            childrenToParents = Collections.unmodifiableMap(childrenToParents);
        }
    }

    private void populateUniqueIdTempTable(final Connection conn) throws SQLException {
        try (UniqueIdTempTableHandler childTempTableHandler = 
                new UniqueIdTempTableHandler(
                        DatabaseProduct.fromMetaData(
                                conn.getMetaData()), conn, false)) {
            for (String propId : this.currentPropIds) {
                childTempTableHandler.insert(propId);
            }
        }
    }

    private void collectParentsAndCountsFromEurekafiedTables(final Connection conn) throws KnowledgeSourceReadException {
        QueryExecutor queryExecutor = new QueryExecutor(conn, new QueryConstructor() {

            @Override
            public void appendStatement(StringBuilder sql, String table) {
                if (I2b2Statistics.this.currentPropIds.isEmpty()) {
                    sql.append("SELECT A1.EK_UNIQUE_ID EK_UNIQUE_ID, A2.EK_UNIQUE_ID PARENT_EK_UNIQUE_ID, A1.C_TOTALNUM FROM ").append(table).append(" A1 LEFT OUTER JOIN ").append(table).append(" A2 ON (A1.C_PATH=A2.C_FULLNAME) WHERE A1.C_HLEVEL=(SELECT MIN(C_HLEVEL) FROM TABLE_ACCESS WHERE C_TABLE_NAME='").append(table).append("') AND A1.M_APPLIED_PATH='@' AND A1.EK_UNIQUE_ID NOT LIKE '" + MetadataUtil.DEFAULT_CONCEPT_ID_PREFIX_INTERNAL + "%'");
                } else {
                    sql.append("SELECT A1.EK_UNIQUE_ID EK_UNIQUE_ID, A2.EK_UNIQUE_ID PARENT_EK_UNIQUE_ID, A1.C_TOTALNUM FROM ").append(table).append(" A1 LEFT OUTER JOIN ").append(table).append(" A2 ON (A1.C_PATH=A2.C_FULLNAME) JOIN EK_TEMP_UNIQUE_IDS A3 ON (A2.EK_UNIQUE_ID=A3.UNIQUE_ID) WHERE A2.M_APPLIED_PATH='@' AND A2.EK_UNIQUE_ID NOT LIKE '" + MetadataUtil.DEFAULT_CONCEPT_ID_PREFIX_INTERNAL + "%'");
                }
            }
        },
                new TableAccessReader(this.metaTableName));
        IntStatisticsBuilder b = queryExecutor.execute(new ResultSetReader<IntStatisticsBuilder>() {

            @Override
            public IntStatisticsBuilder read(ResultSet rs) throws KnowledgeSourceReadException {
                return doProcess(rs);
            }

        });
        counts = b.counts;
        childrenToParents = b.childrenToParents;
    }

    private void collectParentsAndCountsFromEurekaOwnedTable(final Connection conn) throws StatisticsException {
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        I2b2Statistics.this.currentPropIds.isEmpty()
                                ? "SELECT A1.C_BASECODE C_BASECODE, A2.C_BASECODE PARENT_C_BASECODE, A1.C_TOTALNUM FROM " + this.metaTableName + " A1 LEFT OUTER JOIN " + this.metaTableName + " A2 ON (A1.C_PATH=A2.C_FULLNAME) WHERE (A1.C_HLEVEL=(SELECT MIN(C_HLEVEL) FROM TABLE_ACCESS WHERE C_TABLE_NAME='" + this.metaTableName + "') OR A1.C_PATH=(SELECT C_FULLNAME FROM " + this.metaTableName + " WHERE C_BASECODE='AIW|Phenotypes')) AND A1.M_APPLIED_PATH='@' AND A1.C_BASECODE NOT LIKE '" + MetadataUtil.DEFAULT_CONCEPT_ID_PREFIX_INTERNAL + "%'"
                                : "SELECT A1.C_BASECODE C_BASECODE, A2.C_BASECODE PARENT_C_BASECODE, A1.C_TOTALNUM FROM " + this.metaTableName + " A1 LEFT OUTER JOIN " + this.metaTableName + " A2 ON (A1.C_PATH=A2.C_FULLNAME) JOIN EK_TEMP_UNIQUE_IDS A3 ON (A2.C_BASECODE=A3.UNIQUE_ID) WHERE A2.M_APPLIED_PATH='@' AND A2.C_BASECODE NOT LIKE '" + MetadataUtil.DEFAULT_CONCEPT_ID_PREFIX_INTERNAL + "%'")) {
            IntStatisticsBuilder c = doProcess(rs);
            counts.putAll(c.counts);
            for (Map.Entry<String, String> me : c.childrenToParents.entrySet()) {
                if (!childrenToParents.containsKey(me.getKey())) {
                    childrenToParents.put(me.getKey(), me.getValue());
                }
            }
        } catch (SQLException | KnowledgeSourceReadException ex) {
            throw new StatisticsException("Could not retrieve statistics from i2b2 destination", ex);
        }
    }

    private static class IntStatisticsBuilder {

        int numberOfKeys;
        Map<String, Integer> counts;
        Map<String, String> childrenToParents;
    }

    private static IntStatisticsBuilder doProcess(ResultSet rs) throws KnowledgeSourceReadException {
        IntStatisticsBuilder r = new IntStatisticsBuilder();
        Map<String, Integer> counts = new HashMap<>();
        Map<String, String> childrenToParents = new HashMap<>();
        try {
            while (rs.next()) {
                String key = rs.getString(1);
                String parentKey = rs.getString(2);
                childrenToParents.put(key, parentKey);
                Integer totalNum = rs.getObject(3) != null ? rs.getInt(3) : null;
                if (totalNum != null) {
                    counts.put(key, totalNum);
                }
            }
        } catch (SQLException ex) {
            throw new KnowledgeSourceReadException(ex);
        }
        r.counts = counts;
        r.childrenToParents = childrenToParents;
        return r;
    }

}
