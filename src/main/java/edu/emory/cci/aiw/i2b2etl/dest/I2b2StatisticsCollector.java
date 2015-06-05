package edu.emory.cci.aiw.i2b2etl.dest;

/*
 * #%L
 * AIW i2b2 ETL
 * %%
 * Copyright (C) 2012 - 2014 Emory University
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
import edu.emory.cci.aiw.i2b2etl.dest.config.ConfigurationReadException;
import edu.emory.cci.aiw.i2b2etl.dest.config.Database;
import edu.emory.cci.aiw.i2b2etl.dest.config.DatabaseSpec;
import edu.emory.cci.aiw.i2b2etl.ksb.QueryConstructor;
import edu.emory.cci.aiw.i2b2etl.ksb.QueryExecutor;
import edu.emory.cci.aiw.i2b2etl.ksb.ResultSetReader;
import edu.emory.cci.aiw.i2b2etl.ksb.TableAccessReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import org.arp.javautil.sql.ConnectionSpec;
import org.protempa.KnowledgeSourceReadException;
import org.protempa.dest.DefaultStatisticsBuilder;
import org.protempa.dest.Statistics;
import org.protempa.dest.StatisticsException;

/**
 *
 * @author Andrew Post
 */
final class I2b2StatisticsCollector {

    private final ConnectionSpec dataConnectionSpec;
    private final ConnectionSpec metaConnectionSpec;
    private final String metaTableName;

    I2b2StatisticsCollector(Configuration config) throws StatisticsException {
        try {
            config.init();
            Database databaseSection = config.getDatabase();
            DatabaseSpec dataSchemaSpec = databaseSection.getDataSpec();
            this.dataConnectionSpec = dataSchemaSpec.toConnectionSpec();
            DatabaseSpec metaSchemaSpec = databaseSection.getMetadataSpec();
            this.metaConnectionSpec = metaSchemaSpec.toConnectionSpec();
            this.metaTableName = config.getSettings().getMetaTableName();
        } catch (ConfigurationReadException ex) {
            throw new StatisticsException("Could not initialize statistics gathering", ex);
        }
    }

    Statistics collectStatistics() throws StatisticsException {
        int count;
        try (Connection conn = this.dataConnectionSpec.getOrCreate();
                Statement stmt = conn.createStatement();
                ResultSet resultSet = stmt.executeQuery("SELECT COUNT(*) FROM PATIENT_DIMENSION");) {
            if (!resultSet.next()) {
                throw new AssertionError("No count retrieved for i2b2 destination");
            }
            count = resultSet.getInt(1);
        } catch (SQLException ex) {
            throw new StatisticsException("Could not retrieve statistics from i2b2 destination", ex);
        }
        DefaultStatisticsBuilder builder = new DefaultStatisticsBuilder();
        builder.setNumberOfKeys(count);
        Map<String, Integer> counts = new HashMap<>();
        Map<String, String> childrenToParents = new HashMap<>();
        populateCountsAndChildrenToParents(counts, childrenToParents);

        String root = "All Patients";
        for (Map.Entry<String, String> me : childrenToParents.entrySet()) {
            if (me.getValue() == null) {
                me.setValue(root);
            }
        }
        
        childrenToParents.put(root, null);
        builder.setChildrenToParents(childrenToParents);

        counts.put(root, builder.getNumberOfKeys());
        builder.setCounts(counts);

        return builder.toDefaultStatistics();
    }
    
    private void populateCountsAndChildrenToParents(Map<String, Integer> counts, Map<String, String> childrenToParents) throws StatisticsException {
        try (Connection conn = this.metaConnectionSpec.getOrCreate()) {
            QueryExecutor queryExecutor = new QueryExecutor(conn, new QueryConstructor() {

                @Override
                public void appendStatement(StringBuilder sql, String table) {
                    sql.append("SELECT A1.EK_UNIQUE_ID EK_UNIQUE_ID, A1.C_NAME NAME, A2.EK_UNIQUE_ID PARENT_EK_UNIQUE_ID, A2.C_NAME PARENT_NAME, A1.C_TOTALNUM FROM ").append(table).append(" A1 LEFT OUTER JOIN ").append(table).append(" A2 ON (A1.C_PATH=A2.C_FULLNAME)");
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
        } catch (SQLException | KnowledgeSourceReadException ex) {
            throw new StatisticsException("Could not retrieve statistics from i2b2 destination", ex);
        }

        if (this.metaTableName != null) {
            try (Connection conn = this.metaConnectionSpec.getOrCreate();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT A1.C_BASECODE C_BASECODE, A1.C_NAME NAME, A2.C_BASECODE PARENT_C_BASECODE, A2.C_NAME PARENT_NAME, A1.C_TOTALNUM FROM " + this.metaTableName + " A1 LEFT OUTER JOIN " + this.metaTableName + " A2 ON (A1.C_PATH=A2.C_FULLNAME)")) {
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
    }

    private static class IntStatisticsBuilder {
        int numberOfKeys;
        Map<String, Integer> counts;
        Map<String, String> childrenToParents;
    }

    private IntStatisticsBuilder doProcess(ResultSet rs) throws KnowledgeSourceReadException {
        IntStatisticsBuilder r = new IntStatisticsBuilder();
        Map<String, Integer> counts = new HashMap<>();
        Map<String, String> childrenToParents = new HashMap<>();
        try {
            while (rs.next()) {
                String propId = rs.getString(1);
                String displayName = rs.getString(2);
                String parentPropId = rs.getString(3);
                String parentDisplayName = rs.getString(4);
                String key = displayName != null ? displayName + " (" + propId + ")" : propId;
                String parentKey = parentDisplayName != null ? parentDisplayName + " (" + parentPropId + ")" : parentPropId;
                childrenToParents.put(key, parentKey);
                Integer totalNum = rs.getObject(5) != null ? rs.getInt(5) : null;
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
