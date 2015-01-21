package edu.emory.cci.aiw.i2b2etl.table;

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

import edu.emory.cci.aiw.i2b2etl.metadata.Concept;
import edu.emory.cci.aiw.i2b2etl.metadata.MetadataUtil;
import edu.emory.cci.aiw.i2b2etl.metadata.SynonymCode;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import org.arp.javautil.sql.ConnectionSpec;

/**
 *
 * @author Andrew Post
 */
public class MetaTableConceptHandler extends RecordHandler<Concept> {
    private Timestamp importTimestamp;

    public MetaTableConceptHandler(ConnectionSpec connSpec, String tableName) throws SQLException {
        super(connSpec, "insert into " + tableName + "(c_hlevel,c_fullname,c_name,c_synonym_cd,c_visualattributes,c_totalnum,"
                + "c_basecode,c_metadataxml,c_facttablecolumn,c_tablename,c_columnname,c_columndatatype,c_operator,c_dimcode,c_comment,c_tooltip,"
                + "update_date,download_Date,import_date,sourcesystem_cd,valuetype_cd,m_applied_path,m_exclusion_cd,c_path,c_symbol)"
                + " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        this.importTimestamp = new Timestamp(System.currentTimeMillis());
    }

    @Override
    protected void setParameters(PreparedStatement ps, Concept concept) throws SQLException {
        ps.setLong(1, concept.getLevel());
        ps.setString(2, concept.getFullName());
        assert concept.getDisplayName() != null && concept.getDisplayName().length() > 0 : "concept " + concept.getConceptCode() + " (" + concept.getFullName() + ") " + " has an invalid display name '" + concept.getDisplayName() + "'";
        ps.setString(3, concept.getDisplayName());
        String conceptCode = concept.getConceptCode();
        ps.setString(4, SynonymCode.NOT_SYNONYM.getCode());
        ps.setString(5, concept.getCVisualAttributes());
        ps.setObject(6, null);
        ps.setString(7, conceptCode);
        if (null == concept.getMetadataXml() || concept.getMetadataXml().isEmpty()) {
            ps.setObject(8, null);
        } else {
            ps.setObject(8, concept.getMetadataXml());
        }
        ps.setString(9, concept.getFactTableColumn());
        ps.setString(10, concept.getTableName());
        ps.setString(11, concept.getColumnName());
        ps.setString(12, concept.getDataType().getCode());
        ps.setString(13, concept.getOperator().getSQLOperator());
        ps.setString(14, concept.getDimCode());
        ps.setObject(15, concept.getComment());
        ps.setString(16, concept.getToolTip());
        ps.setTimestamp(17, null);
        ps.setTimestamp(18, TableUtil.setTimestampAttribute(concept.getDownloaded()));
        ps.setTimestamp(19, this.importTimestamp);
        ps.setString(20, MetadataUtil.toSourceSystemCode(concept.getSourceSystemCode()));
        ps.setString(21, concept.getValueTypeCode().getCode());
        ps.setString(22, concept.getAppliedPath());
        ps.setString(23, null);
        ps.setString(24, concept.getCPath());
        ps.setString(25, concept.getSymbol());
    }

}
