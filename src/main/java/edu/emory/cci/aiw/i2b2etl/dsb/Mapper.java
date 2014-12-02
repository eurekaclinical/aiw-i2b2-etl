package edu.emory.cci.aiw.i2b2etl.dsb;

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

import java.io.IOException;
import org.protempa.backend.dsb.relationaldb.ColumnSpec;
import org.protempa.backend.dsb.relationaldb.KnowledgeSourceIdToSqlCode;

/**
 *
 * @author Andrew Post
 */
public interface Mapper {

    /**
     * Reads codes associated with a property name or proposition ID from a
     * resource. The resource will be prefixed with the prefix specified at
     * construction time. The result of this method can be passed to the
     * {@link ColumnSpec} constructor.
     *
     * @param resource
     *            a {@link String} that is the name of resource that holds the
     *            mappings. It will be prefixed by the prefix specified at
     *            construction time.
     * @return an array of {@link KnowledgeSourceIdToSqlCode}s read from the
     *         resource
     * @throws IOException
     *             if something goes wrong while accessing the resource
     */
    KnowledgeSourceIdToSqlCode[] propertyNameOrPropIdToSqlCodeArray(String resource) throws IOException;

    /**
     * Reads codes in a resource. The resource is prefixed by the resource
     * prefix specified at construction. Each mapping must be on a separate
     * tab-delimited line. A column number indicates which column holds the
     * knowledge source code for the mapping.
     *
     * @param resource
     *            the name of the resource, as a {@link String}. Will be
     *            prefixed by the prefix indicated at construction.
     * @param colNum
     *            an integer indicating which column of the mapping holds the
     *            knowledge source version of a code
     * @return a {@link String} array containing all of the mapped knowledge source
     *         codes in the resource
     * @throws IOException
     *             if something goes wrong while accessing the resource
     */
    String[] readCodes(String resource, int colNum) throws IOException;
    
}
