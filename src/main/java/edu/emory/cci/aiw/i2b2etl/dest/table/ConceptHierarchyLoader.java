package edu.emory.cci.aiw.i2b2etl.dest.table;

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
import edu.emory.cci.aiw.i2b2etl.dest.metadata.Concept;
import java.sql.SQLException;
import java.util.Enumeration;

import javax.swing.tree.TreeNode;

import org.protempa.ProtempaUtil;

/**
 *
 * @author Andrew Post
 */
public abstract class ConceptHierarchyLoader {
    
    protected ConceptHierarchyLoader() {
        
    }

    public final void execute(Concept... roots) throws SQLException {
        ProtempaUtil.checkArrayForNullElement(roots, "roots");
        for (Concept root : roots) {
            Enumeration<TreeNode> emu = root.breadthFirstEnumeration();
            while (emu.hasMoreElements()) {
                loadConcept((Concept) emu.nextElement());
            }
        }
    }
    
    protected abstract void loadConcept(Concept concept) throws SQLException;
}
