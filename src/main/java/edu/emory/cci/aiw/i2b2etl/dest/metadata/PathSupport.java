package edu.emory.cci.aiw.i2b2etl.dest.metadata;

import java.util.logging.Level;
import java.util.logging.Logger;
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
import javax.swing.tree.TreeNode;

/**
 *
 * @author Andrew Post
 */
class PathSupport {

    private Concept concept;
    private static final Logger LOGGER = Logger.getLogger(PathSupport.class.getName());

    private static interface PathConceptRep {

        String toString(Concept concept);
    }

    private static final PathConceptRep SYMBOL_REP = new PathConceptRep() {

        @Override
        public String toString(Concept concept) {
            String sym = concept.getSymbol();
            int start = 0;
            boolean modified = false;
            if (sym.charAt(start) == '\\') {
                start++;
                modified = true;
            }
            int end = sym.length();
            if (sym.charAt(end - 1) == '\\') {
                end--;
                modified = true;
            }
            if (modified) {
                return sym.substring(start, end);
            } else {
                return sym;
            }
        }

    };

    private static final PathConceptRep DISPLAY_NAME_REP = new PathConceptRep() {

        @Override
        public String toString(Concept concept) {
            return concept.getDisplayName();
        }

    };

    public Concept getConcept() {
        return this.concept;
    }

    public void setConcept(Concept concept) {
        this.concept = concept;
    }

    String getCPath() {
        StringBuilder buf = new StringBuilder();
        pathToString(buf, "\\", SYMBOL_REP);
        buf.append("\\");
        return buf.toString();
    }

    String getFullName() {
        StringBuilder buf = new StringBuilder();
        appendFullname(buf, "\\", SYMBOL_REP);
        buf.append("\\");
        return buf.toString();
    }

    String getToolTip() {
        StringBuilder buf = new StringBuilder();
        appendFullname(buf, " \\ ", DISPLAY_NAME_REP);
        return buf.toString();
    }

    int getLevel() {
        return this.concept.getLevel();
    }

    private void appendFullname(StringBuilder buf, String sep, PathConceptRep rep) {
        pathToString(buf, sep, rep);
        buf.append(sep);
        buf.append(rep.toString(this.concept));
    }

    private void pathToString(StringBuilder buf, String sep, PathConceptRep rep) {
    	LOGGER.log(Level.FINE, "pathToString: current buf: {0}::{1}", 
    			new Object[]{buf.toString(),this.concept.getConceptCode()});
        TreeNode[] path = this.concept.getPath();
        for (int i = 0; i < path.length - 1; i++) {
            TreeNode tn = path[i];
            buf.append(sep);
            buf.append(rep.toString((Concept) tn));
        }
        LOGGER.log(Level.FINE, "After path: {0}", 
    			new Object[]{buf.toString()});
    }

}
