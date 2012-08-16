package edu.emory.cci.aiw.i2b2etl;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import org.protempa.AbstractionDefinition;
import org.protempa.KnowledgeSource;
import org.protempa.KnowledgeSourceReadException;
import org.protempa.PropositionDefinition;

/**
 *
 * @author Andrew Post
 */
class I2b2DerivedPropositionIdExtractor {
    private final KnowledgeSource knowledgeSource;

    public I2b2DerivedPropositionIdExtractor(
            KnowledgeSource knowledgeSource) {
        this.knowledgeSource = knowledgeSource;
    }
    
    String[] extractDerived(PropositionDefinition[] propDefs) 
            throws KnowledgeSourceReadException {
        Set<String> potentialDerivedPropIds = new HashSet<String>();
        
        for (PropositionDefinition propDef : propDefs) {
            Queue<PropositionDefinition> queue = 
                new LinkedList<PropositionDefinition>();
            queue.add(propDef);
            PropositionDefinition pd;
            while ((pd = queue.poll()) != null) {
                List<PropositionDefinition> parents = 
                        this.knowledgeSource.readParents(pd);
                for (PropositionDefinition parent : parents) {
                    if (parent instanceof AbstractionDefinition) {
                        potentialDerivedPropIds.add(parent.getId());
                    }
                    queue.add(parent);
                }
            }
            
        }
        
        return potentialDerivedPropIds.toArray(
                new String[potentialDerivedPropIds.size()]);
    }
}
