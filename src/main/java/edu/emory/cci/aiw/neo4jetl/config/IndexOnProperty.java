package edu.emory.cci.aiw.neo4jetl.config;

/**
 * A node property to index.
 * 
 * @author Andrew Post
 */
public interface IndexOnProperty {
	/**
	 * Returns the name of the property to index.
	 * 
	 * @return the property name. Guaranteed not null.
	 */
	String getPropertyName();
}
