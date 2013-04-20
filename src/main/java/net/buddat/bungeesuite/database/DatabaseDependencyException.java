package net.buddat.bungeesuite.database;

/**
 * Indicates that a database dependency has not been met.
 */
public class DatabaseDependencyException extends Exception {
	/**
	 * Auto-generated serialVersionUID.
	 */
	private static final long serialVersionUID = -1737874811904077160L;

	public DatabaseDependencyException(String message, Throwable cause) {
		super(message, cause);
	}
	
	public DatabaseDependencyException(String message) {
		super(message);
	}
}
