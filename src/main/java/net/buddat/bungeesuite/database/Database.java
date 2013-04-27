package net.buddat.bungeesuite.database;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.dbutils.QueryRunner;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;

import net.buddat.bungeesuite.database.DatabaseDependencyException;

/**
 * Manages database initialisation, queries and connection pooling. This class is thread-safe.
 */
public class Database {
	private static final String DRIVER_MYSQL = "com.mysql.jdbc.Driver";
	
	private final BoneCPConfig config;
	private final BoneCP connectionPool;
	private final QueryRunner queryRunner;
	
	public Database(String host, String database, String port, String username,
			String password) throws DatabaseDependencyException, SQLException {
		// fail fast
		try {
			Class.forName(DRIVER_MYSQL);
		} catch (ClassNotFoundException e) {
			throw new DatabaseDependencyException("Could not find class for Mysql database drivers", e);
		}
		config = new BoneCPConfig();
		config.setJdbcUrl("jdbc:mysql://" + host +":" + port + "/" + database);
		config.setUsername(username);
		config.setPassword(password);
		connectionPool = new BoneCP(config);
		queryRunner = new QueryRunner();
		// attempt connection
		Connection connection = getConnection();
		connection.close();
	}
	
	/**
	 * Returns a connection for the current database. It is up to the caller to
	 * close any resources once the connection has been used.
	 * 
	 * @return Connection for the current database.
	 * @throws SQLException
	 */
	public Connection getConnection() throws SQLException {
		return connectionPool.getConnection();
	}
	
	/**
	 * Executes the given query as an update/insert.
	 * 
	 * @param query
	 *            Query to execute
	 * @return Number of rows updated/inserted.
	 * @throws SQLException
	 *             if any errors occur while executing the query.
	 */
	public int update(String sql) throws SQLException {
		try (Connection connection = getConnection()) {
			return update(connection, sql);
		}
	}
	
	/**
	 * Executes an SQL INSERT, UPDATE or DELETE query using the specified
	 * connection, without any replacement parameters.
	 * 
	 * @param connection
	 *            The connection to use to run the query.
	 * @param sql
	 *            The SQL statement to execute.
	 * @return The number of rows updated.
	 * @throws SQLException
	 *             if a database error occurs
	 */
	public int update(Connection connection, String sql) throws SQLException {
		return queryRunner.update(connection, sql);
	}
	
	/**
	 * Executes an SQL INSERT, UPDATE or DELETE query using the specified
	 * connection, with the specified replacement parameters.
	 * 
	 * @param connection
	 *            The connection to use to run the query.
	 * @param sql
	 *            The SQL statement to execute.
	 * @param params
	 *            The replacement parameters.
	 * @return The number of rows updated.
	 * @throws SQLException
	 *             if a database error occurs
	 */
	public int update(Connection connection, String sql, Object... params) throws SQLException {
		return queryRunner.update(connection, sql, params);
	}
	
	/**
	 * Excecutes an SQL INSERT, UPDATE or DELETE query using a single
	 * replacement parameter.
	 * 
	 * @param sql
	 *            The SQL statement to execute.
	 * @param param
	 *            The replacement parameter.
	 * @return The number of rows updated
	 * @throws SQLException
	 *             if a database error occurs
	 */
	public int update(String sql, Object param) throws SQLException {
		try (Connection connection = getConnection()) {
			return queryRunner.update(connection, sql, param);
		}
	}

	/**
	 * Executes the given SQL INSERT, UPDATE or DELETE statement.
	 * 
	 * @param sql
	 *            SQL to execute.
	 * @param params
	 *            Parameters to bind to placeholders (ie. '?'s).
	 * @return Number of rows updated.
	 * @throws SQLException
	 *             if a database error occurs
	 */
	public int update(String sql, Object... params) throws SQLException {
		try (Connection connection = getConnection()) {
			return queryRunner.update(connection, sql, params);
		}
	}
	/**
	 * Executes the query to determine existence of a result.
	 * 
	 * @param query
	 *            Query to execute. Must be a query that returns a result set.
	 * @return True if any results are returned, otherwise false.
	 * @throws SQLException
	 *             if any errors occur while executing the query.
	 */
	public boolean existenceQuery(String query) throws SQLException {
		Object result = singleResultQuery(query);
		return result != null;
	}
	
	/**
	 * Executes the given query and returns the first result as a string. If no
	 * results are returned, returns null. If the result is null, returns an
	 * empty string.
	 * 
	 * @param query
	 *            Query to execute. It is up to the caller to ensure that the
	 *            query returns at most one result, or returns results in
	 *            expected order.
	 * @return The first result in the result set. If no results are returned
	 *         returns null. If a null result is returned, returns an empty
	 *         string.
	 * @throws SQLException
	 *             if any errors occur while executing the query.
	 */
	public String singleResultStringQuery(String query) throws SQLException {
		Connection connection = getConnection();
		ResultSet results = query(connection, query);
		String result = null;
		if (results.next()) {
			result = results.getString(1);
			if (result == null) {
				result = "";
			}
		}
		results.close();
		connection.close();
		return result;
	}
	
	public Object singleResultQuery(String query) throws SQLException {
		Connection connection = getConnection();
		ResultSet results = query(connection, query);
		Object result = null;
		if (results.next()) {
			result = results.getObject(1);
		}
		results.close();
		connection.close();
		return result;
	}

	public boolean doesTableExist(Connection connection, String table) throws SQLException {
		DatabaseMetaData dbm = connection.getMetaData();
		ResultSet tables = dbm.getTables(null, null, table, null);
		boolean result = tables.next();
		tables.close();
		return result;
	}

	/**
	 * Executes the query with the specified connection. It is up to the caller
	 * to ensure that the connection is properly initialised beforehand and
	 * closed afterwards.
	 * 
	 * @param sql
	 *            Query to execute.
	 * @param connection
	 *            Connection with which to execute the query.
	 * @return Result of the query execution.
	 * @throws SQLException
	 *             if any errors occur while executing the query.
	 */
	public ResultSet query(Connection connection, String sql) throws SQLException {
		Statement statement = connection.createStatement();
		ResultSet result = statement.executeQuery(sql);
		return result;
	}
}
