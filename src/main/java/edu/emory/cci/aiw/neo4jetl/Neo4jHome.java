package edu.emory.cci.aiw.neo4jetl;

/*
 * #%L
 * AIW Neo4j ETL
 * %%
 * Copyright (C) 2015 Emory University
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/gpl-3.0.html>.
 * #L%
 */

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.Executor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Andrew Post
 */
class Neo4jHome {
	private static final Logger LOGGER
			= LoggerFactory.getLogger(Neo4jHome.class);
	private static final String SERVER_PROPERTIES = new File("conf", "neo4j-server.properties").getPath();
	private static final String SERVER_CONTROL_COMMAND = new File("bin", "neo4j").getPath();
	private final String home;
	private final File dbPath;
	
	Neo4jHome(String home) throws IOException {
		assert home != null : "home cannot be null";
		this.home = home;
		Properties serverProperties = new Properties();
		try (FileReader bf = new FileReader(new File(home, SERVER_PROPERTIES))) {
			serverProperties.load(bf);
		}
		this.dbPath = new File(home, serverProperties.getProperty("org.neo4j.server.database.location"));
	}
	
	File getDbPath() {
		return this.dbPath;
	}
	
	void startServer() throws IOException, InterruptedException, CommandFailedException {
		controlServer("start");
	}
	
	void stopServer() throws IOException, InterruptedException, CommandFailedException {
		controlServer("stop");
	}
	
	private void controlServer(String command) throws IOException, InterruptedException, CommandFailedException {
		LOGGER.debug("Executing neo4j command {}...", command);
		CommandLine serverControlCommand = new CommandLine(new File(this.home, SERVER_CONTROL_COMMAND));
		serverControlCommand.addArgument("${command}");
		Map<String,String> map = new HashMap<>();
		map.put("command", command);
		serverControlCommand.setSubstitutionMap(map);
		
		DefaultExecuteResultHandler resultHandler = new DefaultExecuteResultHandler();

		ExecuteWatchdog watchdog = new ExecuteWatchdog(60*1000);
		Executor executor = new DefaultExecutor();
		executor.setExitValue(1);
		executor.setWatchdog(watchdog);
		executor.execute(serverControlCommand, resultHandler);
		LOGGER.debug("Neo4j command {} is completed, checking exit value...", command);
		resultHandler.waitFor();
		int exitValue = resultHandler.getExitValue();
		if (exitValue != 0) {
			ExecuteException exception = resultHandler.getException();
			throw new CommandFailedException(exitValue, "Neo4j command '" + command + "' failed", exception);
		}
		LOGGER.debug("Neo4j command {} was successful", command);
	}
	
}
