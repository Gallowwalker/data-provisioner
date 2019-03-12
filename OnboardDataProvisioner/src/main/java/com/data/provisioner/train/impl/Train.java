package com.data.provisioner.train.impl;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQBytesMessage;

import com.data.provisioner.util.PropertyUtil;
import com.data.provisioner.vehicle.api.Vehicle;

public class Train implements Vehicle, ExceptionListener {
	
	/**
	 * The logger.
	 */
	private static final Logger LOGGER = Logger.getLogger(Train.class.getName());
	
	/**
	 * Vehicle configuration location.
	 */
	private static final String CONFIGURATION = "conf/provisioning.properties";
	
	/**
	 * Vehicle workdir.
	 */
	private static final String WORK_DIRECTORY = "workdir";
	
	/**
	 * Message for missing value in the configuration.
	 */
	private static final String MISSING_CONFIGURATION_VALUE_MESSAGE = "is missing from the configuration.";
	
	/**
	 * Message shown when the message received from MQ is not of type ActiveMQBytesMessage.
	 */
	private static final String ACTIVEMQ_WARNING_MESSAGE = "Skipping message for topic {0}, because it's not instance of ActiveMQBytesMessage.";
	
	/**
	 * Message for successfully established MQ listener.
	 */
	private static final String ESTABLISHED_LISTENER_MESSAGE = "MQ listener successfully established for topic: {0}";
	
	/**
	 * The train unique id.
	 */
	private String trainId = "";
	
	/**
	 * Connection to activeMQ.
	 */
	private String mqConnectionAddress = "";
	
	/**
	 * Reconnect timeout (in seconds) used when connection is lost. Default value is 10 seconds.
	 */
	private long reconnectTimeoutOnConnectionFailure = 10L;
	
	/**
	 * MQ topic name for content.
	 */
	private String mqTopicContent = "";
	
	/**
	 * MQ topic name for messages.
	 */
	private String mqTopicMessages = "";
	
	/**
	 * MQ topic name for realtime.
	 */
	private String mqTopicRealtime = "";
	
	/**
	 * MQ topic name for gtfs.
	 */
	private String mqTopicGTFS = "";
	
	/**
	 * MQ connection instance.
	 */
	private Connection connection = null;
	
	/**
	 * MQ session instance.
	 */
	private Session session = null;
	
	/**
	 * The flag used for checking if connection is available.
	 */
	private boolean connectionAvailable = false;
	
	
	
	public Train() {
		
	}
	
	@Override
	public void start() {
		try {
			this.loadConfiguration();
			this.initializeMQProvisioning();
			Train.LOGGER.log(Level.INFO, "Train {0}", trainId + " started.");
		} catch (IOException exception) {
			Train.LOGGER.log(Level.SEVERE, "Vehicle configuration can't be loaded. Reason: " + exception.toString());
		}
	}
	
	@Override
	public void stop() {
		this.destroyMQConnection();
		Train.LOGGER.log(Level.INFO, "Train {0}", trainId + " stopped.");
	}
	
	@Override
	public void restart(final String reason) {
		Train.LOGGER.log(Level.INFO, "Train {0}", trainId + " is restarting. " + reason);
		this.stop();
		this.start();
	}
	
	private void loadConfiguration() throws IOException {
		Train.LOGGER.log(Level.INFO, "Loading train configuration...");
		final Properties properties = PropertyUtil.loadProperties(Train.CONFIGURATION);
		
		this.trainId = Objects.requireNonNull(properties.getProperty("trainId"), "Train id " + Train.MISSING_CONFIGURATION_VALUE_MESSAGE);
		this.mqConnectionAddress = Objects.requireNonNull(properties.getProperty("mqConnectionAddress"), "MQ Connection address " + Train.MISSING_CONFIGURATION_VALUE_MESSAGE);
		this.mqTopicContent = Objects.requireNonNull(properties.getProperty("mqTopicContent"), "MQ topic for content " + Train.MISSING_CONFIGURATION_VALUE_MESSAGE);
		this.mqTopicMessages = Objects.requireNonNull(properties.getProperty("mqTopicMessages"), "MQ topic for messages " + Train.MISSING_CONFIGURATION_VALUE_MESSAGE);
		this.mqTopicRealtime = Objects.requireNonNull(properties.getProperty("mqTopicRealtime"), " MQ topic for realtime " + Train.MISSING_CONFIGURATION_VALUE_MESSAGE);
		this.mqTopicGTFS = Objects.requireNonNull(properties.getProperty("mqTopicGTFS"), "MQ topic for GTFS " + Train.MISSING_CONFIGURATION_VALUE_MESSAGE);
		this.reconnectTimeoutOnConnectionFailure = Long.parseLong(
			properties.getProperty("reconnectTimeoutOnConnectionFailure", String.valueOf(this.reconnectTimeoutOnConnectionFailure))
		);
	}
	
	/**
	 * Initializes MQ connection, session and topic listeners.
	 */
	private void initializeMQProvisioning() {
		while (!this.connectionAvailable) {
			try {
				this.establishMQConnection(this.mqConnectionAddress);
				
				Train.LOGGER.log(Level.INFO, "Establishing MQ topic listeners.");
				this.establishListenerForMQTopicContent();
				this.establishListenerForMQTopicMessages();
				//this.establishListenerForMQTopicRealtime();
				//this.establishListenerForMQTopicGTFS();
			} catch (JMSException exception) {
				Train.LOGGER.log(Level.SEVERE, "Error occurred while trying to establish MQ connection or topic listener. Reason: {0}", exception.toString());
				try {
					Train.LOGGER.log(Level.INFO, "Trying to reconnect again after {0}", this.reconnectTimeoutOnConnectionFailure + " seconds.");
					Thread.sleep(this.reconnectTimeoutOnConnectionFailure * 1000L);
				} catch (InterruptedException exception2) {
					Train.LOGGER.log(Level.SEVERE, "Error occurred while waiting to reconnect. Reason: {0}", exception2.toString());
					Thread.currentThread().interrupt();
				}
			}
		}
	}
	
	/**
	 * Creates connection using connection factory and after that creates session from it.
	 * @param connectionAddress the connection address used to establish the connection (protocol://ipaddress:port).
	 * @throws JMSException
	 */
	private void establishMQConnection(final String connectionAddress) throws JMSException {
		Train.LOGGER.log(Level.INFO, "Establishing MQ connection to {0}", this.mqConnectionAddress);
		final ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(connectionAddress);
		this.connection = connectionFactory.createConnection();
		this.session = this.connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		this.connection.setExceptionListener(this);
		this.connection.start();
		this.connectionAvailable = true;
		Train.LOGGER.log(Level.INFO, "MQ connection successfully established.");
	}
	
	/**
	 * Establishes listener for MQ topic content for the current session.
	 * @throws JMSException
	 */
	private void establishListenerForMQTopicContent() throws JMSException {
		final MessageConsumer messageConsumer = this.session.createConsumer(this.session.createTopic(this.mqTopicContent));
		messageConsumer.setMessageListener(message -> {
			if (message instanceof ActiveMQBytesMessage) {
				final byte[] content = ((ActiveMQBytesMessage) message).getContent().getData();
				
				final Map<String, String> zipProperties = new HashMap<>();
				zipProperties.put("create", "true");
				zipProperties.put("encoding", "UTF-8");
				
				final Path zipPath = Paths.get(Train.WORK_DIRECTORY,  "content",  this.mqTopicContent + ".zip");
				final URI zipUri = URI.create("jar:" + zipPath.toUri());
				
				try (final FileSystem zipFileSystem = FileSystems.newFileSystem(zipUri, zipProperties)) {
					Files.write(zipPath, content, StandardOpenOption.CREATE);
					Train.LOGGER.log(Level.INFO, "MQ content received and assembled correctly.");
		        } catch (IOException exception) {
					Train.LOGGER.log(Level.SEVERE, "Something went wrong while trying to assemble content from MQ. Reason: {0}", exception.toString());
				}
			} else {
				Train.LOGGER.log(Level.WARNING, Train.ACTIVEMQ_WARNING_MESSAGE, this.mqTopicContent);
			}
		});
		Train.LOGGER.log(Level.INFO, Train.ESTABLISHED_LISTENER_MESSAGE, this.mqTopicContent);
	}
	
	/**
	 * Establishes listener for MQ topic messages for the current session.
	 * @throws JMSException
	 */
	private void establishListenerForMQTopicMessages() throws JMSException {
		final MessageConsumer messageConsumer = this.session.createConsumer(this.session.createTopic(this.mqTopicMessages));
		messageConsumer.setMessageListener(message -> {
			if (message instanceof ActiveMQBytesMessage) {
				final byte[] content = ((ActiveMQBytesMessage) message).getContent().getData();
				Train.LOGGER.log(Level.INFO, "Message received from MQ topic: {0}", new String(content));
			} else {
				Train.LOGGER.log(Level.WARNING, Train.ACTIVEMQ_WARNING_MESSAGE, this.mqTopicMessages);
			}
		});
		Train.LOGGER.log(Level.INFO, Train.ESTABLISHED_LISTENER_MESSAGE, this.mqTopicMessages);
	}
	
	/**
	 * Establishes listener for MQ topic realtime for the current session.
	 * @throws JMSException
	 */
	private void establishListenerForMQTopicRealtime() throws JMSException {
		final MessageConsumer messageConsumer = this.session.createConsumer(this.session.createTopic(this.mqTopicRealtime));
		messageConsumer.setMessageListener(message -> {
			if (message instanceof ActiveMQBytesMessage) {
				//final byte[] content = ((ActiveMQBytesMessage) message).getContent().getData();
				//TODO
			} else {
				Train.LOGGER.log(Level.WARNING, Train.ACTIVEMQ_WARNING_MESSAGE, this.mqTopicRealtime);
			}
		});
		Train.LOGGER.log(Level.INFO, Train.ESTABLISHED_LISTENER_MESSAGE, this.mqTopicRealtime);
	}
	
	/**
	 * Establishes listener for MQ topic gtfs for the current session.
	 * @throws JMSException
	 */
	private void establishListenerForMQTopicGTFS() throws JMSException {
		final MessageConsumer messageConsumer = this.session.createConsumer(this.session.createTopic(this.mqTopicGTFS));
		messageConsumer.setMessageListener(message -> {
			if (message instanceof ActiveMQBytesMessage) {
				//final byte[] content = ((ActiveMQBytesMessage) message).getContent().getData();
				//TODO
			} else {
				Train.LOGGER.log(Level.WARNING, Train.ACTIVEMQ_WARNING_MESSAGE, this.mqTopicGTFS);
			}
		});
		Train.LOGGER.log(Level.INFO, Train.ESTABLISHED_LISTENER_MESSAGE, this.mqTopicGTFS);
	}
	
	/**
	 * Closes the session and the connection.
	 * @throws JMSException
	 */
	private void destroyMQConnection() {
		try {
			if (this.connectionAvailable) {
				Train.LOGGER.log(Level.INFO, "Trying to close MQ connection.");
				if (this.session != null) {
					this.session.close();
				}
				if (this.connection != null) {
					this.connection.close();
				}
				Train.LOGGER.log(Level.INFO, "MQ connection successfully closed.");
			}
		} catch (JMSException exception) {
			Train.LOGGER.log(Level.SEVERE, "Error occurred while trying to destroy MQ connection. Reason: {0}", exception.toString());
		} finally {
			this.session = null;
			this.connection = null;
			this.connectionAvailable = false;
		}
	}
	
	/**
	 * Recovers the connection when it's disrupted.
	 */
	@Override
	public synchronized void onException(final JMSException exception) {
		Train.LOGGER.log(Level.SEVERE, "Connection to MQ has been disrupted. Reason: {0}", exception.toString());
		Train.LOGGER.log(Level.INFO, "Trying to reconnect...");
		this.destroyMQConnection();
		this.initializeMQProvisioning();
	}
	
}
