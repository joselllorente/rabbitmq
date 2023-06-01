package es.joseluisllorente.rabbit;

import com.rabbitmq.client.ConnectionFactory;

public class UtilsConnection {

	public static ConnectionFactory getConnectionFactory() {
		ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUsername("ubuntu");
        connectionFactory.setPassword("password");
        connectionFactory.setPort(8888);
        
        return connectionFactory;
	}
	
}
