package es.joseluisllorente.rabbit;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ASimpleProducer implements Queues{
	static Logger logger = LoggerFactory.getLogger(ASimpleProducer.class);
    public static void main(String[] args) throws IOException, TimeoutException {
    	logger.debug("Empezando");
        String message = "!RabbitMQ - Hola mundo2!";

        // Abrir conexion AMQ y establecer canal
        ConnectionFactory connectionFactory = UtilsConnection.getConnectionFactory();

        //Bloque try with resources, se ponen las instrucciones que crean los recursos, 
        //se cierran solos porque implementan las interfaces Closeable y Autocloseable
        try(Connection connection = connectionFactory.newConnection();
        		//Establecemos el canal
        		Channel channel = connection.createChannel() ){

	        // Crear cola (nombre durable, exclusive, autodestruible, argumentos adicionales en Map)
	        channel.queueDeclare(QUEUE1, false, false, false, null);
	        // Enviar mensaje al exchange "" (nombreExchange, routinkey, propiedades y msg en bytes)
	        logger.info("Publicando mensaje "+message + " en la cola"+ QUEUE1);
	        channel.basicPublish("", QUEUE1, null, message.getBytes());
	        logger.info("Publicado");
        }
        
        logger.debug("Finalizado");
//        channel.close();
//        connection.close();
    }
}
