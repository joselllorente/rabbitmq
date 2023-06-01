package es.joseluisllorente.rabbit;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CEventProducers {

    private static final String EXCHANGE_NAME = "eventos";
    static Logger logger = LoggerFactory.getLogger(CEventProducers.class);
    
    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
    	logger.info("Empezando CEventProducers");
        ConnectionFactory connectionFactory = UtilsConnection.getConnectionFactory();
        // Abrir conexion AMQ y establecer canal
        try( Connection connection = connectionFactory.newConnection();
             Channel channel = connection.createChannel()) {
            // Crear fanout exchange "eventos"
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);

            int count = 1;
            // Enviar mensajes al fanout exchange "eventos"
            while (true) {
                String message = "Evento " + count;
                logger.info("Produciendo mensaje: " + message);
                //Nombre exchange,  routing_key vacio ya que es ignorado en FANOUT, propiedades null y msg en 
                channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes());
                Thread.sleep(1000);
                count++;
            }
        }
    }
}
