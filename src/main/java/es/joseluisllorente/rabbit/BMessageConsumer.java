package es.joseluisllorente.rabbit;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class BMessageConsumer implements Queues{

    static Logger logger = LoggerFactory.getLogger(BMessageConsumer.class);
    
    public static void main(String[] args) throws IOException, TimeoutException {
    	logger.info("Empezando MessageConsumer");
        ConnectionFactory connectionFactory = UtilsConnection.getConnectionFactory();
        // Abrir conexion
        Connection connection = connectionFactory.newConnection();
        // Establecer canal
        Channel channel = connection.createChannel();
        // Crear cola y asociarla al exchange "eventos", 
        // Si la cola ya existe dara error y cerrará el canal por eso  
        channel.queueDeclare(QUEUE1, false, false, false, null);
        logger.info("Queue"+QUEUE1);
        
        // Crear subscripcion a una cola asociada al exchange "eventos"
        channel.basicConsume(QUEUE1,
                true,//autoACK, si debe enviar un "acuse de recibo por cada mensaje", 
                //si true el cliente recibe el acuse tan pronto se reciba el mensaje,
                //Rabbit lo marcará como recibido y lo eliminará de la cola. 
                //Si esta a false se mandará acuse de recibo explicitamente, si no se hace se mandará a otro consumidor
                //DeliverCallBack
                (consumerTag, message) -> {//Deliver
                    String messageBody = new String(message.getBody(), Charset.defaultCharset());
                    logger.info("===================================");
                    logger.info("Consumidor: " + consumerTag);
                    logger.info("Mensaje recibido: " + messageBody);
                    logger.info("Exchange: " + message.getEnvelope().getExchange());
                    logger.info("Routing key: " + message.getEnvelope().getRoutingKey());
                    logger.info("Delivery tag: " + message.getEnvelope().getDeliveryTag());//Identificador del mensaje dentro del canal
                },
                //CancelCallBack
                consumerTag -> {
                	logger.info("Consumidor " + consumerTag + " cancelado");
                });
        
        
        //La ejecucion no finaliza hasta que cerremos el canal y la conexion
        //Ir a http://localhost:9890/ --> Queues y borrar queue para que salte el evento de Cancel
        
        logger.info("Finalizando ConsumidorEventos");
    }
}
