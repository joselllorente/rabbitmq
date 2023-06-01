package es.joseluisllorente.rabbit;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DEventConsumer implements Queues{

    private static final String EXCHANGE_NAME = "eventos";
    static Logger logger = LoggerFactory.getLogger(DEventConsumer.class);
    
    public static void main(String[] args) throws IOException, TimeoutException {
    	logger.info("Empezando EventConsumer");
        ConnectionFactory connectionFactory = UtilsConnection.getConnectionFactory();
        // Abrir conexion
        Connection connection = connectionFactory.newConnection();
        // Establecer canal
        Channel channel = connection.createChannel();
        // Declarar exchange "eventos"
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);
        // Crear cola y asociarla al exchange "eventos"
        
        //Cada consumidor necesita su propia cola
        //Los consumidores leen una sola vez el mensaje una vez es consumido se elimina,
        //Si hay varios consumidores debe consumir de una cola
        
        //queueDeclare(): Genera cola de nombre aleatorio generado por el Broker exclusiva 
        //para garantizar que solo hay un consumidor conectado a ella
        //Será autodestruible para que sea destruida por el Broker una vez desconectado el consumidor
        //Y no durable para que si se reinicia el Broker ya no exista
        String queueName = channel.queueDeclare().getQueue();
        //Asocia cola al Exchange, sin routing key
        channel.queueBind(queueName, EXCHANGE_NAME, "");
        
        
        // Crear subscripcion a una cola asociada al exchange "eventos"
        channel.basicConsume(queueName,
                true,//autoACK, si debe enviar un "acuse de recibo por cada mensaje", 
                //si true el cliente recibe el acuse tan pronto se reciba el mensaje,
                //Rabbit lo marcará como recibido y lo eliminará de la cola. 
                //Si esta a false se mandará acuse de recibo explicitamente, si no se hace se mandará a otro consumidor
                //DeliverCallBack
                (consumerTag, message) -> {//Deliver t
                    String messageBody = new String(message.getBody(), Charset.defaultCharset());
                    logger.info("=====================================");
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
        //rabbitmqctl list_exchanges
        //rabbitmqctl list_queues
        //rabbitmqctl list_bindings
        
    }
}
