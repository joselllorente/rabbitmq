package es.joseluisllorente.rabbit;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FSportsEventConsumer {

    private static final String EXCHANGE = "eventos-deportivos";
    static Logger logger = LoggerFactory.getLogger(FSportsEventConsumer.class);
    public static void main(String[] args) throws IOException, TimeoutException {
    	logger.info("Empezando Consumidor Eventos Deportivos");
        ConnectionFactory connectionFactory = new ConnectionFactory();
        // Abrir conexion
        Connection connection = connectionFactory.newConnection();
        // Establecer canal
        Channel channel = connection.createChannel();
        // Declarar exchange "eventos-deportivos"
        channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.TOPIC);
        // Crear cola y asociarla al exchange "eventos-deportivos"
        String queueName = channel.queueDeclare().getQueue();
        // Patron routing-key -> country.sport.eventType
        // * -> identifica una palabra
        // # -> identifica multiples palabras delimitadas por .
        // eventos tenis -> *.tenis.*
        // eventos en España -> es.# / es.*.*
        // todos los eventos -> #
        System.out.println("Introduzca routing-key: ");
        Scanner scanner = new Scanner(System.in);
        String routingKey = scanner.nextLine();

        channel.queueBind(queueName, EXCHANGE, routingKey);
        // Crear subscripcion a una cola asociada al exchange "eventos-deportivos"
        channel.basicConsume(queueName,
                true,
                (consumerTag, message) -> {
                    String messageBody = new String(message.getBody(), Charset.defaultCharset());
                    logger.info("Mensaje recibido: " + messageBody);
                    logger.info("Routing key: " + message.getEnvelope().getRoutingKey());
                },
                consumerTag -> {
                	logger.info("Consumidor " + consumerTag + " cancelado");
                });
    }
}
