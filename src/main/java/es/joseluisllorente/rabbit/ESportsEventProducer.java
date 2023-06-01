package es.joseluisllorente.rabbit;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ESportsEventProducer {

    private static final String EXCHANGE = "eventos-deportivos";
    static Logger logger = LoggerFactory.getLogger(ESportsEventProducer.class);
    
    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
    	logger.info("Empezando Productor Eventos Deportivos");
        ConnectionFactory connectionFactory = new ConnectionFactory();
        // Abrir conexion AMQ y establecer canal
        try( Connection connection = connectionFactory.newConnection();
             Channel channel = connection.createChannel()) {
            // Crear topic exchange "eventos-deportivos"
            channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.TOPIC);
            // pais: es, fr, usa
            List<String> countries = Arrays.asList("es", "fr", "usa");
            // deporte: futbol, tenis, voleibol
            List<String> sports = Arrays.asList("futbol", "tenis", "voleibol");
            // tipo evento: envivo, noticia
            List<String> eventTypes = Arrays.asList("envivo", "noticia");

            int count = 1;
            // Enviar mensajes al topic exchange "eventos-deportivos"
            while (true) {
            	//Permuto los elementos de cada lista
                shuffle(countries, sports, eventTypes);
                String country = countries.get(0);
                String sport = sports.get(0);
                String eventType = eventTypes.get(0);
                // routing-key -> country.sport.eventType
                String routingKey = country + "." + sport + "." + eventType;

                String message = "Evento " + count;
                logger.info("Produciendo mensaje (" + country + ", " + sport + ", " + eventType + "): " + message);

                channel.basicPublish(EXCHANGE, routingKey, null, message.getBytes());
                Thread.sleep(1000);
                count++;
            }
        }
    }

    private static void shuffle(List<String> ...listas) {
    	for (List<String> lista : listas) {
    		Collections.shuffle(lista);
		}
        
    }
}
