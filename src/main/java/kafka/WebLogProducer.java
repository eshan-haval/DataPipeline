package kafka;

import java.io.*;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.message.Message;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


/**
 * Created by ehaval on 11/12/15.
 */
public class WebLogProducer {

    public static void producer(File fin) throws IOException {

        //Kafka Props
        Properties props = new Properties();
        props.put("metadata.broker.list", "broker1:9092,broker2:9092 ");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "example.producer.SimplePartitioner");
        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);

        Producer<String, Message> kafkaProducer = new Producer<String, Message>(config);


        FileInputStream fis = new FileInputStream(fin);
        BufferedReader br = new BufferedReader(new InputStreamReader(fis));
        String line = null;

        while ((line = br.readLine()) != null) {
            Message m = new Message(line.getBytes());
            KeyedMessage<String, Message> data = new KeyedMessage<String, Message>("inputqueue",new Message(line.getBytes()));
            kafkaProducer.send(data);
        }
        kafkaProducer.close();
        br.close();
    }

    public static void main() throws IOException
    {
        File dir = new File(".");
        producer(new File(dir.getCanonicalPath() + File.separator + "in.txt"));
    }
}
