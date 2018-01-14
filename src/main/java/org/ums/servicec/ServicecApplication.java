package org.ums.servicec;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootApplication
public class ServicecApplication {
    public static Logger logger = LoggerFactory.getLogger(ServicecApplication.class);
    @Autowired
    private KafkaTemplate<String, String> template;

	public static void main(String[] args) {
		SpringApplication.run(ServicecApplication.class, args);
	}

  @KafkaListener(topics = "my_topic")
  public void listen(ConsumerRecord<?, ?> cr) throws Exception {
	    if(cr.key().toString().equals("serviceC")){
            logger.info(cr.key().toString());
            logger.info(cr.value().toString());
        ObjectMapper mapper = new ObjectMapper();
        ServiceStatus receivedServiceStatus = mapper.readValue(cr.value().toString(), ServiceStatus.class);
        ServiceStatus serviceStatus = new ServiceStatus();
        serviceStatus.setParentServiceId(receivedServiceStatus.getServiceId());
        serviceStatus.setServiceId("serviceC");
        serviceStatus.setStatus(true);
            String jsonObject = mapper.writeValueAsString(mapper);
            template.send("tracker", "serviceC", jsonObject);
        }

    }
}
