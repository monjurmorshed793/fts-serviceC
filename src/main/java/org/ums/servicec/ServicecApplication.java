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
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import javax.websocket.server.PathParam;

@SpringBootApplication
@RestController("/")
public class ServicecApplication {
    public static Logger logger = LoggerFactory.getLogger(ServicecApplication.class);
    @Autowired
    private KafkaTemplate<String, String> template;

	public static void main(String[] args) {
		SpringApplication.run(ServicecApplication.class, args);
	}

  @GetMapping("/service-c")
  public boolean getReponse()throws Exception{
    RestTemplate restTemplate = new RestTemplate();
    Thread.sleep(500);
    return true;
  }

  @KafkaListener(topics = "service-c")
  public void listen(ConsumerRecord<?, ?> cr) throws Exception {

            logger.info(cr.key().toString());
            logger.info(cr.value().toString());
    for(int i=1;i<=Integer.parseInt(cr.key().toString());i++){
      ObjectMapper mapper = new ObjectMapper();
      ServiceStatus receivedServiceStatus = mapper.readValue(cr.value().toString(), ServiceStatus.class);
      ServiceStatus serviceStatus = new ServiceStatus();
      serviceStatus.setParentServiceId(receivedServiceStatus.getServiceId());
      serviceStatus.setServiceId("serviceC"+i);
      serviceStatus.setStatus(true);
      String jsonObject = mapper.writeValueAsString(serviceStatus);
      template.send("tracker", "serviceC", jsonObject);
    }

    Thread.sleep(500);


    }
}
