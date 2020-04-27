package com.learnkafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.entity.BookDetails;
import com.learnkafka.jpa.LibraryEventsRepository;
import com.learnkafka.controller.LibraryEventControllerAdvice;
import com.learnkafka.domain.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.io.IOException;
import java.net.PasswordAuthentication;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

@Component
@Slf4j
public class LibraryEventProducer {

    @Autowired
    KafkaTemplate<Integer,String> kafkaTemplate;
    
    
    @Autowired
    private LibraryEventsRepository libraryEventsRepository;
    
    String topic = "library-events";
    
    @Autowired
    ObjectMapper objectMapper;
    



	private static final Logger logger= LogManager.getLogger(LibraryEventProducer.class);

    public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {

        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);

        ListenableFuture<SendResult<Integer,String>> listenableFuture =  kafkaTemplate.sendDefault(key,value);
        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable ex) {
                handleFailure(key, value, ex);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess(key, value, result);
            }
        });
    }

    public ListenableFuture<SendResult<Integer,String>> sendLibraryEvent_Approach2(LibraryEvent libraryEvent) throws JsonProcessingException {

        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);

        ProducerRecord<Integer,String> producerRecord = buildProducerRecord(key, value, topic);

        ListenableFuture<SendResult<Integer,String>> listenableFuture =  kafkaTemplate.send(producerRecord);

        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable ex) {
            	
                try {
                	                 	
                	logger.info("--------------------------------");
                	logger.info("key :"+key);
                	
                	
					LibraryEvent libraryEvent = objectMapper.readValue(producerRecord.value(), LibraryEvent.class);
					logger.info("libraryEventId :" +libraryEvent.getLibraryEventId());
					logger.info("LibraryEventType :" +libraryEvent.getLibraryEventType());
					
					Book ob=new Book();
					ob=libraryEvent.getBook();
					logger.info("bookId :" +ob.getBookId());
					logger.info("bookName :" +ob.getBookName());
					logger.info("bookAuthor :" +ob.getBookAuthor());
					
					
					BookDetails ob1= new BookDetails();
					ob1.setLibraryEventId(libraryEvent.getLibraryEventId());
					ob1.setLibraryEventType("");
					ob1.setBookId(ob.getBookId());
					ob1.setBookName(ob.getBookName());
					ob1.setBookAuthor(ob.getBookAuthor());	
				
				     libraryEventsRepository.save(ob1);
				        logger.info("Successfully Persisted the libary Event {} ", libraryEvent);
				        
				       
					
                	logger.info("--------------------------------");


					

			    	//value{"libraryEventId":305,"libraryEventType":"NEW","book":{"bookId":456,"bookName":"Kafka Using Spring Boot","bookAuthor":"teststt"}}

					
					
				} catch (JsonMappingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (JsonProcessingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

                handleFailure(key, value, ex);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess(key, value, result);
            }
        });

        return listenableFuture;
    }

 

    public SendResult<Integer, String> sendLibraryEventSynchronous(LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, 
    InterruptedException, TimeoutException 
    {

        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);
        SendResult<Integer,String> sendResult=null;
        try {
            sendResult = kafkaTemplate.sendDefault(key,value).get(1, TimeUnit.SECONDS);
        } catch (ExecutionException | InterruptedException e) 
        {
        	logger.error("ExecutionException/InterruptedException Sending the Message and the exception is {}", e.getMessage());
            throw e;
        } catch (Exception e)
        {
        	logger.info("--------------------------------");
        	logger.info("key :"+key);
        	
        	
			LibraryEvent libraryEvent1 = objectMapper.readValue(value, LibraryEvent.class);
			logger.info("libraryEventId :" +libraryEvent1.getLibraryEventId());
			logger.info("LibraryEventType :" +libraryEvent1.getLibraryEventType());
			
			Book ob=new Book();
			ob=libraryEvent1.getBook();
			logger.info("bookId :" +ob.getBookId());
			logger.info("bookName :" +ob.getBookName());
			logger.info("bookAuthor :" +ob.getBookAuthor());
			
			
			BookDetails ob1= new BookDetails();
			ob1.setLibraryEventId(libraryEvent1.getLibraryEventId());
			ob1.setLibraryEventType("");
			ob1.setBookId(ob.getBookId());
			ob1.setBookName(ob.getBookName());
			ob1.setBookAuthor(ob.getBookAuthor());	
		
		     libraryEventsRepository.save(ob1);
		        logger.info("Successfully Persisted the libary Event {} ", libraryEvent1);
		        
		       
			
        	logger.info("--------------------------------");
        	
        	logger.error("Exception Sending the Message and the exception is {}", e.getMessage());
            throw e;
        }

        return sendResult;

    }

    
    private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value, String topic) {


        List<Header> recordHeaders = List.of(new RecordHeader("event-source", "scanner".getBytes()));

        return new ProducerRecord<>(topic, null, key, value, recordHeaders);
    }

    
    private void handleFailure(Integer key, String value, Throwable ex) {
    	logger.error("Error Sending the Message and the exception is {}", ex.getMessage());
    	
    	//logger.info("--------------------------------");
    	//logger.info("key :"+key);
    	
    	//logger.info("value" +value.toString());

    	
    	//value{"libraryEventId":305,"libraryEventType":"NEW","book":{"bookId":456,"bookName":"Kafka Using Spring Boot","bookAuthor":"teststt"}}
    	
      	//logger.info("--------------------------------");

        try {
            throw ex;
        } catch (Throwable throwable) {
        	logger.error("Error in OnFailure: {}", throwable.getMessage());
        	
        	
        	
        }


    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
    	logger.info("Message Sent SuccessFully for the key : {} and the value is {} , partition is {}", key, value, result.getRecordMetadata().partition());
    }
  
}
