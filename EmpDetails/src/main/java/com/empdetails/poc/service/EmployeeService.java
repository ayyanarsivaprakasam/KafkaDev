package com.empdetails.poc.service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.empdetails.poc.entity.EmployeeAttribute;
import com.empdetails.poc.jpa.EmpDetailsJdbcRepository;
import com.empdetails.poc.jpa.EmpDetailsRepository;
import com.empdetails.poc.producer.EmpDetaillsProducer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;


@Component
@Slf4j
public class EmployeeService
{

	

    @Autowired
    KafkaTemplate<Integer,String> kafkaTemplate;
    
    
    @Autowired
    private EmpDetailsRepository empDetailsRepository;
    
    String topic = "employee-details";
    
    @Autowired
    ObjectMapper objectMapper;
    
    @Autowired
	 EmpDetailsJdbcRepository empDetailsJdbcRepository;

    public static AtomicInteger counter = new AtomicInteger(0);
    public static AtomicInteger onFailure = new AtomicInteger(0);
    public static AtomicInteger onSuccess = new AtomicInteger(0);
    public int listSize=0;

	private static final Logger logger= LogManager.getLogger(EmpDetaillsProducer.class);
	
		
	 private static List<EmployeeAttribute> empattributeList=new ArrayList<EmployeeAttribute>();
	   
	  	    
	    public int  sendEmpDetailsEventAsynchronous(List<EmployeeAttribute> employeeAttributeList) throws JsonProcessingException
	    {
	    	counter.set(0);
	    	onSuccess.set(0);
	    	onFailure.set(0);
	    	listSize=0;
	    	listSize=employeeAttributeList.size();
	    	
	    	 logger.info("empattributeList.size:"+ employeeAttributeList.size());
	    	 	
	       	 for(EmployeeAttribute emp : employeeAttributeList)
	 	    {
	    	
	 	    	EmployeeAttribute employeeAttribute=new EmployeeAttribute();
	 	    	employeeAttribute.setEmpID(emp.getEmpID());
	 	    	employeeAttribute.setEmpName(emp.getEmpName());
	 	    	employeeAttribute.setEmpDepartment(emp.getEmpDepartment());
	 	    
		        Integer key = employeeAttribute.getEmpID();
		        String value = objectMapper.writeValueAsString(employeeAttribute);
	    	
		    
		        ProducerRecord<Integer,String> producerRecord = buildProducerRecord(key, value, topic);

		        ListenableFuture<SendResult<Integer,String>> listenableFuture =  kafkaTemplate.send(producerRecord);

		        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>()
		        {
	            @Override
	            public void onFailure(Throwable ex) 
	            {
	            	
	                try 
	                {
	                
	                     EmployeeAttribute libraryEvent1 = objectMapper.readValue(value, EmployeeAttribute.class);
	                	
	        			
	                     EmployeeAttribute empAtr=new EmployeeAttribute();
	                     empAtr.setEmpID(employeeAttribute.getEmpID());
	                     empAtr.setEmpName(employeeAttribute.getEmpName());
	                     empAtr.setEmpDepartment(employeeAttribute.getEmpDepartment());
	                    
	                   
	                     empDetailsRepository.save(empAtr);
	                	 onFailure.getAndIncrement();
	                		               	 
	                	 counter.getAndIncrement();
	                	 onSuccessMail(counter);
		            	
	                  //  logger.info("onFailure--------->:"+onFailure);
	                  
	        		     logger.info("Successfully Persisted the libary Event {} ", libraryEvent1);
	        		     logger.error("Exception Sending the Message and the exception is {}", ex.getMessage());
						
						
					} catch (JsonMappingException e)
	                {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (JsonProcessingException e)
	                {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

	                handleFailure(key, value, ex);
	            }

	            @Override
	            public void onSuccess(SendResult<Integer, String> result) 
	            {
	            	 onSuccess.getAndIncrement();
	            	 counter.getAndIncrement();
	            	
	            	 onSuccessMail(counter);
	                handleSuccess(key, value, result);
	            }
	            
	        });
	 	    }
	       	 
	        
	       	 int max;
	    	return max = (counter.intValue()==listSize) ? counter.intValue() : 0;


	    	
	     
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
	    
	    
	    private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value, String topic)
	    {
	     	return new ProducerRecord<>(topic, null, key, value, null);
	     }
	    
	    public void onSuccessMail(AtomicInteger a)
	    {   	
	    	logger.info("onSuccess :"+onSuccess.intValue());
	    	logger.info("onFailure :"+onFailure.intValue());
	    	
	    	if (a.intValue()==listSize)
	    	{
		    	logger.info("Number of messages are successfully processed for today is "+onSuccess.intValue());
		    	logger.info("Number of messages are failed to process for today is "+ onFailure.intValue());
	    	}    	
	    	
	    }
	  
}
