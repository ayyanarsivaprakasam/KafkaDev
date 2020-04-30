package com.empdetails.poc.producer;

import com.empdetails.poc.entity.EmployeeAttribute;
import com.empdetails.poc.jpa.EmpDetailsJdbcRepository;
import com.empdetails.poc.jpa.EmpDetailsRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.ArrayList;
import java.util.List;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Configuration
@EnableScheduling
@Component
public class EmpDetaillsProducer
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
	    static   
	    {
	    		    	
	    	EmployeeAttribute emp1=new EmployeeAttribute();
	    	emp1.setEmpID(1);
	    	emp1.setEmpName("Ram");
	    	emp1.setEmpDepartment("Computer");
	    	
	    	EmployeeAttribute emp2=new EmployeeAttribute();
	    	emp2.setEmpID(2);
	    	emp2.setEmpName("Sachin");
	    	emp2.setEmpDepartment("Computer");
	    	
	    	EmployeeAttribute emp3=new EmployeeAttribute();
	    	emp3.setEmpID(3);
	    	emp3.setEmpName("Ramesh");
	    	emp3.setEmpDepartment("Electrical");
	    	
	    	EmployeeAttribute emp4=new EmployeeAttribute();
	    	emp4.setEmpID(4);
	    	emp4.setEmpName("Dan");
	    	emp4.setEmpDepartment("Mechanical");
	    	
	    	EmployeeAttribute emp5=new EmployeeAttribute();
	    	emp5.setEmpID(5);
	    	emp5.setEmpName("Ramkumar");
	    	emp5.setEmpDepartment("Computer");
	    	
	    		    	
	    	EmployeeAttribute emp6=new EmployeeAttribute();
	    	emp6.setEmpID(6);
	    	emp6.setEmpName("Santhosh");
	    	emp6.setEmpDepartment("Electrical");
	    	
	    	
	    	EmployeeAttribute emp7=new EmployeeAttribute();
	    	emp7.setEmpID(7);
	    	emp7.setEmpName("Kumar");
	    	emp7.setEmpDepartment("Production");
	    	
	    	
	    	EmployeeAttribute emp8=new EmployeeAttribute();
	    	emp8.setEmpID(8);
	    	emp8.setEmpName("Sachin");
	    	emp8.setEmpDepartment("IT");
	    	
	    	
	    	EmployeeAttribute emp9=new EmployeeAttribute();
	    	emp9.setEmpID(9);
	    	emp9.setEmpName("Karthik");
	    	emp9.setEmpDepartment("Computer");
	    	
	    	
	    	EmployeeAttribute emp10=new EmployeeAttribute();
	    	emp10.setEmpID(10);
	    	emp10.setEmpName("Santhosh");
	    	emp10.setEmpDepartment("Electrical");
	    	
	    	empattributeList.add(emp1);
	    	empattributeList.add(emp2);
	    	empattributeList.add(emp3);
	    	empattributeList.add(emp4);
	    	empattributeList.add(emp5);	    
	    	empattributeList.add(emp6);
	    	empattributeList.add(emp7);	
	    	empattributeList.add(emp8);	
	    	empattributeList.add(emp9);	
	    	empattributeList.add(emp10);		    	
	    	
	    }
	    
	  	    
	    @Scheduled(cron="0 0/2 1-23 * * MON-FRI") 
	    public void  sendEmpDetailsEventAsynchronous() throws JsonProcessingException
	    {
	    	counter.set(0);
	    	onSuccess.set(0);
	    	onFailure.set(0);
	    	listSize=0;
	    	listSize=empattributeList.size();
	    	
	    	 logger.info("empattributeList.size:"+ empattributeList.size());
	    	 	
	       	 for(EmployeeAttribute emp : empattributeList)
	 	    {
	    	
	 	    	EmployeeAttribute employeeAttribute=new EmployeeAttribute();
	 	    	employeeAttribute.setEmpID(emp.getEmpID());
	 	    	employeeAttribute.setEmpName(emp.getEmpName());
	 	    	employeeAttribute.setEmpDepartment(emp.getEmpDepartment());
	 	    
		        Integer key = employeeAttribute.getEmpID();
		        String value = objectMapper.writeValueAsString(employeeAttribute);
	    	
		        try
		        {
		        	if (key==5)
		        	{
		        
		            Thread.sleep(30000);
		        	}
		        }
		        catch(InterruptedException ex)
		        {
		            Thread.currentThread().interrupt();
		        }
		    
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
	
	// @Scheduled(cron="0 40 15 * * MON-FRI")
    public void sendEmpDetailsEventSynchronous() throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException
    {
    	
    	 for(EmployeeAttribute emp : empattributeList)
 	    {
 	    	
 	    	EmployeeAttribute employeeAttribute=new EmployeeAttribute();
 	    	employeeAttribute.setEmpID(emp.getEmpID());
 	    	employeeAttribute.setEmpName(emp.getEmpName());
 	    	employeeAttribute.setEmpDepartment(emp.getEmpDepartment());
 	    
	        Integer key = employeeAttribute.getEmpID();
	        String value = objectMapper.writeValueAsString(employeeAttribute);
	        SendResult<Integer,String> sendResult=null;
	        try
	        {
	        	sendResult = kafkaTemplate.sendDefault(key,value).get(1, TimeUnit.SECONDS);
	        	logger.info("Message Sent SuccessFully for the key : {} and the value is {} , partition is {}", key, value, sendResult.getRecordMetadata().partition());

	        } 
	        catch (ExecutionException | InterruptedException e) 
	        {
    	    	
           throw e;
	        } 
	        catch (Exception e)
	        {
        	       	
        	
        	 //onFailure++;
            // logger.info("onFailure--------->:"+onFailure);
             EmployeeAttribute libraryEvent1 = objectMapper.readValue(value, EmployeeAttribute.class);
        	
			
             EmployeeAttribute empAtr=new EmployeeAttribute();
             empAtr.setEmpID(employeeAttribute.getEmpID());
             empAtr.setEmpName(employeeAttribute.getEmpName());
             empAtr.setEmpDepartment(employeeAttribute.getEmpDepartment());
			
             empDetailsRepository.save(empAtr);
		
		     logger.info("Successfully Persisted the libary Event {} ", libraryEvent1);
		     logger.error("Exception Sending the Message and the exception is {}", e.getMessage());

           throw e;
          
	        }
 	    }

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
