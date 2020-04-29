package com.empdetails.poc.producer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.empdetails.poc.entity.EmployeeAttribute;
import com.empdetails.poc.jpa.EmpDetailsJdbcRepository;
import com.empdetails.poc.jpa.EmpDetailsRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
@Configuration
@EnableScheduling
@Component
public class EmpDetaillsScheduler
{
	private static final Logger logger= LogManager.getLogger(EmpDetaillsScheduler.class);

		
	 @Autowired
	 EmpDetaillsProducer empDetaillsProducer;
	 
	 @Autowired
	 EmpDetailsJdbcRepository empDetailsJdbcRepository;
	 
	 @Autowired
	 KafkaTemplate<Integer,String> kafkaTemplate;
	 

	  @Autowired
	  private EmpDetailsRepository empDetailsRepository;

	 @Autowired
	 ObjectMapper objectMapper;
	  
	  public int onSuccess=0;
	  public int onFailure=0;
	 	   
	  @Scheduled(cron="0 46 15 * * MON-FRI")
	public void scheduleTaskUsingCronExpression() throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException
	{
	  
	  
	    List<EmployeeAttribute> employeeAttributeList=empDetailsJdbcRepository.findAll();

	    for(EmployeeAttribute emp : employeeAttributeList)
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
	        	 onSuccess++;
	             logger.info("onSuccess--------->:"+onSuccess);
	        	sendResult = kafkaTemplate.sendDefault(key,value).get(1, TimeUnit.SECONDS);
	        	logger.info("Message Sent SuccessFully for the key : {} and the value is {} , partition is {}", key, value, sendResult.getRecordMetadata().partition());

	        	
	        } 
	        catch (ExecutionException | InterruptedException e) 
	        {
    	    	
           //throw e;
	        } 
	        catch (Exception e)
	        {
        	       	
        	
        	 onFailure++;
             logger.info("onFailure--------->:"+onFailure);
             EmployeeAttribute libraryEvent1 = objectMapper.readValue(value, EmployeeAttribute.class);
        	
			
             EmployeeAttribute empAtr=new EmployeeAttribute();
             empAtr.setEmpID(employeeAttribute.getEmpID());
             empAtr.setEmpName(employeeAttribute.getEmpName());
             empAtr.setEmpDepartment(employeeAttribute.getEmpDepartment());
			
             empDetailsRepository.save(empAtr);
		
		     logger.info("Successfully Persisted the libary Event {} ", libraryEvent1);
		     logger.error("Exception Sending the Message and the exception is {}", e.getMessage());

           //throw e;
          
	        }
 	    }
	}

}




