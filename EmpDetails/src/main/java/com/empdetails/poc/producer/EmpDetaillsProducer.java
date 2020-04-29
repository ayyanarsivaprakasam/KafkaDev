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
    
    String topic = "library-events";
    
    @Autowired
    ObjectMapper objectMapper;
    
    
    public int onFailure=0,onSuccess=0;


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
	    	    	
	    	empattributeList.add(emp1);
	    	empattributeList.add(emp2);
	    	empattributeList.add(emp3);
	    	empattributeList.add(emp4);
	    	empattributeList.add(emp5);	    	
	    	
	    }
	    

	 @Scheduled(cron="0 40 15 * * MON-FRI")
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

           throw e;
          
	        }
 	    }

    }

}
