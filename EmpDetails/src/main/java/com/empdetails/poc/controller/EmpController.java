package com.empdetails.poc.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.empdetails.poc.entity.EmployeeAttribute;
import com.empdetails.poc.jpa.EmpDetailsJdbcRepository;
import com.empdetails.poc.jpa.EmpDetailsRepository;
import com.empdetails.poc.service.EmployeeService;
import com.fasterxml.jackson.core.JsonProcessingException;

@RestController
public class EmpController 
{
	  
    @Autowired
    private EmpDetailsRepository empDetailsRepository;
    
    @Autowired
    private EmpDetailsJdbcRepository empDetailsJdbcRepository;
    
    @Autowired
    EmployeeService employeeService;
	    
	 @RequestMapping(value = "/message/{id}", method = RequestMethod.GET)
	public String messagePublisher(@PathVariable int id) throws JsonProcessingException 
	{
		    //List<BookDetails> bookDetails=studentJdbcRepository.findAll();
		    
 	    	List<EmployeeAttribute> employeeAttributeList=empDetailsJdbcRepository.findAll();

 	    	int retval=employeeService.sendEmpDetailsEventAsynchronous(employeeAttributeList);
 	    

 	    	return "List of messages are successfully published!\n Please check the email to know the status.";
	}

}
