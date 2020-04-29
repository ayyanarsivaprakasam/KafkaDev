package com.empdetails.poc.jpa;

import org.springframework.data.repository.CrudRepository;

import com.empdetails.poc.entity.EmployeeAttribute;

public interface EmpDetailsRepository extends CrudRepository<EmployeeAttribute,Integer> {
}
