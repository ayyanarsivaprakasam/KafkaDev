package com.empdetails.poc.jpa;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import com.empdetails.poc.entity.EmployeeAttribute;


@Repository
public class EmpDetailsJdbcRepository 
{

    @Autowired
    JdbcTemplate jdbcTemplate;

    class EmpDetailsRowMapper implements RowMapper <EmployeeAttribute> 
    {

        @Override
        public EmployeeAttribute mapRow(ResultSet rs, int rowNum) throws SQLException {
        	
        	EmployeeAttribute employeeAttribute = new EmployeeAttribute();
        	
        	employeeAttribute.setEmpID(rs.getInt("EMPID"));
        	employeeAttribute.setEmpName(rs.getString("EMP_NAME"));
        	employeeAttribute.setEmpDepartment(rs.getString("EMP_DEPARTMENT"));
         
        	
        	return employeeAttribute;
        }



    }

   
    public List <EmployeeAttribute> findAll() {

        return jdbcTemplate.query("select * from Employee_Attribute", new EmpDetailsRowMapper());

    }






}