package com.empdetails.poc.entity;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
@Entity  
@Table
public class EmployeeAttribute 
{
	@Id  
	@Column  
	private int empID;
	
	@Column
	private String empName;
	
	@Column
	private String empDepartment;
	
	public EmployeeAttribute()
	{}
	
	public EmployeeAttribute(int empID, String empName, String empDepartment) {
		super();
		this.empID = empID;
		this.empName = empName;
		this.empDepartment = empDepartment;
	}

	
	public int getEmpID() {
		return empID;
	}
	public void setEmpID(int empID) {
		this.empID = empID;
	}
	public String getEmpName() {
		return empName;
	}
	public void setEmpName(String empName) {
		this.empName = empName;
	}
	public String getEmpDepartment() {
		return empDepartment;
	}
	public void setEmpDepartment(String empDepartment) {
		this.empDepartment = empDepartment;
	}
	

}
