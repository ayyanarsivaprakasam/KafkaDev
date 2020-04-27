package com.learnkafka.producer.test;
 
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
 
/**
 * This program demonstrates how to write characters to a text file.
 * @author www.codejava.net
 *
 */
public class TextFileWritingExample1 
{
 
    public static void main(String[] args) throws IOException 
      {
    	
    	for (int i=0;i<=10;i++)
    	{
    		fileCreation("Hello");
    	}
    	
 
    }
    
    public static void fileCreation(String data) throws IOException
	{
    	Path path = Paths.get("C:\\Users\\sivap\\Tutorials\\MyFile.txt");
    	 
    	//Use try-with-resource to get auto-closeable writer instance
    	try (BufferedWriter writer = Files.newBufferedWriter(path)) 
    	{
    	    writer.write("Hello World !!");
    	}
    	
    	  
    	
    
 
	}}