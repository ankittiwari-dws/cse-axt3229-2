/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.*;
import org.apache.commons.dbcp.*;
import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import redis.clients.jedis.Jedis;
/**
 *
 * @author aa
 */
public class Proj {
    public static void main(String[] args)
    {
        System.out.println("Enter your choice do you want to work with 1.postgre 2.Redis 3.Mongodb");
        InputStreamReader input=new InputStreamReader(System.in);
        BufferedReader data=new BufferedReader(input);
        String choice=null;
        try {
        choice= data.readLine();
        }
        catch(Exception Eex)
        {
            System.out.println(Eex.getMessage());
        }
        
       // loading data from the CSV file 
        
        CsvReader Details=null;
                        
                        try
                        {
                            Details = new CsvReader("folder\\dataset.csv");
                        }
                        catch(FileNotFoundException EB)
                        {
                            System.out.println(EB.getMessage());
                        }
                        int ColumnCount=3;
                        int RowCount =100;
                        int k=0;
                        
                        try 
                        {
                         Details = new CsvReader("folder\\dataset.csv");
                        }
                        catch(FileNotFoundException E)
                        {
                                System.out.println(E.getMessage());
                        }
                     
                        String[][] Dataarray= new String[RowCount][ColumnCount];
                        try
                        {
                            while (Details.readRecord())
                            {    
                            String v;
                            String[] x;
                            v = Details.getRawRecord();
                            x =v.split(",");
                               for(int j=0;j<ColumnCount;j++)
                                {
                                    String value=null;
                                    int z=j;
                                    value=x[z];
                                    try
                                    {
                                    Dataarray[k][j]=value;
                                    }
                                    catch(Exception E)
                                    {
                                        System.out.println(E.getMessage());
                                    }
                                   // System.out.println(Dataarray[iloop][j]);
                                    }
                            k=k+1;    
                            }
                            }
                        catch(IOException Em)
                        {
                        System.out.println(Em.getMessage());
                        }
                        
	Details.close();
        
        
        // connection to Database 
       switch(choice)
               {
                   // postgre code goes here 
                   case "1":
                       
        Connection Conn=null;
        Statement Stmt=null;
        URI dbUri =null;
        InputStreamReader in=new InputStreamReader(System.in);
        BufferedReader out=new BufferedReader(in);
         try 
        {
         Class.forName("org.postgresql.Driver");    
        }
         catch(Exception E1)
        {
         System.out.println(E1.getMessage());
        }
        String username = "ejepndckpvzvrj";
        String password = "uBq0RRYh47bRt_vxrZZDC14ois";
        String dbUrl = "postgres://ejepndckpvzvrj:uBq0RRYh47bRt_vxrZZDC14ois@ec2-54-83-53-120.compute-1.amazonaws.com:5432/d1b2vsoh5fhl6n";
        
        
        
        
         // now update data in the postgress Database 
        
        for(int i=0;i<RowCount;i++)
        {
             try 
                {
               
                Conn= DriverManager.getConnection(dbUrl, username, password);    
                 Stmt = Conn.createStatement();
                 String Connection_String = "insert into Details (first_name,last_name,county) values (' "+ Dataarray[i][0]+" ',' "+ Dataarray[i][1]+" ',' "+ Dataarray[i][2]+" ')";
                 Stmt.executeUpdate(Connection_String);
               
                }
                catch(SQLException E4)
                {
                   System.out.println(E4.getMessage());
                }
        }
        
        // Quering with the Database 
        
        System.out.println("1. Display Data ");
        System.out.println("2. Select data based on Other attributes e.g Name");
        System.out.println("Enter your Choice ");
        try {
        choice=out.readLine();
        }
        catch(IOException E)
        {
            System.out.println(E.getMessage());
        }
        switch(choice)
        {
            case "1": 
                 try 
                 {
                 Conn= DriverManager.getConnection(dbUrl, username, password);    
                 Stmt = Conn.createStatement();
                 String Connection_String = " Select * from Details;";
                 ResultSet objRS = Stmt.executeQuery(Connection_String);
                 while(objRS.next())
                 {
                 System.out.println("First name " + objRS.getInt("First_name"));
                 System.out.println("Last name " + objRS.getString("Last_name"));
                 System.out.println("COunty " + objRS.getString("County"));
                 
                 }
                 
                 }
                 catch(Exception E2)
                 {
                 System.out.println(E2.getMessage());
                 }
                 
            break;
                
            case "2":
                String Name=null;
                System.out.println("Enter First Name to find the record");
                InputStreamReader obj=new InputStreamReader(System.in);
                BufferedReader bur=new BufferedReader(obj);
              
                try
                {
                Name=(bur.readLine()).toString();
                }
                catch(IOException E)
                {
                    System.out.println(E.getMessage());
                }
            try 
                 {
                 Conn= DriverManager.getConnection(dbUrl, username, password);    
                 Stmt = Conn.createStatement();
                 String Connection_String = " Select * from Details Distinct where Name=" + "'" + Name + "'" +  ";";
            
                 ResultSet objRS = Stmt.executeQuery(Connection_String);
                 while(objRS.next())
                 {
                 System.out.println("First name: " + objRS.getInt("First_name"));
                 System.out.println("Last name " + objRS.getString("Last_name"));
                  System.out.println("County " + objRS.getString("County"));
                 }
                 
                 }
                 catch(Exception E2)
                 {
                 System.out.println(E2.getMessage());
                 }
                break;
        }
        try
        {
        Conn.close();
        }
       catch(SQLException E6)
       {
           System.out.println(E6.getMessage());
       }
                   break;
                       
                       // Redis code goes here 
                       
                   case "2":
        int Length=0;
        String ID=null;
        Length= 100;

       // Connection to Redis 
        Jedis jedis = new Jedis("pub-redis-15228.us-east-1-2.1.ec2.garantiadata.com", 15228);
        jedis.auth("redis-cse-axt3229-2-7825704");
        System.out.println("Connected to Redis");
        
        // Storing values in the database 

        System.out.println("Storing values in the Database  ");
         
        for(int i=0;i<Length;i++)
        { 
         
            int j=i+1;
        jedis.hset("DEtail:" + j , "First_name", Dataarray[i][0]);
         jedis.hset("Detail:" + j , "Last_name ", Dataarray[i][1]);
        jedis.hset("Detail:" + j , "Count", Dataarray[i][2]);

        }
      
       System.out.println("Search by First Name");
                   
           
        InputStreamReader inob=new InputStreamReader(System.in);
        BufferedReader buob=new BufferedReader(inob);
          
        String ID2=null;
                try 
            {
             ID2=buob.readLine();
            }
            catch(IOException E3)
            {
            System.out.println(E3.getMessage());
            }
                        for(int i=0;i<100;i++)
                        {
                             Map<String, String> properties3  = jedis.hgetAll("Deatails:" + i);
        for (Map.Entry<String, String> entry : properties3.entrySet())
        {
            String value=entry.getValue();
        if(entry.getValue().equals(ID2))
        {
            System.out.println(jedis.hget("Detials:" + i,"First_name"));
        }
       
        }
                        }
       
          
        
        //for (Map.Entry<String, String> entry : properties1.entrySet())
        //{
        //System.out.println(entry.getKey() + "---" + entry.getValue());
       // }
                      
                
        
                   
                       
                       // mongo db code goes here 
                       
                 case "3":
                        MongoClient mongo = new MongoClient(new MongoClientURI("mongodb://ank:123@ds055574.mongolab.com:55574/heroku_h7mxqs7l"));
                         DB db;
                         db = mongo.getDB("heroku_h7mxqs7l");
                        // storing values in the database 
                         for(int i=0;i<100;i++)
                         {
                             BasicDBObject document = new BasicDBObject();
                            document.put("_id", i+1);
                            document.put("First_name", Dataarray[i][0]);
                            document.put("Last_name", Dataarray[i][1]);    
                            document.put("County", Dataarray[i][2]);    
                            db.getCollection("DetailsRaw").insert(document);
                          
                         }
                System.out.println("Search by Name");
            
                InputStreamReader inobj=new InputStreamReader(System.in);
                BufferedReader objname=new BufferedReader(inobj);
                String Name=null;
                try
                {
                Name=objname.readLine();
                }
                catch(IOException E)
                {
                    System.out.println(E.getMessage());
                }
                BasicDBObject inQuery1 = new BasicDBObject();
                    inQuery1.put("first Name", Name);
                         DBCursor cursor1 = db.getCollection("DetailsRaw").find(inQuery1);
                            while(cursor1.hasNext()) {
                                   // System.out.println(cursor.next());
                                    System.out.println(cursor1.next());
                            }
                       
                        
       }
    }
}
        
    
