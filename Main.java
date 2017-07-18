import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import com.google.common.collect.ImmutableList;


public class Main {

	private static final ImmutableList<String> WORKCLASS_LIST = ImmutableList.of("", "Private","Self-emp-not-inc","Self-emp-inc","Federal-gov","Local-gov","State-gov","Without-pay","Never-worked","?");

	private static Connection connection = null; 

	private static final String marrigeCreateString ="create table MARRIAGE("
			+ "PERSON_ID INTEGER NOT NULL, "
			+ "RELATIVE_ID INTEGER NOT NULL, "
			+ "REL_TYPE VARCHAR(20) NOT NULL, "
			+ "PRIMARY KEY(PERSON_ID, RELATIVE_ID),"
			+ "FOREIGN KEY (PERSON_ID) REFERENCES PERSONS (PERSON_ID))";

	private static final String educationCreateString = "create table EDUCATION (PERSON_ID int NOT NULL, "
			+ "UNIVERSITY_NAME VARCHAR(255) NOT NULL, "
			+ "UNIVERSITY_STATE VARCHAR(255) NOT NULL, "
			+ "DEGREE varchar(5) NOT NULL, "
			+ "GRADUATION_YEAR int NOT NULL, "
			+ "PRIMARY KEY (PERSON_ID))";

	private static final String workclassCreateString = "create table WORKCLASS (WORKCLASS_ID integer NOT NULL, WORKCLASS_NAME varchar(255), PRIMARY KEY (WORKCLASS_ID))" ;  

	private static final String personsCreateString = "create table PERSONS "
			+ "(PERSON_ID integer NOT NULL, "
			+ "PERSON_AGE integer NOT NULL, "
			+ "WORKCLASS_ID integer, "
			+ "FNLWGT integer NOT NULL, "
			+ "EDUCATION varchar(255) NOT NULL, "
			+ "EDUCATION_NUM integer NOT NULL, "
			+ "MARITAL_STATUS varchar(40) NOT NULL, "
			+ "OCCUPATION varchar(255) NOT NULL, "
			+ "RELATIONSHIP varchar(255) NOT NULL, "
			+ "RACE varchar(255) NOT NULL, "
			+ "SEX varchar(10), "
			+ "CAPITAL_GAIN INTEGER NOT NULL, " 
			+ "CAPITAL_LOSS INTEGER NOT NULL, "  
			+ "HOURS_PER_WEEK INTEGER NOT NULL, " 
			+ "NATIVE_COUNTRY varchar(255) NOT NULL, "
			+ "UNDER_OVER varchar(255) NOT NULL, "
			+ "PRIMARY KEY (PERSON_ID), "
			+ "FOREIGN KEY (WORKCLASS_ID) REFERENCES WORKCLASS (WORKCLASS_ID))";

	/** trigger for parent age greater than child and 12 */
	private static final String parentAgeTrigger = 
			"create or replace TRIGGER parent_age_constrain " 
					+" BEFORE INSERT " 
					+" ON MARRIAGE FOR EACH ROW"
					+" DECLARE "
					+" p_age INTEGER; "+" c_age INTEGER; "
					+" BEGIN " 
					+" IF :new.REL_TYPE = 'child' "
					+" THEN "
					+" SELECT PERSONS.PERSON_AGE INTO p_age FROM PERSONS WHERE :NEW.RELATIVE_ID = PERSONS.PERSON_ID; "
					+" SELECT PERSONS.PERSON_AGE INTO c_age FROM PERSONS WHERE :NEW.PERSON_ID = persons.PERSON_ID; "
					+" IF p_age < 12 "
					+" THEN RAISE_APPLICATION_ERROR (-20000, 'A parents age should be over 12'); "
					+" END IF; "
					+" IF p_age <= c_age "
					+" THEN RAISE_APPLICATION_ERROR (-20000, 'A parents age should be over his child age'); "
					+" END IF; "
					+" END IF; "
					+" END;";


	/** trigger for husband making less money than wife */ 
	private static final String hubandLessWifeTrigger	="create or replace TRIGGER huband_less_wife_trigger "
			+" BEFORE INSERT " 
			+" ON MARRIAGE FOR EACH ROW"
			+" DECLARE "
			+" h_salary VARCHAR(255); "+" w_salary VARCHAR(255);"
			+" BEGIN " 
			+" IF :new.REL_TYPE = 'wife' "
			+" THEN "
			+" SELECT PERSONS.UNDER_OVER INTO w_salary FROM PERSONS WHERE :NEW.RELATIVE_ID = PERSONS.PERSON_ID; "
			+" SELECT PERSONS.UNDER_OVER INTO h_salary FROM PERSONS WHERE :NEW.PERSON_ID = PERSONS.PERSON_ID; "
			+" IF (w_salary = '<=50K') AND (h_salary = '>50K' )"
			+" THEN RAISE_APPLICATION_ERROR (-20000,'husband makes more than wife'); "
			+ " END IF;"
			+" END IF; "
			+" IF :new.REL_TYPE = 'husband' "
			+" THEN " 
			+" SELECT persons.UNDER_OVER INTO h_salary FROM PERSONS WHERE :NEW.RELATIVE_ID = PERSONS.PERSON_ID; "
			+" SELECT persons.UNDER_OVER INTO w_salary FROM PERSONS WHERE :NEW.PERSON_ID = PERSONS.PERSON_ID; "
			+" IF (w_salary = '<=50K') AND (h_salary = '>50K' )"
			+" THEN RAISE_APPLICATION_ERROR (-20000,'husband makes more than wife'); "
			+ " END IF;"
			+ " END IF;"
			+" END;";
	;	


	public static void main(String[] argv) throws SQLException, IOException {

		System.out.println("-------- Oracle JDBC Connection Testing ------");

		try {

			Class.forName("oracle.jdbc.driver.OracleDriver");

		} catch (ClassNotFoundException e) {

			System.out.println("Where is your Oracle JDBC Driver?");
			e.printStackTrace();
			return;

		}

		System.out.println("Oracle JDBC Driver Registered!");

		try {

			connection = DriverManager.getConnection(
					"jdbc:oracle:thin:@132.72.40.216:1522:orcl", "user17",
					"pass1718");

		} catch (SQLException e) {

			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return;

		}

		if (connection != null) {
			deleteIfExists("MARRIAGE");
			deleteIfExists("PERSONS");
			deleteIfExists("WORKCLASS");
			deleteIfExists("EDUCATION");
			execute(workclassCreateString);
			execute(personsCreateString);
			execute(marrigeCreateString);
			execute(educationCreateString);
						execute(parentAgeTrigger);
						execute(hubandLessWifeTrigger);
/*
			System.out.println("START LOADING TABLES");
			loadTable("WORKCLASS", "workclass.csv");			
			loadTable("PERSONS","persons.csv");
			loadTable("MARRIAGE","marriage.csv");


			System.out.println("FINISH LOADING TABLES");
			System.out.println("ENTER QUERY");
			richestPersonAlive();
			maxChildInCountry("United-States");
			avgNumOfWomanChilds();
			System.out.println("DONE QUERY");
*/
						
			String nextLine;
			Scanner scanner = new Scanner(System.in);

			while( !(nextLine = scanner.nextLine()).toLowerCase().equals("quit")) {
				String[] command = nextLine.split(" ");
				switch (command[0].toLowerCase()) {

				case "load":

					loadTable(command[2] /* tablename */, command[1] /* filename */ );
					System.out.println("finish loading");
					break;
				case "print":
					printTable(command[1]);
					break;
				case "sql":
					String query = nextLine.substring(3);
					String nextSqlLine = scanner.nextLine();

					while(!nextSqlLine.contains("#sql")) {
						System.out.println(nextSqlLine);
						query += nextSqlLine + " ";
						nextSqlLine = scanner.nextLine();
					}
					query += nextSqlLine.replaceAll("#sql", "");
					Statement stmt = connection.createStatement();
					stmt.executeQuery(query);
					break;
				case "report":
					switch (command[1]) {
					case "1":
						richestPersonAlive();
						break;
					case "2":
						maxChildInCountry(command[2]);//@parametr country
						break;
					case "3":
						avgNumOfWomanChilds();
						break;
					}
				}
			}
			scanner.close();
			System.out.println("finished while");



		} else {
			System.out.println("Failed to make connection!");
		}
	}

	private static void avgNumOfWomanChilds() throws SQLException {
		Statement stmt = connection.createStatement();

		String avgNumOfWomanChildsInCountry="SELECT country,COUNT(country),SUM(childPerWoman)  FROM (SELECT MARRIAGE.PERSON_ID as woman_id , PERSONS.NATIVE_COUNTRY as country , "
				+ "COUNT(MARRIAGE.PERSON_ID) as childPerWoman FROM PERSONS INNER JOIN MARRIAGE ON PERSONS.PERSON_ID = MARRIAGE.PERSON_ID "
				+ "WHERE PERSONS.SEX='Female' AND MARRIAGE.REL_TYPE='child' GROUP BY MARRIAGE.PERSON_ID, PERSONS.NATIVE_COUNTRY ORDER BY MARRIAGE.PERSON_ID) GROUP BY country";



		float womanNum = 1;
		float childNum = 1;
		float result=0;
		ResultSet rs = stmt.executeQuery(avgNumOfWomanChildsInCountry);

		ResultSetMetaData rsmd = rs.getMetaData();
		int columnsNumber = rsmd.getColumnCount();
		while (rs.next()) {
			for (int i = 1; i <= columnsNumber; i++) {
				String columnValue = rs.getString(i);


				if(i==1){
						MyWriter.writeToFile("IN THE COUNTRY "+columnValue+" ");
						//System.out.print("IN THE COUNTRY "+columnValue+" ");
					}
				else if(i==2){
					womanNum=Float.valueOf(columnValue);
				}
				else if(i==3){
					childNum=Float.valueOf(columnValue);
				}           
			}
			if(childNum!=0){
				result=(womanNum/childNum);
				MyWriter.writeToFile("THE AVG CHILDREN FOR WOMAN PER COUNTRY IS  : "+result+" \n");
			}
		}
	}







	private static void maxChildInCountry(String country) throws SQLException {
		Statement stmt = connection.createStatement();
		String personsJoinMarriage = "SELECT PERSONS.PERSON_ID, count(MARRIAGE.REL_TYPE) as CHILD_NUM FROM PERSONS JOIN MARRIAGE ON PERSONS.PERSON_ID = MARRIAGE.PERSON_ID "
				+ "WHERE PERSONS.NATIVE_COUNTRY='"+country+"'AND MARRIAGE.REL_TYPE='child' group by PERSONS.PERSON_ID ORDER BY CHILD_NUM DESC";
		ResultSet queryResult = stmt.executeQuery(personsJoinMarriage);
		if(queryResult.next()) {
			MyWriter.writeToFile("Person with most children in "+ country +" ID: "+ queryResult.getString(1)+"\n");
		}

	}

	public static void richestPersonAlive() throws SQLException{

		Statement stmt = connection.createStatement();

		ResultSet queryResult = stmt.executeQuery("select * from PERSONS where ((CAPITAL_GAIN - CAPITAL_LOSS) in ( select max(CAPITAL_GAIN - CAPITAL_LOSS)  from PERSONS )) ");
		if(queryResult.next()) {
			MyWriter.writeToFile("Richest Person ID: "+ queryResult.getString(1)+"\n");
		}
	}

	public static void loadTable(String tableName, String fileName) throws SQLException {
		if(tableName.equals("PERSONS")) {
			loadPersonsTable(fileName);
			return;
		}
		Statement stmt = connection.createStatement();
		ResultSetMetaData tableMetaData = stmt.executeQuery("select * from "+ tableName).getMetaData();
		List<Integer> varcharColumns = new ArrayList<>();

		for (int i=1 ; i < tableMetaData.getColumnCount()+1 ; i++) {
			if(tableMetaData.getColumnTypeName(i).equals("VARCHAR2")) {
				varcharColumns.add(i-1);
			}
		}

		String prefix = "INSERT INTO "+tableName+" "+"VALUES (";

		try {
			BufferedReader br = new BufferedReader (new FileReader(fileName));
			String nextLine;
			stmt = connection.createStatement();

			while (( nextLine = br.readLine()) != null) {
				String[] splits = nextLine.split(",");
				for (Integer varcharColumn : varcharColumns) {
					splits[varcharColumn] = "'" + splits[varcharColumn] +"'";
				}
				String createString = prefix;
				for (String value : splits) {
					createString += value +", ";
				}
				int place = createString.length()-2; 
				createString= createString.substring(0, place) ;
				createString+= ")";
				stmt.executeUpdate(createString);
			}
			br.close();
		}
		catch(Exception e){
			e.printStackTrace();
		} 

	}


	public static void loadPersonsTable(String fileName) throws SQLException {
		Statement stmt = connection.createStatement();
		ResultSetMetaData tableMetaData = stmt.executeQuery("select * from PERSONS").getMetaData();
		List<Integer> varcharColumns = new ArrayList<>();
		int workclassIndex = 2;

		for (int i=1 ; i < tableMetaData.getColumnCount()+1 ; i++) {
			if(tableMetaData.getColumnTypeName(i).equals("VARCHAR2")) {
				varcharColumns.add(i-1);
			}
		}

		String prefix = "INSERT INTO PERSONS VALUES (";

		try {
			BufferedReader br = new BufferedReader (new FileReader(fileName));
			String nextLine;
			stmt = connection.createStatement();

			while (( nextLine = br.readLine()) != null) {
				String[] splits = nextLine.split(",");
				for (Integer varcharColumn : varcharColumns) {
					splits[varcharColumn] = "'" + splits[varcharColumn] +"'";
				}

				splits[workclassIndex] = Integer.toString(WORKCLASS_LIST.indexOf(splits[workclassIndex]));
				String createString = prefix;
				for (String value : splits) {
					createString += value +", ";
				}
				int place = createString.length()-2; 
				createString= createString.substring(0, place) ;
				createString+= ")";
				stmt.executeUpdate(createString);
			}
			br.close();
		}
		catch(Exception e){
			e.printStackTrace();
		} 

	}

	public static void printTable(String tablename) throws SQLException { 

		System.out.println("\n*********" + tablename + "*********\n");

		Statement stmt = connection.createStatement();
		ResultSet queryResult = stmt.executeQuery("select * from "+ tablename);
		ResultSetMetaData tableMetaData = queryResult.getMetaData();
		int columnCount = tableMetaData.getColumnCount();
		for (int i=1 ; i < columnCount + 1 ; i++) {
			System.out.print(tableMetaData.getColumnName(i) +"\t");
		}
		System.out.println();
		while(queryResult.next()) {
			for (int i=1 ; i < columnCount + 1 ; i++) {
				System.out.print(queryResult.getString(i) + "\t\t");
			}
			System.out.println();
		}

	}

	public static void deleteIfExists(String tableName) throws SQLException {
		DatabaseMetaData meta = connection.getMetaData();
		ResultSet res = meta.getTables(null, null, tableName, null);
		while (res.next()) {
			Statement stmt = connection.createStatement();
			stmt.executeUpdate("drop table " + res.getString("TABLE_NAME"));    
		}
	}


	public static void execute(String createString) throws SQLException {
		Statement stmt = null;
		try {
			stmt = connection.createStatement();
			stmt.executeUpdate(createString);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (stmt != null) { stmt.close(); }
		}
	}


}

