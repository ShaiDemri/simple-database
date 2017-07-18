import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class MyWriter {


	public static void writeToFile(String args) {
		BufferedWriter bw = null;
		try {
			String mycontent = args;
			//Specify the file name and path here
			File file = new File("/users/studs/bsc/2015/demri/Downloads/dbs/resume.txt");

			/* This logic will make sure that the file 
			 * gets created if it is not present at the
			 * specified location*/
			if (!file.exists()) {
				file.createNewFile();
			}

			FileWriter fw = new FileWriter(file, true);
			bw = new BufferedWriter(fw);
			bw.write(mycontent);
			System.out.println("File written Successfully");

		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		finally
		{ 
			try{
				if(bw!=null)
					bw.close();
			}catch(Exception ex){
				System.out.println("Error in closing the BufferedWriter"+ex);
			}
		}
	}
}