package org.apache.derby.debugging;
import java.io.File;

public class DebuggingDerby{
	public static int count = 0;
	public static int lay=0;
  public static void getEnv(){
	File file =new File("/home/rSwitch.lock");
        if (file.exists()) {
			lay=1;
		}
}
}
