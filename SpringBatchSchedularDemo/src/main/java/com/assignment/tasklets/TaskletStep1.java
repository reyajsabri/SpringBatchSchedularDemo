package com.assignment.tasklets;

import java.io.File;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

//@Component
public class TaskletStep1 implements Tasklet{

	@Value("${folderpath}")
	private String FOLDER_PATH;
	
	@Override
	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		System.out.println("### Step 1 - Processing!");
		try{
			// delete folder recursively
	        recursiveEmptyFolder(new File(FOLDER_PATH));
    	}catch(Exception e){
    		e.printStackTrace();
    	}
		return null;
	}
	
	public void recursiveEmptyFolder(File file) {
        // stop condition
        if (!file.exists())
            return;
 
        // if non-empty folder, call recursively
        if (file.isDirectory()) {
            for (File f : file.listFiles()) {
                // recursive function
                recursiveEmptyFolder(f);
            }
        }
        // call delete() function for file and empty directory
        if(!file.getAbsolutePath().equals(FOLDER_PATH)){
        	file.delete();
        }
        System.out.println("Deleted folder(file): " + file.getAbsolutePath());
    }
}