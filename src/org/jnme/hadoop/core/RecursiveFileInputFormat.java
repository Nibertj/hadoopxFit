/**
 * 
 */
package org.jnme.hadoop.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.InvalidInputException;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * author: Joseph Nibert 
 * author: Matt Elliott
 *
 * This recursive file input format will take in an array of regular expressions and
 * find all files on a hadoop distributed file system that meet the criteria of the regular expression - for 
 * the given path that needs to be searched.  This will recursively drill through the
 * given path and all sub folders underneath and return all the files
 * that meet the requirement.  Examples of use would be to find all files
 * with the .txt and .doc extensions.  This was built off of patches and findings
 * on the internet to extend the capabilities of hdfs and file browsing.
 */
public abstract class RecursiveFileInputFormat<K, V> extends FileInputFormat<K, V> {

    public static final String READ_INPUT_FILES_RECURSIVELY = "mapreduce.input.fileinputformat.readinputfilesrecursively";
    public static final String FILE_FILTERS = "mapreduce.input.fileinputformat.filefilters";
    private static final Log LOG = LogFactory.getLog(RecursiveFileInputFormat.class);
    
    protected static final double SPLIT_SLOP = 1.1;   // 10% slop
   
    /**
     * Proxy PathFilter that accepts a path only if any of filters given in the
     * constructor do. Used by the listPaths() to apply the built-in
     * hiddenFileFilter together with a user provided one (if any).
     */
    public static class MultiPathFilter implements PathFilter {

        private List<PathFilter> filters;

        public MultiPathFilter(List<PathFilter> filters) {
            this.filters = filters;
        }

        public boolean accept(Path path) {
            for (PathFilter filter : filters) {
                if(filter.accept(path)){
                	return true;
                }
            }
            return false;
        }
    }

    /**
     * This method determines if the search will be done
     * recursively.  If it is recursive then set the value
     * to true.  Otherwise, it will only read the first path.
     *
     * @param job
     *          the job to modify
     * @param readFilesRecursively
     */
    public static void setReadFilesRecursively(Job job,
            boolean readFilesRecursively) {
        job.getConfiguration().setBoolean(READ_INPUT_FILES_RECURSIVELY,
                readFilesRecursively);
    }

    /**
     * This method returns the recursively set value.
     *
     * @param job
     *          the job to look at.
     * @return should the files to be read recursively?
     */
    public static boolean getReadFilesRecursively(Job job) {
        return job.getConfiguration().getBoolean(READ_INPUT_FILES_RECURSIVELY,
                false);
    }

    /**
     * This method adds files in the input path recursively into the results
     * based on the path filter.  it searches the directory itself and
     * if the path is a directory, it recursively calls to continue to
     * check the sub directories for files that meet the regular expression filter.
     *
     * @param result
     *          The List to store all files.
     * @param fs
     *          The FileSystem.
     * @param path
     *          The input path.
     * @param inputFilter
     *          The input filter that can be used to filter files/dirs.
     * @throws IOException
     */
    
    public void addInputPathRecursively(List<FileStatus> result, FileSystem fs, Path path, PathFilter inputFilter, boolean isRecursive) 
  	      throws IOException {
    	
    	/*  This loop is to determine if the path is a directory
    	 * 
    	 */
  	    for(FileStatus stat: fs.listStatus( path )) {
  	      if (stat.isDir() && isRecursive) {
  	        addInputPathRecursively(result, fs, stat.getPath(), inputFilter, isRecursive);
  	      } 
  	    }    
  	    
  	    /*
  	     * This loop will loop through all the files in the directory
  	     * and any that meet any of the regular expressions will be
  	     * added to the file list.
  	     */
  	    for(FileStatus fileStat : fs.listStatus(path, inputFilter)){
  	    	//if it is a file then add.  If a directory leave alone-
  	    	//since this is looking for files.  We could modify to
  	    	//to handle looking for directories as well.
  	    	if( !fileStat.isDir() ){
  	    		result.add(fileStat);
  	    	}
  	    }
  	  }

    /** List input directories.
     * Subclasses may override to, e.g., select only files matching a regular
     * expression.
     *
     * @param job the job to list input paths for
     * @return array of FileStatus objects
     * @throws IOException if zero items.
     */
    @Override
    protected List<FileStatus> listStatus(JobContext job) throws IOException {
        
        Path[] dirs = getInputPaths(job);
        if (dirs.length == 0) {
            throw new IOException("No input paths specified in job.");
        }

        // get tokens for all the required FileSystems..
        TokenCache.obtainTokensForNamenodes(job.getCredentials(), dirs,
                job.getConfiguration());

        boolean recursive = job.getConfiguration().getBoolean(READ_INPUT_FILES_RECURSIVELY, false);
       
        List<FileStatus> result = new ArrayList<FileStatus>();
        List<IOException> errors = new ArrayList<IOException>();
       
        // creates a MultiPathFilter with the
        // user provided one (if any).
        List<PathFilter> filters = new ArrayList<PathFilter>();
        PathFilter jobFilter = getInputPathFilter(job);
       
        String[] regexpressions = getFileFilters(job);
        
        for( int i = 0; i < regexpressions.length; i++){
        	PathFilter regexFilter = ReflectionUtils.newInstance(jobFilter.getClass(), job.getConfiguration());
        	//If a regular expression filter then set the pattern.  Otherwise,
        	//just add the filter to the list.
        	if ( regexFilter instanceof RegexIncludePathFilter){
        		((RegexIncludePathFilter) regexFilter).setPattern(regexpressions[i]);
        	}
        	filters.add(regexFilter);
        }
        
        PathFilter inputFilter = new MultiPathFilter(filters);

        for (int i = 0; i < dirs.length; ++i) {
        	Path p = dirs[i];
  	      FileSystem fs = p.getFileSystem(job.getConfiguration());
  	      addInputPathRecursively(result, fs, p, inputFilter, recursive );
        }

        if (!errors.isEmpty()) {
            throw new InvalidInputException(errors);
        }
        LOG.info("Total input paths to process : " + result.size());
        return result;
    }
    
    /*
     * This method takes in path filter(s)/regexpression to find files
     * that meet the regular expressions,  this method is or'ed....basically
     * permitting one to search and return all files that meet any of the
     * regular expression.  If a user wants to find all .log and all .txt files
     * in hdfs this will provide the avenue to find different files.
     */
    public static void setFileFilters(JobContext job, String[] regexpressions){
    	job.getConfiguration().setStrings(FILE_FILTERS, regexpressions);
    }
    
    protected static String[] getFileFilters(JobContext job){
    	
    	return job.getConfiguration().getStrings(FILE_FILTERS);
 
    }
    
}
