/**
 * 
 */
package org.jnme.hadoop.core;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * @author nibertj
 *
 */
public class RegexIncludePathFilter implements PathFilter {

     private Pattern pattern;
     private static final Log log = LogFactory.getLog(RegexIncludePathFilter.class);

     /**
      * The default pattern match is a .txt file if no regexpression is supplied.
      */
     public RegexIncludePathFilter() {
    	 setPattern("(.txt)$");
     }

     public RegexIncludePathFilter(String regex) {
     	setPattern(regex);
     }
     
     public void setPattern(String regex){
    	 pattern = Pattern.compile(regex);
     }
     
     public boolean accept(Path path) {
    	 log.info("the path is " + path.getName() + " the pattern  is " +pattern.pattern() );
    	 pattern.pattern();
         Matcher matcher = pattern.matcher(path.getName());
         boolean result = matcher.find();
         
         log.info("Accept is "+result);
         return result;
     }
 
}
