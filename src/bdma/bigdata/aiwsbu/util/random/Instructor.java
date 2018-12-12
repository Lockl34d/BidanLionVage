package bdma.bigdata.aiwsbu.util.random;

import org.apache.commons.lang.CharSet;

public class Instructor {

    private String rowKey = "";

    public Instructor(int year) {
        rowKey = String.format("%s/%4d", generateName(), year);
    }

    public String getRowKey() {
        return rowKey;
    }

    // TODO
    public String generateName() {
    	
    	CharSet.ASCII_ALPHA.
    	
        return "";
    }

    // TODO
    public String[] generateCourses() {
        return new String[1];
    }
}
