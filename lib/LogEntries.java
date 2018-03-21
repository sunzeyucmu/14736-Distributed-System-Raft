package lib;

import java.io.Serializable;

/**
 * Definition of a log object in RaftNode Server
 * Information about each log entry
 */
public class LogEntries implements Serializable{
    private static final long serialVersionUID = 1L;

    public int command; // Command for State Machine
    public int index; // Integer index identifying its position in the log
    public int term; // Time is divided into terms, each term begins with an election

    /* Constructor */
    public LogEntries(int command, int index, int term){
        this.command = command;
        this.index = index;
        this.term = term;
    }

    /* Print the Entry */
    public void print(){
        System.out.println("Index: "+this.index+" Term: "+this.term + " Command "+this.command);
    }
}
