/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.emory.cci.aiw.i2b2etl.table;

/**
 *
 * @author Andrew Post
 */
public class InvalidFactException extends Exception {

    InvalidFactException(Throwable thrwbl) {
        super(thrwbl);
    }

    InvalidFactException(String string, Throwable thrwbl) {
        super(string, thrwbl);
    }

    InvalidFactException(String string) {
        super(string);
    }

    InvalidFactException() {
    }
    
}
