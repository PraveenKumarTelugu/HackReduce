package org.hackreduce.models;

import java.text.ParseException;

import org.apache.hadoop.io.Text;


/**
 * Parses a raw record (line of string data) from the NASDAQ/NYSE CSV data dump into a Java object.
 *
 * Data dump can be found at:
 * http://www.infochimps.com/datasets/daily-1970-2010-open-close-hi-low-and-volume-nasdaq-exchange
 * http://www.infochimps.com/datasets/daily-1970-2010-open-close-hi-low-and-volume-nyse-exchange
 *
 */
public class FollowRecord {


	String userId;
	String followerId;

	public FollowRecord(String inputString) throws IllegalArgumentException {
		String[] attributes = inputString.split("\t");
		if (attributes.length != 2)
			throw new IllegalArgumentException("Input string given did not have 2 values in CSV format");

		try {
			setUserId(attributes[0]);
			setFollowerId(attributes[1]);
		} catch (ParseException e) {
			throw new IllegalArgumentException("Input string contained an unknown value that couldn't be parsed");
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Input string contained an unknown number value that couldn't be parsed");
		}	
	}
	
	public FollowRecord(Text inputText) throws IllegalArgumentException {
		this(inputText.toString());
	}
	
	public String getUserId() {
		return userId;
	}
	
	public void setUserId(String userId) {
		this.userId = userId;
	}
	
	public String getfollowerId() {
		return followerId;
	}
	
	public void setFollowerId(String userId) {
		this.followerId = followerId;
	}
}