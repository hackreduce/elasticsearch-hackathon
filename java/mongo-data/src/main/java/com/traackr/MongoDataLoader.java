package com.traackr;

import java.io.File;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import com.mongodb.*;
import com.mongodb.util.JSON;
import org.apache.commons.cli.*;

/**
 * @author gstathis
 *         Created on: 3/15/13
 */
public class MongoDataLoader {
  
  private static String        _mongoHost         = "localhost:27017";
  private static String        _mongoUser         = null;
  private static String        _mongoPass         = null;
  private static String        _mongoDb           = null;
  private static String        _mongoCollection   = null;
  private static String        _fields            = null;
  private static String        _query             = null;
  private static Integer       _cutoffCount       = null;
  private static Long          _reportInterval    = 1 * 1000l;
  private static DecimalFormat _decimalFormat     = new DecimalFormat("###,###");
  
  private static Options buildOptions() {
    Options options = new Options();
    options.addOption(OptionBuilder.withLongOpt("help").withDescription("print this message").create("h"));
    options.addOption(OptionBuilder.withLongOpt("database")
                                   .withDescription("mongo database")
                                   .hasArg()
                                   .withArgName("db name")
                                   .isRequired()
                                   .create("d"));
    options.addOption(OptionBuilder.withLongOpt("collection")
                                   .withDescription("mongo collection")
                                   .hasArg()
                                   .withArgName("collection name")
                                   .isRequired()
                                   .create("c"));
    options.addOption(OptionBuilder.withLongOpt("mongoHost")
                                   .withDescription("mongo host (defaults to localhost:27017)")
                                   .hasArg()
                                   .withArgName("hostname:port")
                                   .create("m"));
    options.addOption(OptionBuilder.withLongOpt("mongoUser")
                                   .withDescription("mongo username")
                                   .hasArg()
                                   .withArgName("username")
                                   .create("mu"));
    options.addOption(OptionBuilder.withLongOpt("mongoPass")
                                   .withDescription("mongo password")
                                   .hasArg()
                                   .withArgName("password")
                                   .create("mp"));
    options.addOption(OptionBuilder.withLongOpt("fields")
                                   .withDescription("CSV of fields to include, ! will exclude")
                                   .hasArg()
                                   .withArgName("fields")
                                   .create("f"));
    options.addOption(OptionBuilder.withLongOpt("query")
                                   .withDescription("JSON query to optionally limit the records")
                                   .hasArg()
                                   .withArgName("json")
                                   .create("q"));
    options.addOption(OptionBuilder.withLongOpt("cutoff")
                                   .withDescription("stop processing after this number of records")
                                   .hasArg()
                                   .withArgName("count")
                                   .create("o"));
    return options;
  }
  
  private static void printHelp(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("MongoDataLoader", options, true);
  }
  
  private static void setParameters(CommandLine params) {
    System.out.println("Program parameters:");
    for (Option o : params.getOptions())
      System.out.println("- " + o.getLongOpt() + ": " + o.getValue());
    
    if (params.hasOption("m"))
      _mongoHost = params.getOptionValue("m");
    if (params.hasOption("mu"))
      _mongoUser = params.getOptionValue("mu");
    if (params.hasOption("mp"))
      _mongoPass = params.getOptionValue("mp");
    if (params.hasOption("d"))
      _mongoDb = params.getOptionValue("d");
    if (params.hasOption("c"))
      _mongoCollection = params.getOptionValue("c");
    if (params.hasOption("f"))
      _fields = params.getOptionValue("f");
    if (params.hasOption("q"))
      _query = params.getOptionValue("q");
    if (params.hasOption("o"))
      _cutoffCount = Integer.parseInt(params.getOptionValue("o"));
  }
  
  private static DBCollection getCollection() throws Exception {
    String mongoURIString = _mongoHost;
    if (!_mongoHost.contains("mongodb://"))
      mongoURIString = "mongodb://".concat(_mongoHost);
    MongoURI mongoURI = new MongoURI(mongoURIString);
    Mongo mongoServer = new Mongo(mongoURI);
    DB db = null;
    if (null != _mongoDb)
      db = mongoServer.getDB(_mongoDb);
    else
      db = mongoServer.getDB("traackr");
    // If username present, try to authenticate
    if (!db.isAuthenticated() && _mongoUser != null) {
      db.authenticate(_mongoUser, _mongoPass.toCharArray());
    }
    DBCollection coll = db.getCollection(_mongoCollection);
    return coll;
  }

  private static BasicDBObject getFields() {
    BasicDBObject fields = new BasicDBObject();
    if (null == _fields)
      return fields;
    String[] fieldNames = _fields.split(",");
    for (String fieldName : fieldNames) {
      if (fieldName.startsWith("!"))
        fields.append(fieldName.substring(1), false);
      else
        fields.append(fieldName, true);
    }
    return fields;
  }

  private static void cleanExit(DBCursor cursor) {
    System.out.println("Closing mongo connection");
    Mongo m = cursor.getCollection().getDB().getMongo();
    cursor.close();
    m.close();
    System.out.println("Exiting");
    System.exit(0);
  }

  private static void report(Long ops, Long duration) {
    Double durationInSecs = (duration) / 1000.0;
    Double rate = Double.parseDouble(ops.toString()) / durationInSecs;
    System.out.println(String.format("Processed %s records in %s seconds. Average rate: %s ops/sec",
        ops,
        durationInSecs,
        _decimalFormat.format(rate)));
  }

  public static void main(String args[]) throws Exception {
    Options options = buildOptions();
    CommandLineParser parser = new GnuParser();
    try {
      CommandLine line = parser.parse(options, args);
      if (line.hasOption("h")) {
        printHelp(options);
        System.exit(0);
      }
      setParameters(line);
      System.out.println("Starting MongoDataLoader...");
      Long startTime = System.currentTimeMillis();

      DBCollection collection = getCollection();
      BasicDBObject fields = getFields();
      BasicDBObject query = (null != _query) ? (BasicDBObject) JSON.parse(_query) : new BasicDBObject();
      DBCursor cursor = collection.find(query, fields).hint(new BasicDBObject("$natural", 1));
      long totalCount = 0;
      Long lastReport = System.currentTimeMillis();
      while (cursor.hasNext()) {
        File exitFile = new File("stop.txt");
        if (exitFile.exists()) {
          System.out.println("Detected stop.txt");
          exitFile.delete();
          cleanExit(cursor);
        }

        BasicDBObject record = (BasicDBObject) cursor.next();
        System.out.println(record);
        totalCount++;

        if(null!=_cutoffCount && totalCount >= _cutoffCount) {
          System.out.println(String.format("Reached configured cutoff of %s records", _cutoffCount));
          cleanExit(cursor);
        }

        if (System.currentTimeMillis() - lastReport > _reportInterval) {
          report(totalCount, System.currentTimeMillis() - startTime);
          lastReport = System.currentTimeMillis();
        }
      }

      System.out.println("Exiting MongoDataLoader");
      cleanExit(cursor);
    }
    catch (ParseException e) {
      System.out.println(e.getMessage());
      printHelp(options);
      System.exit(1);
    }
    catch (Exception e) {
      e.printStackTrace();
      System.err.println(e.getMessage());
      System.exit(1);
    }

  }
  
}
