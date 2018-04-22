
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

//change backtype to org.apache
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class FileReaderSpout implements IRichSpout {
  private SpoutOutputCollector _collector;
  private TopologyContext context;

  BufferedReader br = null;
  FileReader fr = null;


  @Override
  public void open(Map conf, TopologyContext context,
                   SpoutOutputCollector collector) {

     /*
    ----------------------TODO-----------------------
    Task: initialize the file reader


    ------------------------------------------------- */
    try {
	fr = new FileReader(conf.get("fileIn").toString());
    } catch (FileNotFoundException e) {
	throw new RuntimeException("Missing file " + conf.get("fileIn"));
    }

    this.context = context;
    this._collector = collector;
  }

  @Override
  public void nextTuple() {

     /*
    ----------------------TODO-----------------------
    Task:
    1. read the next line and emit a tuple for it
    2. don't forget to sleep when the file is entirely read to prevent a busy-loop

    ------------------------------------------------- */

    if (done) {
	try {
	  Thread.sleep(1000);
	} catch (InterruptedException e) {

	}
    }
	
    String lineIn;
    br = new BufferedReader(fr);
    try {
	while ((lineIn = br.readLine()) != null) {
		this._collector.emit(new Values(lineIn), lineIn);
	}
    } catch (Exception e) {
	throw new RuntimeException("Error reading tuple", e);
    } finally {
	done = true;
    }
  }

  private boolean done = false;

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

    declarer.declare(new Fields("word"));

  }

  @Override
  public void close() {
   /*
    ----------------------TODO-----------------------
    Task: close the file


    ------------------------------------------------- */
    try {
			fr.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
  }


  @Override
  public void activate() {
  }

  @Override
  public void deactivate() {
  }

  @Override
  public void ack(Object msgId) {
  }

  @Override
  public void fail(Object msgId) {
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
