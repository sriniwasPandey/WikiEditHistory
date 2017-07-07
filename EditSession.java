//package wiki.hadoopcomposite;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class EditSession extends Mapper<LongWritable, Text, CompositeKey, Text> {

	private static final Log LOG = LogFactory.getLog(EditSession.class);

	// Fprivate Text videoName = new Text();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		try {

			InputStream is = new ByteArrayInputStream(value.toString()
					.getBytes());
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory
					.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(is);
			CompositeKey localkey = new CompositeKey();
			doc.getDocumentElement().normalize();

			NodeList nList = doc.getElementsByTagName("revision");

			for (int temp = 0; temp < nList.getLength(); temp++) {

				Node nNode = nList.item(temp);

				if (nNode.getNodeType() == Node.ELEMENT_NODE) {

					Element eElement = (Element) nNode;

					Element uname = ((Element) eElement.getElementsByTagName(
							"contributor").item(0));
					Element time = (Element) eElement.getElementsByTagName(
							"timestamp").item(0);
					// String id = ((Element)
					// eElement.getElementsByTagName("contributor").item(0)).getTextContent();
					// String id =
					// eElement.getElementsByTagName("name").item(0).getTextContent();
					// String gender =
					// eElement.getElementsByTagName("gender").item(0).getTextContent();

					// System.out.println(id + "," + name + "," + gender);
					String username = uname.getElementsByTagName("username")
							.item(0).getTextContent();
					String id = uname.getElementsByTagName("id").item(0)
							.getTextContent();
					// System.out.println(username+" :uname");
					if (!username.toLowerCase().contains("bot")) {
						localkey.setUser(new Text(username));
						localkey.settStamp(new Text(time.getTextContent()));
						context.write(localkey, new Text(time.getTextContent()));
					}
				}
			}
		} catch (Exception e) {
			// LogWriter.getInstance().WriteLog(e.getMessage());
		}

	}

}
