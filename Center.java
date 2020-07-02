import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.fs.FileStatus;
public class Center {
	//质心的个数
	protected static int num = 3;		
	
	/**
	 * 从初始的质心文件中加载质心，并返回字符串，质心之间用tab分割
	 * @param path
	 * @return
	 * @throws IOException
	 */
	//读取给定的初始簇之心文件
	public String loadInitCenter(Path path) throws IOException {
	
		//创建配置对象
		Configuration conf = new Configuration();
		//初始化文件系统
		FileSystem hdfs = FileSystem.get(conf);
		//打开指定路径文件
		FSDataInputStream dis = hdfs.open(path);
		
		StringBuffer sb = new StringBuffer();
		LineReader in = new LineReader(dis, conf);
		
		String result = getCenter(in, 0);
		
		/*Text line = new Text();
		//按行读取质心，质心之间用tab分割
		while(in.readLine(line) > 0) {
			sb.append(line.toString().trim());
			sb.append("\t");
		}*/
		return result;
		//return sb.toString().trim();
	}
	
	/**
	 * 从每次迭代的质心文件中读取质心，并返回字符串
	 * @param path
	 * @return
	 * @throws IOException
	 */
	//读取reduce生成的质心文件
	public String loadCenter(Path path) throws IOException {
		
		//创建配置对象
		Configuration conf = new Configuration();
		//初始化文件系统
		FileSystem hdfs = FileSystem.get(conf);
		//获取文件列表，保存part-r-00000及_SUCCESS文件
		FileStatus[] files = hdfs.listStatus(path);
		
		StringBuffer sb = new StringBuffer();
		String result = new String();
		for(int i = 0; i < files.length; i++) {
			//得到文件路径
			Path filePath = files[i].getPath();
			if(!filePath.getName().contains("part")) 
				continue;
			FSDataInputStream dis = hdfs.open(filePath);
			
			LineReader in = new LineReader(dis, conf);
			
			result = getCenter(in, 1);
			/*Text line = new Text();
			while(in.readLine(line) > 0) {
				sb.append(line.toString().trim());
				sb.append("\t");
			}*/
		}
		return result;
		//return sb.toString().trim();
	}
	//按行读取质心，质心之间用tab分割
	public String getCenter(LineReader in, int flag) throws IOException {
		
		StringBuffer sb = new StringBuffer();
		Text line = new Text();
		//按行读取质心
		while(in.readLine(line) > 0) {
				if(flag==0)
					sb.append(line.toString().trim());
				else{
					String str = line.toString();
					int index = str.indexOf("#");
					str = str.substring(0, index).trim();
					sb.append(str);
				}
				sb.append("\t");
			}
		return sb.toString().trim();
	}
}
