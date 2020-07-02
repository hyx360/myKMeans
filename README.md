# myKMeans
## 1.Center类
### （1）initCenter函数
- 读取给定的初始簇质心文件，返回字符串，质心向量间用tab分隔。
````java
public String initCenter(Path path) throws IOException {
	
    //创建配置对象
    Configuration conf = new Configuration();
    //初始化文件系统
    FileSystem hdfs = FileSystem.get(conf);
    //打开指定路径文件
    FSDataInputStream dis = hdfs.open(path);

    StringBuffer sb = new StringBuffer();
    LineReader in = new LineReader(dis, conf);

    String result = getCenter(in, 0);
    return result;
}
````

### （2）loadCenter函数
- 读取reduce生成的质心文件，返回字符串，质心向量间用tab分隔。
````java
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
    }
    return result;
}
````
### （3）getCenter函数
- 按行读取质心文件，质心向量间用tab分隔。
````java
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
````
## 2.Kmeans类
### （1）KmeansMapper类setup函数
- 预处理操作，读取最新的簇质心向量，并将其保存在二维数组中。
````java
//保存所有二维簇质心向量
double[][] centers = new double[Center.num][];
//保存簇质心向量
String[] centerstrArray = null;

//预处理，收集初始簇质心向量
@Override
public void setup(Context context) {

    //读取context中的簇聚类中心向量
    String centers_str = context.getConfiguration().get(centersStr);
    centerstrArray = centers_str.split("\t");

    for(int i = 0; i < centerstrArray.length; i++) {
        //保存单个二维簇质心向量
        String[] segs = centerstrArray[i].split(",");
        centers[i] = new double[segs.length];
        //给二维簇质心向量赋值
        for(int j = 0; j < segs.length; j++) {
            centers[i][j] = Double.parseDouble(segs[j]);
        }
    }
}
````
### （2）KmeansMapper类map函数
- 记录与样本向量距离最小的簇质心向量，输出<最小距离的簇质心向量下标，样本向量>。
````java
public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    //读取样本向量
    String line = value.toString();
    String[] segs = line.split(",");
    //保存样本向量
    double[] single_sample = new double[segs.length];
    //样本向量赋值
    for(int i = 0; i < segs.length; i++) {
        single_sample[i] = Float.parseFloat(segs[i]);
    }
    
    //求得距离样本向量最近的质心
    double min = Double.MAX_VALUE;
    int index = 0;
    //遍历所有的簇质心向量
    for(int i = 0; i < centers.length; i++) {
        double dis = getDistance(centers[i], single_sample);
        if(dis < min) {
            min = dis;
            index = i;
        }
    }
    //输出<簇质心向量下标，样本向量>
    context.write(new Text(centerstrArray[index]), new Text(line));
}
````
### （3）KmeansReducer类reduce函数
- 重新计算簇质心向量，若新质心向量与原质心向量距离小于设置的阈值，则记数变量counter加1，输出<新质心向量，聚类后的样本向量>，形成新的簇质心文件。
````java
Counter counter = null;	
public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

    //保存同一质心类的样本向量维度和
    double[] sum = new double[2];
    int size = 0;
    //计算对应维度上值的加和，存放在sum数组中
    Text newVaule = new Text();
    String sampleList = new String();
    sampleList += "#";
    for(Text text : values) {
        sampleList += "(" + text.toString() + ");";
        //保存单个样本向量
        String[] segs = text.toString().split(",");
        //对每个维度的向量值进行累加
        for(int i = 0; i < segs.length; i++) {
            sum[i] += Double.parseDouble(segs[i]);
        }
        size ++;
    }

    //求sum数组中每个维度向量的平均值，作为新的质心
    StringBuffer sb = new StringBuffer();
    for(int i = 0; i < sum.length; i++) {
        sum[i] /= size;
        sb.append(sum[i]);
        //if(i != (sum.length-1))
        sb.append(",");
    }

    //判断新质心向量与原质心向量是否一致
    boolean flag = false;
    String[] centerStrArray = key.toString().split(",");
    //遍历每个维度的向量值
    for(int i = 0; i < centerStrArray.length; i++) {
        if(Math.abs(Double.parseDouble(centerStrArray[i]) - sum[i]) <= 1E-5) {
            flag = true;
        }
        else{
            flag = false;
            break;
        }
    }
    //如果新的质心跟老的质心是一样的，那么相应的计数器加1
    if(flag) {
        counter = context.getCounter("myCounter", "centerCounter");
        counter.increment(1);
    }
    newVaule.set(sampleList);
    //输出<null,新质心向量>
    //context.write(null, new Text(sb.toString()));
    context.write(new Text(sb.toString()), newVaule);
}
````
### （4）KmeansMeans类main函数
- 比较num与counter值，若不相等，继续迭代，重复Mapper、Reducer步骤，否则停止迭代，并输出最终的簇质心。
````java
public static void main(String[] args) throws Exception {
    //初始的质心文件
    Path centerPath = new Path("/usr/local/hadoop/my_kmeans/center");	
    //样本文件
    Path samplePath = new Path("/usr/local/hadoop/my_kmeans/sample");	
    //加载聚类中心文件
    Center center = new Center();
    String centerString = center.initCenter(centerPath);

    //迭代的次数
    int count = 0;	
    while(count < 10) {
        //创建配置对象
        Configuration conf = new Configuration();
        //将簇聚类质心的字符串放到configuration中
        conf.set(centersStr, centerString);	
        System.out.println(centerString+"---------------"+count);

        //本次迭代的输出路径，也是新质心的读取路径
        centerPath = new Path("/usr/local/hadoop/my_kmeans/newCenter" + count);	

        //判断输出路径是否存在，如果存在，则删除
        FileSystem hdfs = FileSystem.get(conf);
        if(hdfs.exists(centerPath)) 
            hdfs.delete(centerPath);

        //创建Job对象
        Job job = new Job(conf, "kmeans" + count); 
        //设置运行Job的类
        job.setJarByClass(Kmeans.class);
        //设置Mapper类
        job.setMapperClass(KmeansMapper.class);
        //设置Reducer类
        job.setReducerClass(KmeansReducer.class);
        //设置Map输出的key vaule
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //设置Reduce输出的key value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);	
        //设置输入输出的路径
        FileInputFormat.addInputPath(job, samplePath);
        FileOutputFormat.setOutputPath(job, centerPath);
        //提交job
        boolean b = job.waitForCompletion(true);
        if(!b) {
            System.out.println("Kmeans task fail!");
            break;
        }

        //获取自定义counter的大小，若等于质心数量，停止迭代
        long counter = job.getCounters().getGroup("myCounter").findCounter("centerCounter").getValue();
        if(counter == Center.num)	
            System.exit(0);

        //重新设置簇质心向量
        center = new Center();
        centerString = center.loadCenter(centerPath);
        count ++;
    }
    System.exit(0);
}
````
## 3.文件结构
````
├── Center.java                   
├── Center.class
├── Kmeans.java
├── Kmeans.class
├── Kmeans$KmeansMapper.class
├── Kmeans$KmeansReducer.class
├── Kmeans.jar                   jar包
├── center                       初始质心文件
├── sample                       样本文件
└── README.md                        
└── newCenter                    迭代生成的质心文件
    ├── newCenter0
    │	├── ._SUCCESS.crc
    │	├── .part-r-00000.crc
    │	├── _SUCCESS
    │	└── part-r-00000
    ├── newCenter1
    │	├── ._SUCCESS.crc
    │	├── .part-r-00000.crc
    │	├── _SUCCESS
    │	└── part-r-00000
    ├── newCenter2
    │	├── ._SUCCESS.crc
    │	├── .part-r-00000.crc
    │	├── _SUCCESS
    │	└── part-r-00000
    ├── newCenter3
    │	├── ._SUCCESS.crc
    │	├── .part-r-00000.crc
    │	├── _SUCCESS
    │	└── part-r-00000 
    ├── newCenter4
    │	├── ._SUCCESS.crc
    │	├── .part-r-00000.crc
    │	├── _SUCCESS
    │	└── part-r-00000
    ├── newCenter5
    │	├── ._SUCCESS.crc
    │	├── .part-r-00000.crc
    │	├── _SUCCESS
    │	└── part-r-00000
    ├── newCenter6
    │	├── ._SUCCESS.crc
    │	├── .part-r-00000.crc
    │	├── _SUCCESS
    │	└── part-r-00000
    └── newCenter7
   	├── ._SUCCESS.crc
   	├── .part-r-00000.crc
   	├── _SUCCESS
   	└── part-r-00000
````
