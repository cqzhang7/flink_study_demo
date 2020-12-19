import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/** @author cqzhang
 * @create 2020-12-13 0:10 
 */
object flatmappp {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val socketStream: DataStreamSource[String] = environment.socketTextStream("hadoop102",9999)
    socketStream.flatMap(new FlatMapFunction[String,(String,Int)] {
      override def flatMap(value: String, out: Collector[(String, Int)]): Unit = {
        val strings: Array[String] = value.split(" ")
        for (elem <- strings) {
          out.collect((elem,1))
        }
      }
    })
  }

}
