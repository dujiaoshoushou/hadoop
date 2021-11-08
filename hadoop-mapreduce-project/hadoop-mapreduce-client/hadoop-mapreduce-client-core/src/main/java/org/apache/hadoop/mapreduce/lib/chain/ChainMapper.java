/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapreduce.lib.chain;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.chain.Chain.ChainBlockingQueue;

/**
 * The ChainMapper class allows to use multiple Mapper classes within a single
 * Map task.
 * 
 * <p>
 * The Mapper classes are invoked in a chained (or piped) fashion, the output of
 * the first becomes the input of the second, and so on until the last Mapper,
 * the output of the last Mapper will be written to the task's output.
 * </p>
 * <p>
 * The key functionality of this feature is that the Mappers in the chain do not
 * need to be aware that they are executed in a chain. This enables having
 * reusable specialized Mappers that can be combined to perform composite
 * operations within a single task.
 * </p>
 * <p>
 * Special care has to be taken when creating chains that the key/values output
 * by a Mapper are valid for the following Mapper in the chain. It is assumed
 * all Mappers and the Reduce in the chain use matching output and input key and
 * value classes as no conversion is done by the chaining code.
 * </p>
 * <p>
 * Using the ChainMapper and the ChainReducer classes is possible to compose
 * Map/Reduce jobs that look like <code>[MAP+ / REDUCE MAP*]</code>. And
 * immediate benefit of this pattern is a dramatic reduction in disk IO.
 * </p>
 * <p>
 * IMPORTANT: There is no need to specify the output key/value classes for the
 * ChainMapper, this is done by the addMapper for the last mapper in the chain.
 * </p>
 * ChainMapper usage pattern:
 * <p>
 * 
 * <pre>
 * ...
 * Job = new Job(conf);
 *
 * Configuration mapAConf = new Configuration(false);
 * ...
 * ChainMapper.addMapper(job, AMap.class, LongWritable.class, Text.class,
 *   Text.class, Text.class, true, mapAConf);
 *
 * Configuration mapBConf = new Configuration(false);
 * ...
 * ChainMapper.addMapper(job, BMap.class, Text.class, Text.class,
 *   LongWritable.class, Text.class, false, mapBConf);
 *
 * ...
 *
 * job.waitForComplettion(true);
 * ...
 * </pre>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ChainMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends
    Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

  /**
   * Adds a {@link Mapper} class to the chain mapper.
   * 
   * <p>
   * The key and values are passed from one element of the chain to the next, by
   * value. For the added Mapper the configuration given for it,
   * <code>mapperConf</code>, have precedence over the job's Configuration. This
   * precedence is in effect when the task is running.
   * </p>
   * <p>
   * IMPORTANT: There is no need to specify the output key/value classes for the
   * ChainMapper, this is done by the addMapper for the last mapper in the chain
   * </p>
   * 
   * @param job
   *          The job.
   * @param klass
   *          the Mapper class to add.
   * @param inputKeyClass
   *          mapper input key class.
   * @param inputValueClass
   *          mapper input value class.
   * @param outputKeyClass
   *          mapper output key class.
   * @param outputValueClass
   *          mapper output value class.
   * @param mapperConf
   *          a configuration for the Mapper class. It is recommended to use a
   *          Configuration without default values using the
   *          <code>Configuration(boolean loadDefaults)</code> constructor with
   *          FALSE.
   */
  public static void addMapper(Job job, Class<? extends Mapper> klass,
      Class<?> inputKeyClass, Class<?> inputValueClass,
      Class<?> outputKeyClass, Class<?> outputValueClass,
      Configuration mapperConf) throws IOException {
    job.setMapperClass(ChainMapper.class); // 就作业而言，所用的Mapper是ChainMapper
    job.setMapOutputKeyClass(outputKeyClass); // 作为整个Mapper的K输出类型
    job.setMapOutputValueClass(outputValueClass); // 作为整个Mapper的V输出类型
    Chain.addMapper(true, job, klass, inputKeyClass, inputValueClass,
        outputKeyClass, outputValueClass, mapperConf);
  }

  private Chain chain;

  protected void setup(Context context) {
    chain = new Chain(true); // 创建Chain对象
    chain.setup(context.getConfiguration()); // 根据JobConf中的内容依次创建链中的各个Mapper对象
  }

  public void run(Context context) throws IOException, InterruptedException {

    setup(context);

    int numMappers = chain.getAllMappers().size(); // 链中共有几个Mapper
    if (numMappers == 0) {
      return;
    }

    ChainBlockingQueue<Chain.KeyValuePair<?, ?>> inputqueue;
    ChainBlockingQueue<Chain.KeyValuePair<?, ?>> outputqueue;
    if (numMappers == 1) {
      chain.runMapper(context, 0); // 只有一个实际的mapper，那么就运行这个Mapper
    } else { // 是多个mapper的链
      // add all the mappers with proper context
      // add first mapper
      outputqueue = chain.createBlockingQueue(); // 创建一个Mapper的输出队列
      chain.addMapper(context, outputqueue, 0); // 将第一个mapper加入mapper链
      // add other mappers
      for (int i = 1; i < numMappers - 1; i++) {
        inputqueue = outputqueue; // 前一个mapper的输出队列就是后一个mapper的输入队列
        outputqueue = chain.createBlockingQueue(); // 再创建后一个Mapper的输出队列
        chain.addMapper(inputqueue, outputqueue, context, i); // 将后一个mapper加入mapper链
      }
      // add last mapper 将最后一个Mapper加入Mapper链，这个mapper无须输出队列
      chain.addMapper(outputqueue, context, numMappers - 1);
    }
    
    // start all threads
    chain.startAllThreads();
    
    // wait for all threads
    chain.joinAllThreads();
  }
}
