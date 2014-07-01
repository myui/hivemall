/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013-2014
 *   National Institute of Advanced Industrial Science and Technology (AIST)
 *   Registration Number: H25PRO-1520
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
package hivemall.utils.hadoop;

import hivemall.common.WeightValue;
import hivemall.utils.collections.OpenHashMap;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.MapredContextAccessor;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hcatalog.data.transfer.HCatReader;
import org.apache.hcatalog.data.transfer.ReadEntity;
import org.apache.hcatalog.data.transfer.ReaderContext;

public final class HCatalogUtils {

    public static void loadWeightsFromHDFS(OpenHashMap<Object, WeightValue> weights, String tableName)
            throws IOException, HiveException {
        ReadEntity.Builder builder = new ReadEntity.Builder();

        final ReadEntity entity;
        String[] tableNames = tableName.split("\\.");
        if(tableNames.length == 1) {
            entity = builder.withTable(tableName).build();
        } else if(tableNames.length == 2) {
            entity = builder.withDatabase(tableNames[0]).withTable(tableNames[1]).build();
        } else {
            throw new HiveException("Invalid tableName: " + tableName);
        }

        MapredContext context = MapredContextAccessor.get();
        JobConf jobconf = context.getJobConf();
        Map<String, String> config = HadoopUtils.copy(jobconf);

        HCatReader reader = DataTransferFactory.getHCatReader(entity, config);
        ReaderContext readCntxt = reader.prepareRead();

        List<InputSplit> splits = readCntxt.getSplits();
        //Collections.shuffle(splits); // for load balancing among workers
        for(InputSplit split : splits) {
            HCatReader hcatReader = DataTransferFactory.getHCatReader(split, readCntxt.getConf());
            Iterator<HCatRecord> itr = hcatReader.read();
            while(itr.hasNext()) {
                HCatRecord record = itr.next();
                Object feature = record.get(0); // feature                
                Float weight = (Float) record.get(1); // weights
                weights.put(feature, new WeightValue(weight));
            }
        }
    }

}
