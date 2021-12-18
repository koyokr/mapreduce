package kr.koyo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONArray;
import org.json.JSONObject;

public class ReactionMapper extends Mapper<Object, Text, Text, IntWritable> {
    private static final Text outputKey = new Text();
    private static final IntWritable outputValue = new IntWritable(1);

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        JSONObject json = new JSONObject(value.toString());
        String date = json.getString("yyyymm");
        JSONArray reactions = json.getJSONArray("contents").getJSONObject(0).getJSONArray("reactions");

        for (int i = 0; i < reactions.length(); i++) {
            JSONObject reaction = reactions.getJSONObject(i);
            String reactionType = reaction.getString("reactionType");
            int reactionCount = reaction.getInt("count");
            outputKey.set(date + "," + reactionType);
            outputValue.set(reactionCount);
            context.write(outputKey, outputValue);
        }
    }
}
