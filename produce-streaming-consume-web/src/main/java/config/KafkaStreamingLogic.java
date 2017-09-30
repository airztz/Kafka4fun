package config;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueMapper;

public class KafkaStreamingLogic {
	public static KStreamBuilder TputByMarket_LogicBuilder(String intputTopic, String outputTopic) {
		final FractionSerializer FractionSerializer = new FractionSerializer();
		final FractionDeserializer FractionDeserializer = new FractionDeserializer();
		final Serde<Fraction> FractionSerde = Serdes.serdeFrom(FractionSerializer, FractionDeserializer);
		KStreamBuilder builder = new KStreamBuilder();
		KStream<String, String> inputRecord = builder.stream(intputTopic);
		KTable<String, String> marketUserTput = inputRecord
				.filter((recordKey,
						recordValue) -> (recordValue.length() > 0 && !recordValue.equals("") && recordValue != null))
				.map(new KeyValueMapper<String, String, KeyValue<String, String>>() {
					@Override
					public KeyValue<String, String> apply(String recordKey, String recordValue) {
						int comma = 0, walker = 0, runner = -1;
						String region = null, market = null;
						while (++runner < recordValue.length()) {
							if (recordValue.charAt(runner) != ',')
								continue;
							comma++;
							if (comma == 5)
								walker = runner + 1;
							else if (comma == 6) {
								region = recordValue.substring(walker, runner);
								walker = runner + 1;
							} else if (comma == 7)
								market = recordValue.substring(walker, runner);
							else if (comma == 12)
								break;
						}
						String newKey = "Date:" + recordKey + "_Region:" + region + "_Market:" + market;
						String newRecord = recordValue.substring(runner + 1, recordValue.length());
						return new KeyValue<String, String>(newKey, newRecord);
					}
				}).groupByKey()
				// .groupBy(new KeyValueMapper<String, String, String>() {//new
				// KeyValueMapper<OldKeyType, OldValueType, NewKeyType>
				// @Override
				// //return NewKey
				// public String apply(String recordKey, String recordValue) {
				// String[] record = recordValue.split(",");
				// String region = record[5];
				// String market = record[6];
				// String newKey =
				// "Date:"+recordKey+"_Region:"+region+"_Market:"+market;
				// return newKey;
				// }
				// })
				.aggregate(new Initializer<Fraction>() {
					@Override
					public Fraction apply() {
						return new Fraction(0, 0.0, 0.0);
					}
				}, new Aggregator<String, String, Fraction>() {
					@Override
					public Fraction apply(String recordKey, String recordValue, Fraction aggregate) {
						String[] record = recordValue.split(",");
						Double EUCELL_DL_TPUT_NUM_KBITS = 0.0;
						Double EUCELL_DL_TPUT_DEN_SECS = 0.0;
						if (record.length != 0) {
							EUCELL_DL_TPUT_NUM_KBITS = Double.parseDouble(record[0]);
							EUCELL_DL_TPUT_DEN_SECS = Double.parseDouble(record[1]);
						}
						aggregate.count++;
						aggregate.numerator += EUCELL_DL_TPUT_NUM_KBITS;
						aggregate.denominator += EUCELL_DL_TPUT_DEN_SECS;
						return aggregate;
					}
				}, FractionSerde).mapValues(new ValueMapper<Fraction, String>() {
					@Override
					public String apply(Fraction aggregate) {
						return "{\"count\":\"" + aggregate.count + "\", \"UTput\":\""
								+ (aggregate.numerator / aggregate.denominator) + "\"}";
						// return "count:" + aggregate.count + " UTput:" +
						// (aggregate.numerator / aggregate.denominator);
					}
				});
		marketUserTput.to(Serdes.String(), Serdes.String(), outputTopic);
		return builder;
	}
}
