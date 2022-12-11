import argparse
import logging
import sys

from pyflink.common import WatermarkStrategy, Encoder, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat, FileSink, OutputFileConfig, RollingPolicy


def word_count(input_path):
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.BATCH)
    # write all the data to one file
    env.set_parallelism(1)

    # define the source
    ds = env.from_source(
        source=FileSource.for_record_stream_format(StreamFormat.text_line_format(),
                                                   input_path)
                         .process_static_file_set().build(),
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
        source_name="file_source"
    )

    def split(line):
        yield from line.split()

    # compute word count
    ds = ds.flat_map(split) \
        .map(lambda i: (i[0], i[1], 1), output_type=Types.TUPLE([Types.STRING(), Types.FLOAT()])) \
        .key_by(lambda i: i[0]) \
        .reduce(lambda i, j: (i[0], (i[1] + j[1]) / (i[2] + j[2])))

    print("Printing result to stdout. Use --output to specify output path.")
    ds.print()

    # submit for execution
    env.execute()


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    word_count('ACC_minute_data_with_indicators.csv')
