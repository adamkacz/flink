import random

from pyflink.common import WatermarkStrategy, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat, FileSink, RollingPolicy


def mean_counter(input_path):
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
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
        return line.split(",")

    # compute word count
    ds = ds.map(split, output_type=Types.TUPLE([Types.STRING(), Types.STRING()])) \
        .map(lambda i: (1, float(i[1]), 1), output_type=Types.TUPLE([Types.INT(), Types.FLOAT(), Types.FLOAT()])) \
        .key_by(lambda i: i[0]) \
        .reduce(lambda i, j: (i[0], (i[1] * i[2] + j[1]) / (i[2] + j[2]), i[2] + j[2]))

    print("Printing result to stdout. Use --output to specify output path.")
    ds.print()

    # submit for execution
    env.execute()


if __name__ == '__main__':
    mean_counter('ACC_minute_data_with_indicators.csv')
