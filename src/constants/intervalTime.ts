export enum IntervalTime {
  DELETE_FOLDER = 36000000, // 36000000 ms -> 10h
  UNPROCESSED_FILES = 28800000, // 28800000 ms ->  8h
  FAILED_FILES = 7200000, //  7200000 ms -> 2h
  CLEAN_UP_PROCESSED_FILES = 7200000, //  7200000 ms -> 2h
  DATA_PROCESSING = 9000, // 9000 ms -> 9s
  COPY_GZ_S3_TO_REDSHIFT = 2000, // 2000 ms -> 2c
  COPY_GZ_TO_S3 = 2000, // 2000 ms -> 2c
  SEND_AFFILIATES_IDS_TO_SQS = 7200000, // 7200000 ms -> 2h
}
