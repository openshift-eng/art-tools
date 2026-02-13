import threading


class RecordLogger:
    """A class to log records of actions taken by Doozer that need to be communicated to outside systems.
    This class usually writes record.log file in the working directory
    """

    def __init__(self, path: str) -> None:
        self._record_log = open(path, "a", encoding="utf-8")
        self._log_lock = threading.Lock()

    def close(self):
        self._record_log.close()

    def add_record(self, record_type: str, **kwargs):
        """
        Records an action taken by oit that needs to be communicated to outside
        systems. For example, the update a Dockerfile which needs to be
        reviewed by an owner. Each record is encoded on a single line in the
        record.log. Records cannot contain line feeds -- if you need to
        communicate multi-line data, create a record with a path to a file in
        the working directory.

        :param record_type: The type of record to create.
        :param kwargs: key/value pairs

        A record line is designed to be easily parsed and formatted as:
        record_type|key1=value1|key2=value2|...|
        """
        record = "%s|" % record_type
        for k, v in kwargs.items():
            assert "\n" not in str(k)
            # Make sure the values have no linefeeds as this would interfere with simple parsing.
            v = str(v).replace("\n", " ;;; ").replace("\r", "")
            record += "%s=%s|" % (k, v)
        # Multiple image build processes could be calling us with action simultaneously, so
        # synchronize output to the file.
        with self._log_lock:
            # Add the record to the file
            self._record_log.write("%s\n" % record)
            self._record_log.flush()
