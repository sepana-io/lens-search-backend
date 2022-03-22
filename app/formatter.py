from pythonjsonlogger import jsonlogger


class ElkJsonFormatter(jsonlogger.JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        super(ElkJsonFormatter, self).add_fields(
            log_record, record, message_dict)
        log_record['level'] = record.levelname
        log_record['logger'] = record.name
