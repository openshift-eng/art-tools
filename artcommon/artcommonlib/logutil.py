import logging


class EntityLoggingAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return '[%s] %s' % (self.extra['entity'], msg), kwargs


def get_logger(module_name=None):
    """
    Returns a logger appropriate for use in the art-tools package.
    Modules should request a logger using their __name__
    """

    logger_name = 'art_tools'

    if module_name:
        logger_name = '{}.{}'.format(logger_name, module_name)

    return logging.getLogger(logger_name)
