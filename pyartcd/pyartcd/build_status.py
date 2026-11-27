import logging

from pyartcd import jenkins, tekton

logger = logging.getLogger(__name__)


def update_title(title: str):
    if tekton.is_tekton_context():
        logger.info('Build status: %s', title.strip())
    else:
        jenkins.update_title(title)


def update_description(description: str):
    if tekton.is_tekton_context():
        logger.info('%s', description.replace('<br/>', '\n').strip())
    else:
        jenkins.update_description(description)
