STATE_PEND = "pending"
STATE_PASS = "passed"
STATE_FAIL = "failed"

TEMPLATE_BASE_STATE = {
    "status": STATE_PASS,
    "msg": "Complete",
}

TEMPLATE_IMAGE = {
    "status": STATE_PEND,
    "msg": "",
    "images": {},
    "total": 0,
    "success": 0,
    "fail": 0,
}


class DoozerStateError(Exception):
    """A broad exception for errors during Brew CRUD operations"""

    pass


def record_image_success(state, image):
    state["success"] += 1
    state["images"][image.distgit_key] = True


def record_image_fail(state, image, msg, logger=None):
    state["fail"] += 1
    state["images"][image.distgit_key] = msg
    if logger:
        logger.error("[{}] {}".format(image.qualified_key, msg))


def record_image_finish(state, msg="Complete"):
    if state["fail"]:
        state["status"] = STATE_FAIL
    elif state["total"] == state["success"]:
        state["status"] = STATE_PASS
