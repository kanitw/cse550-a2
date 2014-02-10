PLEASE_ASK_LEADER = "PLEASE_ASK_LEADER"
PLEASE_WAIT = "PLEASE_WAIT"
CLIENT_REQUEST = "CLIENT_REQUEST"
PREPARE_AGREE = "PREPARE_AGREE"
PREPARE_REJECT = "PREPARE_REJECT"
PREPARE_REQUEST = "PREPARE_REQUEST"
ACCEPT_REQUEST = "ACCEPT_REQUEST"
ACCEPT = "ACCEPT"
EXECUTE = "EXECUTE"
EXECUTED = "EXECUTED"
ARE_YOU_AWAKE = "ARE_YOU_AWAKE"
IM_AWAKE = "IM_AWAKE"
PLEASE_UPDATE_ME = "PLEASE_UPDATE_ME"


class Message():
  message = None
  def __init__(self, type, params):
    _message = params.copy()
    _message.update({"type": type})

