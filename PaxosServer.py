import sys
import SocketServer
from message import *
from datetime import datetime



class PaxosServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
  timeout = 5

  daemon_threads = True
  allow_reuse_address = True


  # s stores persistent information that should be stored in the disk
  s = {
    "n": 0
  }

  def __init__(self, node_id, nodes_count, RequestHandlerClass):
    self.node_id = int(node_id)
    self.nodes_count = nodes_count
    self.largest_accepted_proposal_n = -1
    self.proposal_queue = []
    self.current_leader_id = 1 # 1 by default
    self.promise_count = {}
    self.acceptance_count = {}
    self.leader_last_seen = datetime.now()

    server_address = ("localhost", 9000+node_id)

    self.load_s()
    SocketServer.TCPServer.__init__(self, server_address, RequestHandlerClass)

  def handle_timeout(self):
    #FIXME handle timeout
    print 'Timeout!'

  def sendToServer(self, server_id, msg_type, params):
    msg = params.copy()
    params["type"] = msg_type
    params["sender"] = self.node_id

    self.log("sendToServer %s: %s" % (server_id, msg))
    # FIXME(kanitw): Shih-wen please finish this method
    pass

  def sendToClient(self, client_id, msg_type, params):
    msg = params.copy()
    params["type"] = msg_type
    params["sender"] = self.node_id

    self.log("sendToClient %s: %s" % (client_id, msg))
    # FIXME(kanitw): Shih-wen please finish this method
    pass

  def log(self, log):
    print "Server %s: %s" % (self.server_id, log)

  def in_proposal_queue(self, msg):
    #FIXME check if (msg.client_id, msg.client_command_id0 are in queue
    pass

  def is_executed(self, msg):
    #FIXME check the map

  def broadcast_prepare(self):
    msg = self.proposal_queue[0]
    for node in range(self.nodes_count):
      if node != self.node_id:
        # for all nodes other than this one!
        self.sendToServer(node, PREPARE_REQUEST, {
          "proposer_id": self.node_id,
          "n": self.s["n"],
          "command": msg["command"]
        })

  def broadcast_accept(self, n, command):
    for node in range(self.nodes_count):
      if node != self.node_id:
        # for all nodes other than this one!
        self.sendToServer(node, ACCEPT_REQUEST, {
          "n": n,
          "command": command
        })

  def get_n_tuple(self):
    return [self.s["n"], self.node_id]

  def inc_count(self, counter, n):
    if not n in counter:
      counter[n] = 1
    else
      counter[n] += 1


  ## compare tuple of n  (n, node_id)
  @staticmethod
  def compare_n_tuples(nt1, nt2):
    if nt1[0]-nt2[0] == 0:
      return nt1[1] - nt2[1]
    return nt1[0]-nt2[0]

  def save_s(self):
    pass
    #FIXME(kanitw) Shih-wen says it's easy

  def load_s(self):
    pass
    #FIXME(kanitw) Shih-wen says it's easy

  def reset_instance(self):
    self.largest_accepted_proposal = None
    self.promise_count = {}
    self.acceptance_count = {}
    self.s["n"] = 0

  def messageHandler(self, msg):
    # PROPOSER’s message
    if msg["type"] == CLIENT_REQUEST:

      # CLIENT_REQUEST(client_id, client_command_id, command)

      if self.node_id != self.current_leader_id:
        self.sendToClient(msg.client_id, PLEASE_ASK_LEADER, {"current_leader_id": self.current_leader_id})
      elif self.in_proposal_queue(msg):
        pass  # ignore
      elif self.is_executed(msg):
        self.sendToClient(msg.client_id, EXECUTED, {"client_command_id": msg.client_command_id})
      else:
        self.proposal_queue.append(msg)
        if len(self.proposal_queue) == 1:  # so it was empty before
          self.broadcast_prepare()


    if msg["type"] == PREPARE_AGREE:
      # PREPARE_AGREE(instance, n, largest_accepted_proposal_n, largest_accepted_proposal_cmd)
      # FIXME(kanitw): use compare
      if msg["largest_accepted_proposal_n"] is not None and \
                      msg["largest_accepted_proposal_n"] > self.largest_accepted_proposal_n:
        self.largest_accepted_proposal_n = msg["largest_accepted_proposal_n"]
        self.largest_accepted_proposal_cmd = msg["largest_accepted_proposal_cmd"]

      self.inc_count(self.promise_count, msg["n"])

      # check if a majority has agreed
      if self.promise_count[msg["n"]] + 1 == self.nodes_count/2 + 1:
        # 1 on the left side = the leader itself!
        # and we only broadcast only the first time it has the majority to vote on something

        # (from PMS) issue a proposal with number n and value v, where v is the value of the highest-numbered proposal
        # among the responses, or is any value selected by the proposer if the responders reported no proposals.
        if self.largest_accepted_proposal_n is not None:
          self.broadcast_accept(self.largest_accepted_proposal_n, self.largest_accepted_proposal_cmd)
        else:
          self.broadcast_accept(msg["n"], self.proposal_queue[0]["command"])

    if msg["type"] == PREPARE_REJECT:
      # PREPARE_REJECT comes with (instance, n, min_n, min_n_proposer)
      if self.s["n"] < msg["min_n"]: #FIXME use compare
        self.s["n"] = msg["min_n"] + 1

      # abandon all proposal less with number < min_n
      # but we propose one at a time
      if msg["n"] < self.s["n"]:
        pass  # this is a rejection for abandoned message
      else:
        # first time to get rejection for message n\
        self.broadcast_prepare()


    # messages for ACCEPTOR:
    # Note: Acceptor must remember its highest promise for each command instance.

    if msg["type"] == PREPARE_REQUEST:
      # PREPARE_REQUEST(proposer_id, n_tuple, v):
      self.leader_last_seen = datetime.now()
      # TODO is there a case that leader is not the sender

      if compare_n_tuples(msg["n_tuple"], )

      if compare_n(msg["n"], self.s["n"], msg["sender"], self.s["n_proposer"]) >= 0:
        self.s["n"] = msg["n"]
        self.s["n_proposer"] = msg["sender"]
        save_s()

        self.sendToServer(msg["sender"], PREPARE_AGREE, {
          "n": msg["n"],
          "largest_accepted_proposal_n": self.largest_accepted_proposal_n,
          "largest_accepted_proposal_cmd": self.largest_accepted_proposal_cmd
        })
      else:
        self.sendToServer(msg["sender"], PREPARE_REJECT, {
          "n": msg["n"],
          "my_n": None #FIXME check....
        })


    if msg["type"] == ACCEPT_REQUEST:
      # ACCEPT_REQUEST(sender/proposer, n_tuple, v)
      self.leader_last_seen = datetime.now()
      if self.compare_n_tuples(msg["n_tuple"], self.get_n_tuple()) > 0:
        self.sendToServer(msg["sender"], {
          "n_tuple": msg["n_tuple"]
        })
        self.largest_accepted_proposal_n = msg["n_tuple"][0]
        self.largest_accepted_proposal_cmd = msg["v"]
      else:
        pass
        #QUESTION: Do we have to notify proposer in this case?


    # messages for DISTINGUISHED_LEARNERS

    if msg["type"]==ACCEPT:
      #comes with (instance, client_id, client_command_id, n,v)
      self.inc_count(self.acceptance_count,)
      inc_acceptance_count(instance, n)
        if acceptance_count[instance][n] + 1 > nodes_count/2: # 1=itself!
          send EXECUTE(instance, v) to all nodes (including itself)
          send EXECUTED(client_command_id) to client


    #messages for LEARNERS

      if msg["type"] == EXECUTE
    #comes with (instance, client_id, client_command_id, n, v)
        chosen_commands[instance] = v
        save_state()
        if instance == latest_instance + 1
          i = instance
          while chosen_command[i] != None:
          execute(client_id, v)
          client_latest_executed[client_id] = client_command_id
          latest_instance = i
          i++



      #assume we propose one instance at a time
      self.reset_instance()
      if node_id == current_leader_id:
        propose_next_instance()


    if msg["type"] == ARE_YOU_AWAKE:
    #send I_AM_AWAKE to pid

    if msg["type"] == PLEASE_UPDATE_ME:
      send

    check_timestamp()

  d
